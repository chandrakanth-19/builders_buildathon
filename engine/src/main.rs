// Shopping Sessions engine.
//
// Pipeline (all phases parallel over a fixed pool of worker threads):
//   1. products / campaigns / manifest (tiny, sequential)
//   2. links: parallel chunked parse -> partitioned key maps -> union-find -> person name
//   3. events + clicks: one work pool over ~2 MB newline-aligned chunks of every file.
//      events -> scattered into buckets by hash(event_id); clicks -> buckets by hash(person)
//   4. per event bucket: hash-table dedup (min ingest), validate, scatter survivors by hash(person)
//   5. per person bucket: sort, sessionize, attribute, assists; aggregate
//   6. each worker writes its own user_daily parquet file; campaign_daily merged + written once
//
// IDs are packed into a 16-byte order-preserving `Key` (bytes big-endian + length byte), so
// comparisons are integer compares that agree with byte-wise string order. IDs longer than 15
// bytes are interned exactly in a global table and compared through it.

use std::collections::HashMap;
use std::fs::File;
use std::hash::{BuildHasherDefault, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use arrow::array::{ArrayRef, Date32Array, Int32Array, Int64Array, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use memmap2::Mmap;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::schema::types::ColumnPath;

// ---------------------------------------------------------------------------
// Allocator: large blocks straight from mmap with transparent huge pages,
// grown in place with mremap; small blocks from the system allocator.
// ---------------------------------------------------------------------------

struct HugeAlloc;
/// Whether to request transparent huge pages (MADV_HUGEPAGE); see `main`.
static USE_THP: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(true);
const ARENA_MIN: usize = 4 << 10; // [ARENA_MIN, BIG): per-thread huge-page bump arenas
const BIG: usize = 4 << 20; // >= BIG: dedicated mmap
const HUGE: usize = 2 << 20;
const ARENA_REGION: usize = 64 << 20;

#[inline(always)]
fn round_up(n: usize) -> usize {
    (n + 4095) & !4095
}

unsafe fn big_map(size: usize) -> *mut u8 {
    let len = round_up(size);
    let p = libc::mmap(
        std::ptr::null_mut(),
        len,
        libc::PROT_READ | libc::PROT_WRITE,
        libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
        -1,
        0,
    );
    if p == libc::MAP_FAILED {
        return std::ptr::null_mut();
    }
    if len >= HUGE && USE_THP.load(std::sync::atomic::Ordering::Relaxed) {
        libc::madvise(p, len, libc::MADV_HUGEPAGE);
    }
    p as *mut u8
}

thread_local! {
    // (cursor, end) of this thread's current arena region
    static ARENA: std::cell::Cell<(usize, usize)> = const { std::cell::Cell::new((0, 0)) };
}

#[inline(always)]
fn class(l: &std::alloc::Layout) -> u8 {
    if l.align() > 4096 || l.size() < ARENA_MIN {
        0
    } else if l.size() < BIG {
        1
    } else {
        2
    }
}

unsafe fn arena_alloc(size: usize, align: usize) -> *mut u8 {
    let align = align.max(64);
    ARENA.with(|a| {
        let (mut cur, mut end) = a.get();
        let mut p = (cur + align - 1) & !(align - 1);
        if cur == 0 || p + size > end {
            let raw = big_map(ARENA_REGION + HUGE);
            if raw.is_null() {
                return std::ptr::null_mut();
            }
            let start = ((raw as usize) + HUGE - 1) & !(HUGE - 1);
            cur = start;
            end = raw as usize + ARENA_REGION + HUGE;
            p = (cur + align - 1) & !(align - 1);
        }
        a.set((p + size, end));
        p as *mut u8
    })
}

// Arena blocks come in size classes 2^k and 1.5*2^k (>= ARENA_MIN), so a freed block can
// be reused by any later request of the same class. Free blocks sit on global intrusive
// lists (one per class, spinlock-protected): no syscalls, hence no TLB shootdowns, and
// memory freed in one phase is reused by the next (pool threads are per phase).
#[inline(always)]
fn arena_class(size: usize) -> (usize, usize) {
    let s = size.max(ARENA_MIN);
    let c = if s.is_power_of_two() {
        s
    } else {
        let p2 = s.next_power_of_two();
        let h = p2 / 2;
        if s <= h + h / 2 { h + h / 2 } else { p2 }
    };
    let tz = c.trailing_zeros() as usize;
    (c, 2 * tz + (!c.is_power_of_two()) as usize)
}

struct FreeList {
    lock: std::sync::atomic::AtomicBool,
    head: std::cell::UnsafeCell<usize>,
}
unsafe impl Sync for FreeList {}
const FREE_LIST_INIT: FreeList =
    FreeList { lock: std::sync::atomic::AtomicBool::new(false), head: std::cell::UnsafeCell::new(0) };
static FREE_LISTS: [FreeList; 64] = [FREE_LIST_INIT; 64];

impl FreeList {
    #[inline(always)]
    fn with<R>(&self, f: impl FnOnce(&mut usize) -> R) -> R {
        use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
        while self.lock.compare_exchange_weak(false, true, Acquire, Relaxed).is_err() {
            std::hint::spin_loop();
        }
        let r = f(unsafe { &mut *self.head.get() });
        self.lock.store(false, Release);
        r
    }
}

unsafe fn class_alloc(size: usize, align: usize) -> *mut u8 {
    let (c, idx) = arena_class(size);
    if align <= 64 {
        let p = FREE_LISTS[idx].with(|h| {
            let p = *h;
            if p != 0 {
                *h = *(p as *const usize);
            }
            p
        });
        if p != 0 {
            return p as *mut u8;
        }
    }
    arena_alloc(c, align)
}

/// Rewinds the arena if this is its most recent block, otherwise puts it on its free list.
unsafe fn class_free(p: *mut u8, size: usize) {
    let (c, idx) = arena_class(size);
    let rewound = ARENA.with(|a| {
        let (cur, end) = a.get();
        if p as usize + c == cur {
            a.set((p as usize, end));
            true
        } else {
            false
        }
    });
    if !rewound {
        FREE_LISTS[idx].with(|h| {
            *(p as *mut usize) = *h;
            *h = p as usize;
        });
    }
}

unsafe impl std::alloc::GlobalAlloc for HugeAlloc {
    #[inline]
    unsafe fn alloc(&self, l: std::alloc::Layout) -> *mut u8 {
        match class(&l) {
            0 => std::alloc::System.alloc(l),
            1 => class_alloc(l.size(), l.align()),
            _ => big_map(l.size()),
        }
    }
    #[inline]
    unsafe fn alloc_zeroed(&self, l: std::alloc::Layout) -> *mut u8 {
        match class(&l) {
            0 => std::alloc::System.alloc_zeroed(l),
            1 => {
                let p = class_alloc(l.size(), l.align());
                if !p.is_null() {
                    // arena memory may be recycled from a freed block
                    std::ptr::write_bytes(p, 0, l.size());
                }
                p
            }
            _ => big_map(l.size()), // fresh anonymous pages are zero
        }
    }
    #[inline]
    unsafe fn dealloc(&self, p: *mut u8, l: std::alloc::Layout) {
        match class(&l) {
            0 => std::alloc::System.dealloc(p, l),
            1 => class_free(p, l.size()),
            _ => {
                libc::munmap(p as *mut libc::c_void, round_up(l.size()));
            }
        }
    }
    #[inline]
    unsafe fn realloc(&self, p: *mut u8, l: std::alloc::Layout, new_size: usize) -> *mut u8 {
        let nl = std::alloc::Layout::from_size_align_unchecked(new_size, l.align());
        let (co, cn) = (class(&l), class(&nl));
        if co == 0 && cn == 0 {
            return std::alloc::System.realloc(p, l, new_size);
        }
        if co == 2 && cn == 2 {
            let q = libc::mremap(
                p as *mut libc::c_void,
                round_up(l.size()),
                round_up(new_size),
                libc::MREMAP_MAYMOVE,
            );
            if q == libc::MAP_FAILED {
                return std::ptr::null_mut();
            }
            if USE_THP.load(std::sync::atomic::Ordering::Relaxed) {
                libc::madvise(q, round_up(new_size), libc::MADV_HUGEPAGE);
            }
            return q as *mut u8;
        }
        if co == 1 && cn == 1 {
            let (oc, _) = arena_class(l.size());
            let (nc, _) = arena_class(new_size);
            if oc == nc {
                return p; // same size class: the block already fits
            }
            // grow/shrink in place when this is the arena's most recent block
            let grown = ARENA.with(|a| {
                let (cur, end) = a.get();
                if p as usize + oc == cur && p as usize + nc <= end {
                    a.set((p as usize + nc, end));
                    true
                } else {
                    false
                }
            });
            if grown {
                return p;
            }
        }
        let q = self.alloc(nl);
        if !q.is_null() {
            std::ptr::copy_nonoverlapping(p, q, l.size().min(new_size));
            self.dealloc(p, l);
        }
        q
    }
}

#[global_allocator]
static GLOBAL: HugeAlloc = HugeAlloc;

const DAY_MS: i64 = 86_400_000;
const WEEK_MS: i64 = 7 * DAY_MS;
const GAP_MS: i64 = 1_800_000;
const CHUNK: usize = 2 << 20;

const P_BITS: u32 = 10; // person buckets
const NP: usize = 1 << P_BITS;

// ---------------------------------------------------------------------------
// Keys
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, Debug)]
struct Key {
    hi: u64,
    lo: u64,
}

impl Key {
    #[inline(always)]
    fn is_long(self) -> bool {
        (self.lo & 0xFF) == 0xFF
    }
}

// IDs longer than 15 bytes (1 in 8 IDs since shopping-sessions-5: a prefix plus 32 hex
// digits) are keyed by a 120-bit hash instead of their bytes: no shared table, no locks.
// Equality of long keys is equality of hashes (collision odds for millions of IDs are
// ~1e-23). Only user and click IDs ever need their bytes back (byte-wise ordering for
// person names and click tie-breaks, and writing names out); the threads that parse them
// record key -> location of the bytes inside the input mapping, which stays mapped until
// the process exits, so nothing is copied.

/// Where a long ID's bytes live: (address, length) inside the input mappings.
type LongMap = KMap<(usize, u32)>;

/// Long-ID byte tables: those recorded while parsing links (needed for person names
/// during link processing) and those recorded while parsing events and clicks.
static LINK_LONG: OnceLock<Vec<LongMap>> = OnceLock::new();
static DATA_LONG: OnceLock<Vec<LongMap>> = OnceLock::new();

#[inline]
fn long_key(b: &[u8]) -> Key {
    // Two 64-bit lanes over little-endian 8-byte words (zero-padded tail), seeded with the
    // length; the second lane sees rotated words so the lanes stay independent.
    let n = b.len() as u64;
    let mut h1: u64 = 0x243F_6A88_85A3_08D3 ^ n.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    let mut h2: u64 = 0x1319_8A2E_0370_7344 ^ n.wrapping_mul(0xC2B2_AE3D_27D4_EB4F);
    let mut chunks = b.chunks_exact(8);
    for c in &mut chunks {
        let w = u64::from_le_bytes(c.try_into().unwrap());
        h1 = fold(h1 ^ w, 0x9E37_79B9_7F4A_7C15);
        h2 = fold(h2 ^ w.rotate_left(29), 0xD6E8_FEB8_6659_FD93);
    }
    let rem = chunks.remainder();
    if !rem.is_empty() {
        let mut t = [0u8; 8];
        t[..rem.len()].copy_from_slice(rem);
        let w = u64::from_le_bytes(t);
        h1 = fold(h1 ^ w, 0x9E37_79B9_7F4A_7C15);
        h2 = fold(h2 ^ w.rotate_left(29), 0xD6E8_FEB8_6659_FD93);
    }
    let f1 = fold(h1 ^ 0xA076_1D64_78BD_642F, h2 ^ 0xE703_7ED1_A0B4_28DB);
    let f2 = fold(h2 ^ 0x8EBC_6AF0_9C88_C6E3, h1 ^ 0x5899_65CC_7537_4CC3);
    Key { hi: f1, lo: (f2 << 8) | 0xFF }
}

/// Records where a long ID's bytes are (no-op for short IDs).
#[inline(always)]
fn note_long(map: &mut LongMap, k: Key, b: &[u8]) {
    if k.is_long() {
        map.entry(k).or_insert((b.as_ptr() as usize, b.len() as u32));
    }
}

fn long_bytes(k: Key) -> &'static [u8] {
    for tables in [LINK_LONG.get(), DATA_LONG.get()].into_iter().flatten() {
        for t in tables {
            if let Some(&(p, l)) = t.get(&k) {
                return unsafe { std::slice::from_raw_parts(p as *const u8, l as usize) };
            }
        }
    }
    panic!("bytes of a long ID were not recorded");
}

/// The first 15 bytes of a key's ID, zero-padded, as a big-endian integer. If two IDs'
/// prefixes differ, their order equals the IDs' byte-wise order; equal prefixes say nothing.
fn key_prefix(k: Key) -> u128 {
    let mut buf = [0u8; 16];
    let b = key_slice(k, &mut buf);
    let mut p = [0u8; 16];
    let n = b.len().min(15);
    p[..n].copy_from_slice(&b[..n]);
    u128::from_be_bytes(p)
}

/// The bytes behind a key (short keys decode into `buf`).
#[inline(always)]
fn key_slice(k: Key, buf: &mut [u8; 16]) -> &[u8] {
    if k.is_long() {
        return long_bytes(k);
    }
    buf[0..8].copy_from_slice(&k.hi.to_be_bytes());
    buf[8..16].copy_from_slice(&k.lo.to_be_bytes());
    let n = buf[15] as usize;
    &buf[..n]
}

#[inline(always)]
fn key_of(b: &[u8]) -> Key {
    let n = b.len();
    if n > 15 {
        return long_key(b);
    }
    let mut buf = [0u8; 16];
    buf[..n].copy_from_slice(b);
    buf[15] = n as u8;
    Key {
        hi: u64::from_be_bytes(buf[0..8].try_into().unwrap()),
        lo: u64::from_be_bytes(buf[8..16].try_into().unwrap()),
    }
}

/// Same as `key_of`, using one unaligned 16-byte load when `end` (one past the last readable
/// byte of the underlying buffer) leaves room for it.
#[inline(always)]
fn key_fast(f: &[u8], end: usize) -> Key {
    let n = f.len();
    if n > 15 || (f.as_ptr() as usize) + 16 > end {
        return key_of(f);
    }
    let raw = unsafe { std::ptr::read_unaligned(f.as_ptr() as *const [u8; 16]) };
    let hi = u64::from_be_bytes(raw[0..8].try_into().unwrap());
    let lo = u64::from_be_bytes(raw[8..16].try_into().unwrap());
    let (hi, lo) = if n > 8 {
        (hi, lo & (!0u64 << (8 * (16 - n))))
    } else if n == 8 {
        (hi, 0)
    } else if n == 0 {
        (0, 0)
    } else {
        (hi & (!0u64 << (8 * (8 - n))), 0)
    };
    Key { hi, lo: lo | n as u64 }
}

#[inline(always)]
fn write_key(k: Key, out: &mut Vec<u8>) {
    let mut buf = [0u8; 16];
    out.extend_from_slice(key_slice(k, &mut buf));
}

/// Byte-wise string order of the IDs behind two keys.
#[inline(always)]
fn key_cmp(a: Key, b: Key) -> std::cmp::Ordering {
    if !a.is_long() && !b.is_long() {
        a.cmp(&b)
    } else {
        let (mut x, mut y) = ([0u8; 16], [0u8; 16]);
        key_slice(a, &mut x).cmp(key_slice(b, &mut y))
    }
}

#[inline(always)]
fn fold(a: u64, b: u64) -> u64 {
    let m = (a as u128).wrapping_mul(b as u128);
    (m as u64) ^ ((m >> 64) as u64)
}

#[inline(always)]
fn khash(k: Key) -> u64 {
    fold(
        fold(k.hi ^ 0x243F_6A88_85A3_08D3, k.lo ^ 0x1319_8A2E_0370_7344),
        0x9E37_79B9_7F4A_7C15,
    )
}

// Bucket index uses middle bits so hashbrown (low bits = slot, top 7 = tag) still sees entropy.
#[inline(always)]
fn ebucket(h: u64, mask: usize) -> usize {
    ((h >> 40) as usize) & mask
}
#[inline(always)]
fn pbucket(h: u64) -> usize {
    ((h >> 30) as usize) & (NP - 1)
}

#[derive(Default)]
struct KHasher(u64);
impl Hasher for KHasher {
    #[inline(always)]
    fn finish(&self) -> u64 {
        self.0
    }
    #[inline(always)]
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 = fold(self.0 ^ b as u64, 0x9E37_79B9_7F4A_7C15);
        }
    }
    #[inline(always)]
    fn write_u64(&mut self, v: u64) {
        self.0 = v;
    }
}
impl std::hash::Hash for Key {
    #[inline(always)]
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(khash(*self));
    }
}
type KMap<V> = HashMap<Key, V, BuildHasherDefault<KHasher>>;

fn kmap<V>(cap: usize) -> KMap<V> {
    HashMap::with_capacity_and_hasher(cap, Default::default())
}

// Flat open-addressing map Key -> Key, partitioned; misses return the key itself.
struct PSub {
    slots: Vec<(Key, Key)>,
    mask: usize,
}
impl PSub {
    fn new(n: usize) -> Self {
        let cap = (n * 2).next_power_of_two().max(16);
        PSub { slots: vec![(Key::default(), Key::default()); cap], mask: cap - 1 }
    }
    fn insert(&mut self, k: Key, h: u64, v: Key) {
        let mut i = (h as usize) & self.mask;
        loop {
            let s = &mut self.slots[i];
            if s.0 == Key::default() || s.0 == k {
                *s = (k, v);
                return;
            }
            i = (i + 1) & self.mask;
        }
    }
}
struct PMap {
    parts: Vec<PSub>,
}
impl PMap {
    #[inline(always)]
    fn part(&self, h: u64) -> &PSub {
        &self.parts[((h >> 40) as usize) & (self.parts.len() - 1)]
    }
    #[inline(always)]
    fn prefetch(&self, h: u64) {
        let p = self.part(h);
        let ptr = unsafe { p.slots.as_ptr().add((h as usize) & p.mask) };
        #[cfg(target_arch = "x86_64")]
        unsafe {
            std::arch::x86_64::_mm_prefetch(ptr as *const i8, std::arch::x86_64::_MM_HINT_T0);
        }
    }
    #[inline(always)]
    fn get(&self, k: Key, h: u64) -> Key {
        let p = self.part(h);
        let mut i = (h as usize) & p.mask;
        loop {
            let s = &p.slots[i];
            if s.0 == k {
                return s.1;
            }
            if s.0 == Key::default() {
                return k;
            }
            i = (i + 1) & p.mask;
        }
    }
}

// ---------------------------------------------------------------------------
// CSV tokenizer: iterates lines of a chunk, yielding exactly-N unquoted fields.
// ---------------------------------------------------------------------------

/// Calls `f(fields)` for every well-formed line of `buf` with exactly N fields.
/// Malformed lines (wrong field count, bad quotes, empty) are skipped.
///
/// Works per line from per-block bitmasks: comma positions come from the comma mask, and
/// quoting is validated in closed form: every field that starts with '"' must end with '"'
/// and be at least 2 bytes long, no field may end with '"' without starting with one, and the
/// line's total quote count must equal 2 x (number of quoted fields), so no stray quotes remain.
#[inline(always)]
fn for_each_line<const N: usize, F: FnMut(&[&[u8]; N])>(buf: &[u8], mut f: F) {
    let len = buf.len();
    let mut commas = [0usize; N];
    let mut ncomma = 0usize;
    let mut qcount = 0u32;
    let mut line_start = 0usize;

    #[inline(always)]
    fn emit<const N: usize, F: FnMut(&[&[u8]; N])>(
        buf: &[u8],
        ls: usize,
        le: usize,
        commas: &[usize; N],
        ncomma: usize,
        qcount: u32,
        f: &mut F,
    ) {
        if le == ls || ncomma != N - 1 {
            return;
        }
        let mut fields: [&[u8]; N] = [&[]; N];
        if qcount == 0 {
            let mut s = ls;
            for i in 0..N - 1 {
                fields[i] = &buf[s..commas[i]];
                s = commas[i] + 1;
            }
            fields[N - 1] = &buf[s..le];
            f(&fields);
            return;
        }
        // Branch-free: a field is quoted iff it has >= 2 bytes and '"' at both ends; any
        // other quote anywhere makes the line's quote count exceed 2 x quoted fields.
        let mut quoted = 0u32;
        let mut s = ls;
        for i in 0..N {
            let e = if i < N - 1 { commas[i] } else { le };
            let first = *buf.get(s).unwrap_or(&0);
            let last = *buf.get(e.wrapping_sub(1)).unwrap_or(&0);
            let q = ((e >= s + 2) & (first == b'"') & (last == b'"')) as usize;
            fields[i] = &buf[s + q..e - q];
            quoted += q as u32;
            s = e + 1;
        }
        if qcount != 2 * quoted {
            return;
        }
        f(&fields);
    }

    let mut base = 0usize;
    let mut tail = [0u8; 64];
    while base < len {
        let blk: &[u8; 64] = if base + 64 <= len {
            buf[base..base + 64].try_into().unwrap()
        } else {
            tail[..len - base].copy_from_slice(&buf[base..]);
            tail[len - base..].fill(0);
            &tail
        };
        let (mut cm, mut nm, mut qm) = delim_masks(blk);
        loop {
            let below = if nm != 0 { (nm & nm.wrapping_neg()) - 1 } else { !0u64 };
            let mut cs = cm & below;
            qcount += (qm & below).count_ones();
            while cs != 0 {
                if ncomma < N {
                    commas[ncomma] = base + cs.trailing_zeros() as usize;
                }
                ncomma += 1;
                cs &= cs - 1;
            }
            if nm == 0 {
                break;
            }
            let nl = base + nm.trailing_zeros() as usize;
            emit::<N, F>(buf, line_start, nl, &commas, ncomma, qcount, &mut f);
            line_start = nl + 1;
            ncomma = 0;
            qcount = 0;
            let upto = below | (nm & nm.wrapping_neg());
            cm &= !upto;
            qm &= !upto;
            nm &= nm - 1;
        }
        base += 64;
    }
    if len > line_start {
        emit::<N, F>(buf, line_start, len, &commas, ncomma, qcount, &mut f);
    }
}

/// Per-block masks of ',' / '\n' / '"' positions.
#[inline(always)]
fn delim_masks(b: &[u8; 64]) -> (u64, u64, u64) {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    unsafe {
        use std::arch::x86_64::*;
        let c = _mm256_set1_epi8(b',' as i8);
        let n = _mm256_set1_epi8(b'\n' as i8);
        let q = _mm256_set1_epi8(b'"' as i8);
        let v0 = _mm256_loadu_si256(b.as_ptr() as *const __m256i);
        let v1 = _mm256_loadu_si256(b.as_ptr().add(32) as *const __m256i);
        let mk = |x: __m256i, y: __m256i| -> u64 {
            (_mm256_movemask_epi8(x) as u32 as u64) | ((_mm256_movemask_epi8(y) as u32 as u64) << 32)
        };
        return (
            mk(_mm256_cmpeq_epi8(v0, c), _mm256_cmpeq_epi8(v1, c)),
            mk(_mm256_cmpeq_epi8(v0, n), _mm256_cmpeq_epi8(v1, n)),
            mk(_mm256_cmpeq_epi8(v0, q), _mm256_cmpeq_epi8(v1, q)),
        );
    }
    #[allow(unreachable_code)]
    {
        let (mut c, mut n, mut q) = (0u64, 0u64, 0u64);
        for (i, &x) in b.iter().enumerate() {
            c |= ((x == b',') as u64) << i;
            n |= ((x == b'\n') as u64) << i;
            q |= ((x == b'"') as u64) << i;
        }
        (c, n, q)
    }
}

// ---------------------------------------------------------------------------
// Timestamps
// ---------------------------------------------------------------------------

#[inline(always)]
fn d2(s: &[u8], i: usize) -> Option<u32> {
    let a = s[i].wrapping_sub(b'0');
    let b = s[i + 1].wrapping_sub(b'0');
    if a > 9 || b > 9 {
        return None;
    }
    Some(a as u32 * 10 + b as u32)
}

#[inline(always)]
fn days_from_civil(mut y: i32, m: u32, d: u32) -> i64 {
    y -= (m <= 2) as i32;
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = (y - era * 400) as u32;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era as i64 * 146097 + doe as i64 - 719468
}

/// True where every byte selected by `m` is an ASCII digit.
#[inline(always)]
fn swar_digits(x: u64, m: u64) -> bool {
    let hi_ok = (x & 0xF0F0_F0F0_F0F0_F0F0 & m) == (0x3030_3030_3030_3030 & m);
    let lo_ok = (((x & 0x0F0F_0F0F_0F0F_0F0F) + 0x0606_0606_0606_0606) & 0x1010_1010_1010_1010 & m) == 0;
    hi_ok & lo_ok
}

#[inline(always)]
fn byte_at(x: u64, i: u32) -> u32 {
    ((x >> (8 * i)) & 0xFF) as u32
}

/// Seconds since the epoch for `YYYY-MM-DDTHH:MM:SS` at the start of `s` (len >= 20),
/// validated with a few 64-bit operations (SWAR) instead of per-byte branches.
#[inline(always)]
fn civil_secs(s: &[u8]) -> Option<i64> {
    let a = u64::from_le_bytes(s[0..8].try_into().unwrap()); // "YYYY-MM-"
    let b = u64::from_le_bytes(s[8..16].try_into().unwrap()); // "DDTHH:MM"
    let c = u32::from_le_bytes(s[16..20].try_into().unwrap()) as u64; // ":SS?"
    const DA: u64 = 0x00FF_FF00_FFFF_FFFF;
    const DB: u64 = 0xFFFF_00FF_FF00_FFFF;
    const DC: u64 = 0x0000_0000_00FF_FF00;
    let seps = ((a & 0xFF00_00FF_0000_0000) == 0x2D00_002D_0000_0000)
        & ((b & 0x0000_FF00_00FF_0000) == 0x0000_3A00_0054_0000)
        & ((c & 0xFF) == 0x3A);
    if !(seps & swar_digits(a, DA) & swar_digits(b, DB) & swar_digits(c, DC)) {
        return None;
    }
    let a = (a & DA) - (0x3030_3030_3030_3030 & DA);
    let b = (b & DB) - (0x3030_3030_3030_3030 & DB);
    let c = (c & DC) - (0x3030_3030_3030_3030 & DC);
    let y = byte_at(a, 0) * 1000 + byte_at(a, 1) * 100 + byte_at(a, 2) * 10 + byte_at(a, 3);
    let m = byte_at(a, 5) * 10 + byte_at(a, 6);
    let d = byte_at(b, 0) * 10 + byte_at(b, 1);
    let h = byte_at(b, 3) * 10 + byte_at(b, 4);
    let mi = byte_at(b, 6) * 10 + byte_at(b, 7);
    let se = byte_at(c, 1) * 10 + byte_at(c, 2);
    if m == 0 || m > 12 || d == 0 || h > 23 || mi > 59 || se > 59 {
        return None;
    }
    let dim = match m {
        2 => {
            let yy = y as i32;
            if (yy % 4 == 0) && (yy % 100 != 0 || yy % 400 == 0) {
                29
            } else {
                28
            }
        }
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    if d > dim {
        return None;
    }
    Some(days_from_civil(y as i32, m, d) * 86400 + (h * 3600 + mi * 60 + se) as i64)
}

#[inline(always)]
fn parse_ts(s: &[u8]) -> Option<i64> {
    let n = s.len();
    if n == 20 {
        if s[19] == b'Z' {
            return civil_secs(s).map(|v| v * 1000);
        }
    } else if n == 25 {
        let sign = s[19];
        if (sign == b'+' || sign == b'-') && s[22] == b':' {
            let base = civil_secs(s)?;
            let oh = d2(s, 20)?;
            let om = d2(s, 23)?;
            if oh > 23 || om > 59 {
                return None;
            }
            let off = (oh * 3600 + om * 60) as i64;
            let off = if sign == b'+' { off } else { -off };
            return Some((base - off) * 1000);
        }
    }
    if n == 0 {
        return None;
    }
    let mut v: i64 = 0;
    if n <= 18 {
        // cannot overflow i64
        for &b in s {
            let d = b.wrapping_sub(b'0');
            if d > 9 {
                return None;
            }
            v = v * 10 + d as i64;
        }
        return Some(v);
    }
    for &b in s {
        let d = b.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        v = v.checked_mul(10)?.checked_add(d as i64)?;
    }
    Some(v)
}

/// Purchase price: digits only, value 1..=10_000_000.
#[inline(always)]
fn parse_price(s: &[u8]) -> Option<u32> {
    if s.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &b in s {
        let d = b.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        v = (v * 10 + d as u64).min(100_000_000);
    }
    if (1..=10_000_000).contains(&v) {
        Some(v as u32)
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// BlockVec: append-only list of fixed 512-element blocks (no realloc copies, no slack
// beyond one partial block). Blocks come from the huge-page arena allocator.
// ---------------------------------------------------------------------------

/// Every block is BLOCK_BYTES regardless of element type, so blocks freed by one phase
/// (event-id buckets after dedup) are reused by the next (person buckets) without new page
/// faults.
const BLOCK_BYTES: usize = 4 << 10;

thread_local! {
    static FREE_BLOCKS: std::cell::UnsafeCell<Vec<usize>> = const { std::cell::UnsafeCell::new(Vec::new()) };
}

#[inline(always)]
fn block_layout() -> std::alloc::Layout {
    std::alloc::Layout::from_size_align(BLOCK_BYTES, 64).unwrap()
}

fn alloc_block() -> *mut u8 {
    let reused = FREE_BLOCKS.with(|f| unsafe { (*f.get()).pop() });
    if let Some(p) = reused {
        return p as *mut u8;
    }
    let p = unsafe { std::alloc::alloc(block_layout()) };
    if p.is_null() {
        std::alloc::handle_alloc_error(block_layout());
    }
    p
}

struct BlockVec<T: Copy> {
    blocks: Vec<*mut T>,
    len: usize,
    cur: *mut T, // next free slot in the last block
    left: usize, // free slots left in the last block
}
unsafe impl<T: Copy + Send> Send for BlockVec<T> {}
unsafe impl<T: Copy + Sync> Sync for BlockVec<T> {}

impl<T: Copy> BlockVec<T> {
    const CAP: usize = BLOCK_BYTES / std::mem::size_of::<T>();

    fn new() -> Self {
        BlockVec { blocks: Vec::new(), len: 0, cur: std::ptr::null_mut(), left: 0 }
    }
    #[inline(always)]
    fn len(&self) -> usize {
        self.len
    }
    #[inline(always)]
    fn push(&mut self, v: T) {
        if self.left == 0 {
            let p = alloc_block() as *mut T;
            self.blocks.push(p);
            self.cur = p;
            self.left = Self::CAP;
        }
        unsafe {
            self.cur.write(v);
            self.cur = self.cur.add(1);
        }
        self.left -= 1;
        self.len += 1;
    }
    #[inline(always)]
    fn block_len(&self, i: usize) -> usize {
        if i + 1 == self.blocks.len() { self.len - i * Self::CAP } else { Self::CAP }
    }
    /// Raw (pointer, length) of each block, for in-place updates.
    fn raw_blocks(&self) -> impl Iterator<Item = (*mut T, usize)> + '_ {
        (0..self.blocks.len()).map(move |i| (self.blocks[i], self.block_len(i)))
    }
    #[inline(always)]
    fn block(&self, i: usize) -> &[T] {
        unsafe { std::slice::from_raw_parts(self.blocks[i], self.block_len(i)) }
    }
    fn iter(&self) -> impl Iterator<Item = &T> + '_ {
        (0..self.blocks.len()).flat_map(move |i| self.block(i).iter())
    }
    /// Hands this list's blocks to the calling thread's free list.
    /// Safety: the contents must never be read again.
    unsafe fn recycle(&self) {
        FREE_BLOCKS.with(|f| {
            let fl = &mut *f.get();
            for &b in &self.blocks {
                fl.push(b as usize);
            }
        });
    }
}
impl<T: Copy> std::ops::Index<usize> for BlockVec<T> {
    type Output = T;
    #[inline(always)]
    fn index(&self, i: usize) -> &T {
        unsafe { &*self.blocks[i / Self::CAP].add(i % Self::CAP) }
    }
}
// Blocks are intentionally never freed: the process exits right after writing output.

// ---------------------------------------------------------------------------
// Records
// ---------------------------------------------------------------------------

const TS_NONE: u32 = u32::MAX;

#[derive(Clone, Copy)]
#[repr(C, packed(4))] // 44 bytes instead of 48; fields are only read by value
struct ERec {
    eid: Key,
    user: Key,
    ingest: i64,
    ts: u32, // ms since `base` (click window start); TS_NONE = invalid or losing copy
    pc: u32, // price << 8 | category (price 0 = not a purchase)
}

#[derive(Clone, Copy)]
struct PRec {
    person: Key,
    ts: u32,
    pc: u32,
}

#[derive(Clone, Copy)]
struct CRec {
    person: Key,
    click: Key,
    ts: u32,
    camp: u32,
}

struct URow {
    person: Key,
    day: i32,
    sessions: i64,
    events: i64,
    revenue: i64,
    top: i32, // category id or -1
}

// ---------------------------------------------------------------------------
// Work pool helpers
// ---------------------------------------------------------------------------

fn nthreads() -> usize {
    if let Ok(v) = std::env::var("NCPU") {
        if let Ok(n) = v.parse::<usize>() {
            if n > 0 {
                return n;
            }
        }
    }
    std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8)
}

/// Runs `work(state, item)` over items 0..n with dynamic scheduling on `t` threads,
/// each thread owning a state created by `init`. Returns the states.
fn pool<S: Send, I: Fn() -> S + Sync, W: Fn(&mut S, usize) + Sync>(
    t: usize,
    n: usize,
    init: I,
    work: W,
) -> Vec<S> {
    let next = AtomicUsize::new(0);
    std::thread::scope(|sc| {
        let hs: Vec<_> = (0..t)
            .map(|_| {
                sc.spawn(|| {
                    let mut s = init();
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        if i >= n {
                            break;
                        }
                        work(&mut s, i);
                    }
                    s
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    })
}

fn list_csv(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map_or(false, |x| x == "csv") && p.is_file())
            .collect(),
        Err(_) => Vec::new(),
    };
    v.sort();
    v
}

fn map_file(p: &Path) -> Option<Mmap> {
    let f = File::open(p).ok()?;
    let len = f.metadata().ok()?.len();
    if len == 0 {
        return None;
    }
    let m = unsafe { Mmap::map(&f) }.ok()?;
    #[cfg(unix)]
    {
        let _ = m.advise(memmap2::Advice::Sequential);
    }
    Some(m)
}

/// A slice of a mapped file covering whole lines (header already excluded).
#[derive(Clone, Copy)]
struct Chunk {
    file: usize,
    start: usize,
    end: usize,
}

/// Splits every file into fixed byte ranges (no data is touched here). A range owns exactly
/// the lines that start inside it; `chunk_lines` finds them.
fn make_chunks(maps: &[Mmap], kind_of: &[u8], out: &mut Vec<(u8, Chunk)>) {
    for (fi, m) in maps.iter().enumerate() {
        let len = m.len();
        let mut start = 0;
        while start < len {
            let end = (start + CHUNK).min(len);
            out.push((kind_of[fi], Chunk { file: fi, start, end }));
            start = end;
        }
    }
}

/// The whole lines owned by byte range [start, end) of `m`: the first is the one after the
/// first '\n' at or after `start - 1` (for start 0 this skips the header line), the last is
/// the one containing byte `end - 1`.
fn chunk_lines(m: &[u8], c: Chunk) -> &[u8] {
    let from = if c.start == 0 { 0 } else { c.start - 1 };
    let first = match memchr::memchr(b'\n', &m[from..]) {
        Some(p) => from + p + 1,
        None => return &[],
    };
    if first >= c.end {
        return &[];
    }
    let stop = match memchr::memchr(b'\n', &m[c.end - 1..]) {
        Some(q) => c.end + q,
        None => m.len(),
    };
    &m[first..stop]
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn read_small_csv2(path: &Path) -> Vec<(Vec<u8>, Vec<u8>)> {
    let data = std::fs::read(path).unwrap_or_default();
    let body = match memchr::memchr(b'\n', &data) {
        Some(p) => &data[p + 1..],
        None => &[][..],
    };
    let mut out = Vec::new();
    for_each_line::<2, _>(body, |f| {
        if !f[0].is_empty() && !f[1].is_empty() {
            out.push((f[0].to_vec(), f[1].to_vec()));
        }
    });
    out
}

fn main() {
    if std::env::var_os("NOTHP").is_some() {
        USE_THP.store(false, std::sync::atomic::Ordering::Relaxed);
    }
    if let Err(e) = run() {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
    // Skip destructors of the large in-memory structures.
    std::process::exit(0);
}

fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let t0 = Instant::now();
    let lg = |m: &str| eprintln!("[{:.3}s] {m}", t0.elapsed().as_secs_f64());

    let args: Vec<String> = std::env::args().collect();
    let data_dir = args
        .get(1)
        .cloned()
        .or_else(|| std::env::var("DATA_DIR").ok())
        .unwrap_or_else(|| "/data".into());
    let out_dir = args
        .get(2)
        .cloned()
        .or_else(|| std::env::var("OUTPUT_DIR").ok())
        .unwrap_or_else(|| "/output".into());
    let data = Path::new(&data_dir);
    let nt = nthreads();

    // --- 1. small inputs ---------------------------------------------------
    // products.csv parsed in place: categories are few, so a linear scan over the
    // distinct names beats hashing each one; no per-line allocations.
    let mut cat_names: Vec<Vec<u8>> = vec![b"unknown".to_vec()];
    let mut product_cat: KMap<u16> = kmap(1 << 16);
    {
        let pdata = std::fs::read(data.join("products.csv")).unwrap_or_default();
        let body = match memchr::memchr(b'\n', &pdata) {
            Some(p) => &pdata[p + 1..],
            None => &[][..],
        };
        let mut last: Option<u16> = None; // last category seen
        for_each_line::<2, _>(body, |f| {
            if f[0].is_empty() || f[1].is_empty() {
                return;
            }
            let id = match last {
                Some(id) if cat_names[id as usize] == f[1] => id,
                _ => {
                    let id = match cat_names.iter().position(|c| c.as_slice() == f[1]) {
                        Some(i) => i as u16,
                        None => {
                            cat_names.push(f[1].to_vec());
                            (cat_names.len() - 1) as u16
                        }
                    };
                    last = Some(id);
                    id
                }
            };
            product_cat.insert(key_of(f[0]), id);
        });
    }
    // rank of each category by byte-wise name (lower rank wins ties)
    let mut order: Vec<usize> = (0..cat_names.len()).collect();
    order.sort_by(|&a, &b| cat_names[a].cmp(&cat_names[b]));
    let mut cat_rank = vec![0u32; cat_names.len()];
    for (r, &c) in order.iter().enumerate() {
        cat_rank[c] = r as u32;
    }

    let mut camp_names: Vec<Vec<u8>> = Vec::new();
    let mut camp_channels: Vec<Vec<u8>> = Vec::new();
    let mut camp_idx: KMap<u32> = kmap(4096);
    for (cid, ch) in read_small_csv2(&data.join("campaigns.csv")) {
        let k = key_of(&cid);
        if camp_idx.contains_key(&k) {
            continue;
        }
        camp_idx.insert(k, camp_names.len() as u32);
        camp_names.push(cid);
        camp_channels.push(ch);
    }
    let ncamp = camp_names.len();
    let organic = ncamp as u32;

    let manifest: serde_json::Value = serde_json::from_slice(&std::fs::read(data.join("manifest.json"))?)?;
    let ws = manifest["window_start"].as_str().ok_or("window_start")?;
    let we = manifest["window_end"].as_str().ok_or("window_end")?;
    let win_start = parse_ts(ws.as_bytes()).ok_or("bad window_start")?;
    let win_end = parse_ts(we.as_bytes()).ok_or("bad window_end")?;
    let click_start = win_start - WEEK_MS;
    let day0 = win_start.div_euclid(DAY_MS);
    // Timestamps are stored as u32 ms offsets from `base`, categories in 8 bits.
    let base = click_start;
    if win_end - base >= TS_NONE as i64 || win_end <= win_start {
        return Err("window too long for compact timestamps".into());
    }
    if cat_names.len() > 256 {
        return Err("more than 255 product categories".into());
    }
    let ndays = ((win_end - 1).div_euclid(DAY_MS) - day0 + 1).max(1) as usize;

    // --- 2. map all inputs -------------------------------------------------
    let link_files = list_csv(&data.join("links"));
    let event_files = list_csv(&data.join("events"));
    let click_files = list_csv(&data.join("clicks"));

    let mut maps: Vec<Mmap> = Vec::new();
    let mut kinds: Vec<u8> = Vec::new();
    for (k, files) in [(0u8, &link_files), (1, &event_files), (2, &click_files)] {
        for p in files.iter() {
            if let Some(m) = map_file(p) {
                maps.push(m);
                kinds.push(k);
            }
        }
    }
    let mut all_chunks: Vec<(u8, Chunk)> = Vec::new();
    make_chunks(&maps, &kinds, &mut all_chunks);
    let link_chunks: Vec<Chunk> = all_chunks.iter().filter(|c| c.0 == 0).map(|c| c.1).collect();
    let mut data_chunks: Vec<(u8, Chunk)> = all_chunks.into_iter().filter(|c| c.0 != 0).collect();
    // biggest first for better balance
    data_chunks.sort_by_key(|c| std::cmp::Reverse(c.1.end - c.1.start));
    lg(&format!(
        "{} threads, {} link chunks, {} data chunks",
        nt,
        link_chunks.len(),
        data_chunks.len()
    ));

    // --- 3. links -> person names (runs concurrently with event parsing) ----
    const LP: usize = 64;
    let lpart = |h: u64| ((h >> 40) as usize) & (LP - 1);
    let lt = 3usize.min(nt);
    let build_links = || -> PMap {
        // --- 3. links -> person names -------------------------------------------

        // Parse pairs: each endpoint goes to its key partition tagged with its slot
        // (2 * thread-local pair index + side); pairs themselves are only counted.
        let mut link_states = pool(
            lt,
            link_chunks.len(),
            || (0u32, (0..LP).map(|_| Vec::<(Key, u32)>::new()).collect::<Vec<_>>(), LongMap::default()),
            |st, i| {
                let c = link_chunks[i];
                let b = chunk_lines(&maps[c.file], c);
                for_each_line::<2, _>(b, |f| {
                    if f[0].is_empty() || f[1].is_empty() {
                        return;
                    }
                    let a = key_of(f[0]);
                    let bk = key_of(f[1]);
                    note_long(&mut st.2, a, f[0]);
                    note_long(&mut st.2, bk, f[1]);
                    let slot = st.0 * 2;
                    st.0 += 1;
                    st.1[lpart(khash(a))].push((a, slot));
                    st.1[lpart(khash(bk))].push((bk, slot + 1));
                });
            },
        );
        // long link IDs' bytes, needed to pick each person's byte-wise smallest name
        let _ = LINK_LONG.set(link_states.iter_mut().map(|s| std::mem::take(&mut s.2)).collect());
        let mut toff = vec![0usize; link_states.len() + 1];
        for (t, s) in link_states.iter().enumerate() {
            toff[t + 1] = toff[t] + s.0 as usize;
        }
        let npairs = toff[link_states.len()];

        // Per partition: unique keys -> local index; record each endpoint's
        // (partition, local index) at its pair slot while inserting.
        let ends: Vec<AtomicU32> = (0..npairs * 2).map(|_| AtomicU32::new(0)).collect();
        let part_keys: Vec<Mutex<Vec<Key>>> = (0..LP).map(|_| Mutex::new(Vec::new())).collect();
        pool(lt, LP, || (), |_, p| {
            let total: usize = link_states.iter().map(|s| s.1[p].len()).sum();
            let mut m: KMap<u32> = kmap(total / 2 + 16);
            let mut keys = Vec::new();
            for (t, s) in link_states.iter().enumerate() {
                let base = toff[t] * 2;
                for &(k, slot) in &s.1[p] {
                    let local = *m.entry(k).or_insert_with(|| {
                        keys.push(k);
                        (keys.len() - 1) as u32
                    });
                    ends[base + slot as usize].store(((p as u32) << 26) | local, Ordering::Relaxed);
                }
            }
            *part_keys[p].lock().unwrap() = keys;
        });
        let mut part_keys: Vec<Vec<Key>> = part_keys.into_iter().map(|m| m.into_inner().unwrap()).collect();
        let mut offs = vec![0u32; LP + 1];
        for p in 0..LP {
            offs[p + 1] = offs[p] + part_keys[p].len() as u32;
        }
        let nnodes = offs[LP] as usize;

        // union-find
        let mut parent: Vec<u32> = (0..nnodes as u32).collect();
        fn find(parent: &mut [u32], mut x: u32) -> u32 {
            while parent[x as usize] != x {
                let g = parent[parent[x as usize] as usize];
                parent[x as usize] = g;
                x = g;
            }
            x
        }
        let dec = |e: u32| offs[(e >> 26) as usize] + (e & ((1 << 26) - 1));
        for i in 0..npairs {
            let a = dec(ends[2 * i].load(Ordering::Relaxed));
            let b = dec(ends[2 * i + 1].load(Ordering::Relaxed));
            let ra = find(&mut parent, a);
            let rb = find(&mut parent, b);
            if ra != rb {
                if ra < rb {
                    parent[rb as usize] = ra;
                } else {
                    parent[ra as usize] = rb;
                }
            }
        }
        drop(link_states);
        // node keys in global index order
        let mut node_key: Vec<Key> = Vec::with_capacity(nnodes);
        for p in 0..LP {
            node_key.extend_from_slice(&part_keys[p]);
        }
        // Byte-wise smallest ID per component. Compare 15-byte prefixes first (computed once
        // per node; for long IDs this is the only byte lookup) and fall back to the full
        // comparison only when the prefixes tie.
        let prefix: Vec<u128> = node_key.iter().map(|&k| key_prefix(k)).collect();
        let mut root_min: Vec<Key> = vec![Key::default(); nnodes];
        let mut min_prefix: Vec<u128> = vec![0; nnodes];
        let mut has_min = vec![false; nnodes];
        let mut roots: Vec<u32> = vec![0; nnodes];
        for i in 0..nnodes {
            let r = find(&mut parent, i as u32);
            roots[i] = r;
            let k = node_key[i];
            let r = r as usize;
            let better = !has_min[r]
                || match prefix[i].cmp(&min_prefix[r]) {
                    std::cmp::Ordering::Less => true,
                    std::cmp::Ordering::Greater => false,
                    std::cmp::Ordering::Equal => key_cmp(k, root_min[r]) == std::cmp::Ordering::Less,
                };
            if better {
                root_min[r] = k;
                min_prefix[r] = prefix[i];
                has_min[r] = true;
            }
        }
        // person map: user key -> person key (only for linked IDs whose name differs)
        let pmap: PMap = {
            let pk = &part_keys;
            let cells: Vec<Mutex<PSub>> = (0..LP).map(|_| Mutex::new(PSub::new(0))).collect();
            pool(lt, LP, || (), |_, p| {
                let keys = &pk[p];
                let mut m = PSub::new(keys.len());
                for (li, &k) in keys.iter().enumerate() {
                    let gi = offs[p] as usize + li;
                    let name = root_min[roots[gi] as usize];
                    if name != k {
                        m.insert(k, khash(k), name);
                    }
                }
                *cells[p].lock().unwrap() = m;
            });
            PMap { parts: cells.into_iter().map(|c| c.into_inner().unwrap()).collect() }
        };
        part_keys.clear();
        lg(&format!("links done: {} ids", nnodes));
        pmap
    };
    let ev_chunks: Vec<Chunk> = data_chunks.iter().filter(|c| c.0 == 1).map(|c| c.1).collect();
    let ck_chunks: Vec<Chunk> = data_chunks.iter().filter(|c| c.0 == 2).map(|c| c.1).collect();

    // --- 4. parse events + clicks -------------------------------------------
    struct PState {
        ev: Vec<BlockVec<ERec>>,
        long: LongMap, // bytes of long user IDs (for writing person names)
    }
    // Event-id buckets: ~20K events each keeps every dedup hash table cache-resident.
    let ev_bytes: usize = ev_chunks.iter().map(|c| c.end - c.start).sum();
    let ne = (ev_bytes / 85 / 20_000).next_power_of_two().clamp(1024, 8192);
    let ne_mask = ne - 1;
    let (pmap, pstates) = std::thread::scope(|sc| {
        let h = sc.spawn(build_links);
        let pstates = pool(
            nt,
            ev_chunks.len(),
            || PState { ev: (0..ne).map(|_| BlockVec::new()).collect(), long: LongMap::default() },
            |st, i| {
                let c = ev_chunks[i];
                let b = chunk_lines(&maps[c.file], c);
                let end = b.as_ptr() as usize + b.len();
                for_each_line::<7, _>(b, |f| {
                    if f[0].is_empty() {
                        return;
                    }
                    let ingest = match parse_ts(f[3]) {
                        Some(v) => v,
                        None => return,
                    };
                    let eid = key_fast(f[0], end);
                    let mut rec = ERec { eid, user: Key::default(), ingest, ts: TS_NONE, pc: 0 };
                    let ty = f[4];
                    let ok_type = ty == b"view" || ty == b"cart";
                    let purchase = ty == b"purchase";
                    if (ok_type || purchase) && !f[1].is_empty() && !f[5].is_empty() {
                        if let Some(ts) = parse_ts(f[2]) {
                            if ts >= win_start && ts < win_end && ingest - ts <= DAY_MS {
                                let mut valid = true;
                                if purchase {
                                    match parse_price(f[6]) {
                                        Some(p) => {
                                            let cat = product_cat.get(&key_fast(f[5], end)).copied().unwrap_or(0);
                                            rec.pc = (p << 8) | cat as u32;
                                        }
                                        None => valid = false,
                                    }
                                }
                                if valid {
                                    rec.ts = (ts - base) as u32;
                                    rec.user = key_fast(f[1], end);
                                    note_long(&mut st.long, rec.user, f[1]);
                                }
                            }
                        }
                    }
                    st.ev[ebucket(khash(eid), ne_mask)].push(rec);
                });
            },
        );
        (h.join().unwrap(), pstates)
    });
    let mut pstates = pstates;
    lg(&format!("events parsed ({} event buckets)", ne));
    struct CState {
        ck: Vec<BlockVec<CRec>>,
        pend: Vec<(Key, u64, Key, u32, u32)>,
        long: LongMap, // bytes of long click IDs (for click_id tie-breaks)
    }
    #[inline(always)]
    fn flush_clicks(st: &mut CState, pmap: &PMap) {
        for &(user, h, click, ts, camp) in &st.pend {
            let person = pmap.get(user, h);
            st.ck[pbucket(khash(person))].push(CRec { person, click, ts, camp });
        }
        st.pend.clear();
    }
    let cstates = pool(
        nt,
        ck_chunks.len(),
        || CState { ck: (0..NP).map(|_| BlockVec::new()).collect(), pend: Vec::with_capacity(32), long: LongMap::default() },
        |st, i| {
            let c = ck_chunks[i];
            let b = chunk_lines(&maps[c.file], c);
            let end = b.as_ptr() as usize + b.len();
            for_each_line::<4, _>(b, |f| {
                if f[0].is_empty() || f[1].is_empty() {
                    return;
                }
                let camp = match camp_idx.get(&key_fast(f[2], end)) {
                    Some(&c) => c,
                    None => return,
                };
                let ts = match parse_ts(f[3]) {
                    Some(t) if t >= click_start && t < win_end => t,
                    _ => return,
                };
                let user = key_fast(f[1], end);
                let h = khash(user);
                pmap.prefetch(h);
                let click = key_fast(f[0], end);
                note_long(&mut st.long, click, f[0]);
                st.pend.push((user, h, click, (ts - base) as u32, camp));
                if st.pend.len() == 32 {
                    flush_clicks(st, &pmap);
                }
            });
            flush_clicks(st, &pmap);
        },
    );
    let mut cstates = cstates;
    let _ = DATA_LONG.set(
        pstates
            .iter_mut()
            .map(|s| std::mem::take(&mut s.long))
            .chain(cstates.iter_mut().map(|s| std::mem::take(&mut s.long)))
            .collect(),
    );
    lg("parse done");

    // --- 5. dedup events per event bucket -> person buckets -----------------
    let dstates = pool(
        nt,
        ne,
        || ((0..NP).map(|_| BlockVec::<PRec>::new()).collect::<Vec<_>>(), kmap::<usize>(0)),
        |st, b| {
            let (out, table) = st;
            table.clear();
            let total: usize = pstates.iter().map(|s| s.ev[b].len()).sum();
            table.reserve(total);
            // Insert pass: on a collision the copy with the larger ingest_ts is marked dead
            // right away (equal ingest_ts copies are identical, so either may die).
            // Records of bucket b are only touched by this task.
            for s in pstates.iter() {
                for (base, n) in s.ev[b].raw_blocks() {
                    for k in 0..n {
                        unsafe {
                            let rp = base.add(k);
                            match table.entry((*rp).eid) {
                                std::collections::hash_map::Entry::Vacant(v) => {
                                    v.insert(rp as usize);
                                }
                                std::collections::hash_map::Entry::Occupied(mut o) => {
                                    let op = *o.get() as *mut ERec;
                                    if (*rp).ingest < (*op).ingest {
                                        (*op).ts = TS_NONE;
                                        o.insert(rp as usize);
                                    } else {
                                        (*rp).ts = TS_NONE;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            // Sequential scan; resolve persons in batches with prefetching.
            let mut cand: [(*const ERec, u64); 32] = [(std::ptr::null(), 0); 32];
            for src in pstates.iter() {
                let v = &src.ev[b];
                for bi in 0..v.blocks.len() {
                    let blk = v.block(bi);
                    let mut ri = 0usize;
                    while ri < blk.len() {
                        let mut n = 0usize;
                        while ri < blk.len() && n < 32 {
                            let r = &blk[ri];
                            if r.ts != TS_NONE {
                                let h = khash(r.user);
                                pmap.prefetch(h);
                                cand[n] = (r as *const ERec, h);
                                n += 1;
                            }
                            ri += 1;
                        }
                        for &(rp, h) in &cand[..n] {
                            let r = unsafe { &*rp };
                            let person = pmap.get(r.user, h);
                            out[pbucket(khash(person))].push(PRec { person, ts: r.ts, pc: r.pc });
                        }
                    }
                }
            }
            for src in pstates.iter() {
                unsafe { src.ev[b].recycle() };
            }
        },
    );
    lg("dedup done");

    // --- 6. sessionize per person bucket ------------------------------------
    #[derive(Clone, Copy, Default)]
    struct SEv {
        ts: u32,
        pc: u32,
    }
    #[derive(Clone, Copy, Default)]
    struct SClk {
        click: Key,
        ts: u32,
        camp: u32,
    }
    struct SState {
        rows: Vec<URow>,
        camp: Vec<[i64; 4]>, // [sessions, purchasing, revenue, assisted] per (camp, day)
        // sliding click window: clicks per campaign, and campaigns with count > 0
        wcnt: Vec<u32>,
        active: Vec<u32>,
        apos: Vec<u32>,
        catacc: Vec<(u16, i64)>,
        pidx: KMap<u32>,
        pkeys: Vec<Key>,
        evp: Vec<u32>,
        eoff: Vec<u32>,
        coff: Vec<u32>,
        sev: Vec<SEv>,
        sck: Vec<SClk>,
        ckp: Vec<u32>,
    }
    let nci = ncamp + 1;
    let sstates = pool(
        nt,
        NP,
        || SState {
            rows: Vec::new(),
            camp: vec![[0i64; 4]; nci * ndays],
            wcnt: vec![0u32; nci],
            active: Vec::new(),
            apos: vec![0u32; nci],
            catacc: Vec::new(),
            pidx: kmap(1024),
            pkeys: Vec::new(),
            evp: Vec::new(),
            eoff: Vec::new(),
            coff: Vec::new(),
            sev: Vec::new(),
            sck: Vec::new(),
            ckp: Vec::new(),
        },
        |st, b| {
            // dense person index for this bucket + counting sort of events by person
            st.pidx.clear();
            st.pkeys.clear();
            st.evp.clear();
            st.eoff.clear();
            for d in &dstates {
                for r in d.0[b].iter() {
                    let next = st.pkeys.len() as u32;
                    let id = *st.pidx.entry(r.person).or_insert(next);
                    if id == next {
                        st.pkeys.push(r.person);
                        st.eoff.push(0);
                    }
                    st.eoff[id as usize] += 1;
                    st.evp.push(id);
                }
            }
            let np = st.pkeys.len();
            let mut acc = 0u32;
            for x in st.eoff.iter_mut() {
                let c = *x;
                *x = acc;
                acc += c;
            }
            st.eoff.push(acc);
            st.sev.clear();
            st.sev.resize(acc as usize, SEv::default());
            {
                let mut k = 0usize;
                for d in &dstates {
                    for r in d.0[b].iter() {
                        let id = st.evp[k] as usize;
                        k += 1;
                        let pos = st.eoff[id] as usize;
                        st.eoff[id] += 1;
                        st.sev[pos] = SEv { ts: r.ts, pc: r.pc };
                    }
                }
            }
            // eoff[id] now holds the end of id's range; shift back to starts
            for id in (1..=np).rev() {
                st.eoff[id] = st.eoff[id - 1];
            }
            st.eoff[0] = 0;

            // clicks: only for persons with events
            st.coff.clear();
            st.coff.resize(np + 1, 0);
            st.ckp.clear();
            for s in &cstates {
                for c in s.ck[b].iter() {
                    let id = match st.pidx.get(&c.person) {
                        Some(&id) => id,
                        None => u32::MAX,
                    };
                    if id != u32::MAX {
                        st.coff[id as usize] += 1;
                    }
                    st.ckp.push(id);
                }
            }
            let mut acc = 0u32;
            for x in st.coff.iter_mut() {
                let c = *x;
                *x = acc;
                acc += c;
            }
            st.sck.clear();
            st.sck.resize(acc as usize, SClk::default());
            {
                let mut k = 0usize;
                for s in &cstates {
                    for c in s.ck[b].iter() {
                        let id = st.ckp[k];
                        k += 1;
                        if id == u32::MAX {
                            continue;
                        }
                        let pos = st.coff[id as usize] as usize;
                        st.coff[id as usize] += 1;
                        st.sck[pos] = SClk { click: c.click, ts: c.ts, camp: c.camp };
                    }
                }
            }
            for id in (1..=np).rev() {
                st.coff[id] = st.coff[id - 1];
            }
            st.coff[0] = 0;

            let mut sev = std::mem::take(&mut st.sev);
            let mut sck = std::mem::take(&mut st.sck);
            for pid in 0..np {
                let person = st.pkeys[pid];
                let pev_m = &mut sev[st.eoff[pid] as usize..st.eoff[pid + 1] as usize];
                if pev_m.len() > 1 {
                    pev_m.sort_unstable_by_key(|e| e.ts);
                }
                let pck_full = &mut sck[st.coff[pid] as usize..st.coff[pid + 1] as usize];
                if pck_full.len() > 1 {
                    pck_full.sort_unstable_by(|a, b| a.ts.cmp(&b.ts).then_with(|| key_cmp(a.click, b.click)));
                }
                // dedup identical click ids (copies have identical ts, so they are adjacent)
                let mut w = 0usize;
                for r in 0..pck_full.len() {
                    if w > 0 && pck_full[w - 1].click == pck_full[r].click && pck_full[w - 1].ts == pck_full[r].ts {
                        continue;
                    }
                    pck_full[w] = pck_full[r];
                    w += 1;
                }
                let pck = &pck_full[..w];
                let pev: &[SEv] = &sev[st.eoff[pid] as usize..st.eoff[pid + 1] as usize];
                let mut cur_day: i64 = i64::MIN;
                let mut drow = URow {
                    person,
                    day: 0,
                    sessions: 0,
                    events: 0,
                    revenue: 0,
                    top: -1,
                };
                st.catacc.clear();
                let mut lo = 0usize; // first click with ts >= start - 7d
                let mut hi = 0usize; // first click with ts > start

                let mut j = 0usize;
                let m = pev.len();
                while j < m {
                    let start = pev[j].ts as i64;
                    let mut last = start;
                    let mut cnt = 0i64;
                    let mut rev = 0i64;
                    let mut purch = false;
                    let js = j;
                    while j < m && pev[j].ts as i64 - last <= GAP_MS {
                        last = pev[j].ts as i64;
                        cnt += 1;
                        let price = pev[j].pc >> 8;
                        if price != 0 {
                            purch = true;
                            rev += price as i64;
                        }
                        j += 1;
                    }
                    let day = (base + start).div_euclid(DAY_MS);
                    if day != cur_day {
                        if cur_day != i64::MIN {
                            drow.top = top_cat(&st.catacc, &cat_rank);
                            st.rows.push(std::mem::replace(
                                &mut drow,
                                URow { person, day: 0, sessions: 0, events: 0, revenue: 0, top: -1 },
                            ));
                            st.catacc.clear();
                        }
                        cur_day = day;
                        drow.day = day as i32;
                    }
                    drow.sessions += 1;
                    drow.events += cnt;
                    drow.revenue += rev;
                    if purch {
                        for e in &pev[js..j] {
                            let price = (e.pc >> 8) as i64;
                            if price != 0 {
                                let cat = (e.pc & 0xFF) as u16;
                                match st.catacc.iter_mut().find(|x| x.0 == cat) {
                                    Some(x) => x.1 += price,
                                    None => st.catacc.push((cat, price)),
                                }
                            }
                        }
                    }

                    // clicks window [start - 7d, start]; session starts are increasing
                    // [lo, hi) is maintained incrementally, together with the set of campaigns
                    // that have a click in it (each click enters and leaves exactly once).
                    let wlo = start - WEEK_MS;
                    while lo < pck.len() && (pck[lo].ts as i64) < wlo {
                        if lo < hi {
                            let c = pck[lo].camp as usize;
                            st.wcnt[c] -= 1;
                            if st.wcnt[c] == 0 {
                                let p = st.apos[c] as usize;
                                let last = st.active.pop().unwrap();
                                if p < st.active.len() {
                                    st.active[p] = last;
                                    st.apos[last as usize] = p as u32;
                                }
                            }
                        }
                        lo += 1;
                    }
                    if hi < lo {
                        hi = lo;
                    }
                    while hi < pck.len() && (pck[hi].ts as i64) <= start {
                        let c = pck[hi].camp as usize;
                        if st.wcnt[c] == 0 {
                            st.apos[c] = st.active.len() as u32;
                            st.active.push(c as u32);
                        }
                        st.wcnt[c] += 1;
                        hi += 1;
                    }
                    let di = (day - day0) as usize;
                    let credited = if hi > lo {
                        // latest ts; ties -> smallest click id (sorted ascending within ts)
                        let mts = pck[hi - 1].ts;
                        let mut k = hi - 1;
                        while k > lo && pck[k - 1].ts == mts {
                            k -= 1;
                        }
                        pck[k].camp
                    } else {
                        organic
                    };
                    let row = di * nci;
                    let a = &mut st.camp[row + credited as usize];
                    a[0] += 1;
                    a[1] += purch as i64;
                    a[2] += rev;
                    // assists: every campaign with a click in the window, once
                    for &c in &st.active {
                        st.camp[row + c as usize][3] += 1;
                    }
                }
                // reset the window state for the next person
                for &c in &st.active {
                    st.wcnt[c as usize] = 0;
                }
                st.active.clear();
                if cur_day != i64::MIN {
                    drow.top = top_cat(&st.catacc, &cat_rank);
                    st.rows.push(drow);
                }
            }
            st.sev = sev;
            st.sck = sck;
        },
    );
    lg("sessionize done");

    // --- 7. write outputs ---------------------------------------------------
    let user_dir = Path::new(&out_dir).join("user_daily");
    let camp_dir = Path::new(&out_dir).join("campaign_daily");
    std::fs::create_dir_all(&user_dir)?;
    std::fs::create_dir_all(&camp_dir)?;
    let props = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_statistics_enabled(EnabledStatistics::None)
        // low-cardinality columns: dictionary + bit-packing makes them a few bits per row
        .set_column_dictionary_enabled(ColumnPath::from("day"), true)
        .set_column_dictionary_enabled(ColumnPath::from("top_category"), true)
        .build();

    let user_schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int32, false),
        Field::new("events", DataType::Int32, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("top_category", DataType::Utf8, true),
    ]));
    let cat_strs: Vec<String> = cat_names.iter().map(|c| String::from_utf8_lossy(c).into_owned()).collect();

    let errs: Vec<Option<String>> = pool(nt, sstates.len() + 1, Vec::new, |errs: &mut Vec<Option<String>>, i| {
        let r: Result<(), Box<dyn std::error::Error + Send + Sync>> = (|| {
            if i == sstates.len() {
                // campaign_daily
                let mut agg = vec![[0i64; 4]; nci * ndays];
                for s in &sstates {
                    for (a, b) in agg.iter_mut().zip(s.camp.iter()) {
                        for k in 0..4 {
                            a[k] += b[k];
                        }
                    }
                }
                let mut idb = StringBuilder::new();
                let mut chb = StringBuilder::new();
                let (mut d, mut s0, mut s1, mut s2, mut s3) = (vec![], vec![], vec![], vec![], vec![]);
                for c in 0..nci {
                    for di in 0..ndays {
                        let a = agg[di * nci + c];
                        if a[0] == 0 && a[3] == 0 {
                            continue;
                        }
                        if c == ncamp {
                            idb.append_value("organic");
                            chb.append_value("organic");
                        } else {
                            idb.append_value(String::from_utf8_lossy(&camp_names[c]));
                            chb.append_value(String::from_utf8_lossy(&camp_channels[c]));
                        }
                        d.push((day0 + di as i64) as i32);
                        s0.push(a[0]);
                        s1.push(a[1]);
                        s2.push(a[2]);
                        s3.push(a[3]);
                    }
                }
                let schema = Arc::new(Schema::new(vec![
                    Field::new("campaign_id", DataType::Utf8, false),
                    Field::new("channel", DataType::Utf8, false),
                    Field::new("day", DataType::Date32, false),
                    Field::new("sessions", DataType::Int64, false),
                    Field::new("purchasing_sessions", DataType::Int64, false),
                    Field::new("revenue_cents", DataType::Int64, false),
                    Field::new("assisted_sessions", DataType::Int64, false),
                ]));
                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(idb.finish()) as ArrayRef,
                        Arc::new(chb.finish()),
                        Arc::new(Date32Array::from(d)),
                        Arc::new(Int64Array::from(s0)),
                        Arc::new(Int64Array::from(s1)),
                        Arc::new(Int64Array::from(s2)),
                        Arc::new(Int64Array::from(s3)),
                    ],
                )?;
                let f = File::create(camp_dir.join("part-0.parquet"))?;
                let mut w = ArrowWriter::try_new(f, schema, Some(props.clone()))?;
                w.write(&batch)?;
                w.close()?;
                return Ok(());
            }
            let rows = &sstates[i].rows;
            if rows.is_empty() && i != 0 {
                return Ok(());
            }
            let n = rows.len();
            let mut ub = StringBuilder::with_capacity(n, n * 10);
            let mut tb = StringBuilder::with_capacity(n, n * 4);
            let mut d = Vec::with_capacity(n);
            let mut s = Vec::with_capacity(n);
            let mut e = Vec::with_capacity(n);
            let mut rv = Vec::with_capacity(n);
            let mut tmp = Vec::with_capacity(32);
            for r in rows {
                tmp.clear();
                write_key(r.person, &mut tmp);
                ub.append_value(std::str::from_utf8(&tmp).map_err(|_| "non-utf8 user id")?);
                d.push(r.day);
                s.push(r.sessions as i32);
                e.push(r.events as i32);
                rv.push(r.revenue);
                if r.top >= 0 {
                    tb.append_value(&cat_strs[r.top as usize]);
                } else {
                    tb.append_null();
                }
            }
            let batch = RecordBatch::try_new(
                user_schema.clone(),
                vec![
                    Arc::new(ub.finish()) as ArrayRef,
                    Arc::new(Date32Array::from(d)),
                    Arc::new(Int32Array::from(s)),
                    Arc::new(Int32Array::from(e)),
                    Arc::new(Int64Array::from(rv)),
                    Arc::new(tb.finish()),
                ],
            )?;
            let f = File::create(user_dir.join(format!("part-{i}.parquet")))?;
            let mut w = ArrowWriter::try_new(f, user_schema.clone(), Some(props.clone()))?;
            w.write(&batch)?;
            w.close()?;
            Ok(())
        })();
        errs.push(r.err().map(|e| e.to_string()));
    })
    .into_iter()
    .flatten()
    .collect();
    for e in errs.into_iter().flatten() {
        return Err(e.into());
    }
    lg("write done");
    // Exit right here: dropping the multi-GB working set would cost ~0.2 s.
    std::process::exit(0);
}

#[inline]
fn top_cat(acc: &[(u16, i64)], rank: &[u32]) -> i32 {
    let mut best: i32 = -1;
    let mut best_rev = 0i64;
    for &(c, r) in acc {
        if r <= 0 {
            continue;
        }
        if best < 0 || r > best_rev || (r == best_rev && rank[c as usize] < rank[best as usize]) {
            best = c as i32;
            best_rev = r;
        }
    }
    best
}

#[cfg(test)]
mod tests {
    use super::*;

    // The original per-byte parser, kept as a reference for differential testing.
    fn ref_civil_secs(s: &[u8]) -> Option<i64> {
        if s[4] != b'-' || s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
            return None;
        }
        let y = d2(s, 0)? * 100 + d2(s, 2)?;
        let m = d2(s, 5)?;
        let d = d2(s, 8)?;
        let h = d2(s, 11)?;
        let mi = d2(s, 14)?;
        let se = d2(s, 17)?;
        if m == 0 || m > 12 || d == 0 || h > 23 || mi > 59 || se > 59 {
            return None;
        }
        let leap = (y % 4 == 0) && (y % 100 != 0 || y % 400 == 0);
        let dim = match m {
            2 => if leap { 29 } else { 28 },
            4 | 6 | 9 | 11 => 30,
            _ => 31,
        };
        if d > dim {
            return None;
        }
        Some(days_from_civil(y as i32, m, d) * 86400 + (h * 3600 + mi * 60 + se) as i64)
    }

    fn ref_parse_ts(s: &[u8]) -> Option<i64> {
        let n = s.len();
        if n == 20 && s[19] == b'Z' {
            return ref_civil_secs(s).map(|v| v * 1000);
        }
        if n == 25 && (s[19] == b'+' || s[19] == b'-') && s[22] == b':' {
            let base = ref_civil_secs(s)?;
            let oh = d2(s, 20)?;
            let om = d2(s, 23)?;
            if oh > 23 || om > 59 {
                return None;
            }
            let off = (oh * 3600 + om * 60) as i64;
            return Some(base - if s[19] == b'+' { off } else { -off }) .map(|v| v * 1000);
        }
        if n == 0 {
            return None;
        }
        let mut v: i64 = 0;
        for &b in s {
            let d = b.wrapping_sub(b'0');
            if d > 9 {
                return None;
            }
            v = v.checked_mul(10)?.checked_add(d as i64)?;
        }
        Some(v)
    }

    #[test]
    fn swar_timestamps_match_reference() {
        let seeds: [&[u8]; 8] = [
            b"2026-10-03T23:58:10Z",
            b"2026-10-03T19:58:10-04:00",
            b"2028-02-29T00:00:00Z",
            b"2027-02-29T00:00:00Z",
            b"2026-12-31T24:00:00Z",
            b"2026-01-01T00:00:60+05:60",
            b"1791071890000",
            b"0000-00-00T00:00:00+24:00",
        ];
        let alphabet = b"0123456789-T:Z+zt \".";
        let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut rnd = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        let mut checked = 0;
        for seed in seeds {
            assert_eq!(parse_ts(seed), ref_parse_ts(seed), "{:?}", std::str::from_utf8(seed));
            for _ in 0..250_000 {
                let mut v = seed.to_vec();
                for _ in 0..(rnd() % 3 + 1) {
                    let i = (rnd() as usize) % v.len();
                    v[i] = alphabet[(rnd() as usize) % alphabet.len()];
                }
                assert_eq!(parse_ts(&v), ref_parse_ts(&v), "{:?}", String::from_utf8_lossy(&v));
                checked += 1;
            }
        }
        // every valid civil date/time over a range of years, months and days
        for y in [1970u32, 2000, 2024, 2025, 2026, 2027, 2028, 2100, 2400, 9999] {
            for m in 0..=13u32 {
                for d in 0..=32u32 {
                    let t = format!("{:04}-{:02}-{:02}T12:34:56Z", y, m, d);
                    assert_eq!(parse_ts(t.as_bytes()), ref_parse_ts(t.as_bytes()), "{t}");
                }
            }
        }
        assert!(checked > 1_000_000);
    }

    // Naive reference for the CSV dialect: split on '\n' and ',', each field either has no
    // quotes or is wrapped in exactly one pair of quotes with none inside.
    fn ref_lines<const N: usize>(buf: &[u8]) -> Vec<Vec<Vec<u8>>> {
        let mut out = Vec::new();
        for line in buf.split(|&c| c == b'\n') {
            if line.is_empty() {
                continue;
            }
            let parts: Vec<&[u8]> = line.split(|&c| c == b',').collect();
            if parts.len() != N {
                continue;
            }
            let mut fields = Vec::new();
            let mut ok = true;
            for p in parts {
                let nq = p.iter().filter(|&&c| c == b'"').count();
                if nq == 0 {
                    fields.push(p.to_vec());
                } else if nq == 2 && p.len() >= 2 && p[0] == b'"' && p[p.len() - 1] == b'"' {
                    fields.push(p[1..p.len() - 1].to_vec());
                } else {
                    ok = false;
                    break;
                }
            }
            if ok {
                out.push(fields);
            }
        }
        out
    }

    fn check_tokenizer<const N: usize>(seed: u64) {
        let mut x = seed;
        let mut rnd = move || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        let alphabet = b"abcdefgh01234,,,,,\"\" \r";
        for _ in 0..3000 {
            let mut buf = Vec::new();
            for _ in 0..(rnd() % 12) {
                let len = match rnd() % 4 {
                    0 => (rnd() % 200) as usize, // includes > 128 (slow path)
                    _ => (rnd() % 100) as usize,
                };
                let mut line: Vec<u8> = (0..len).map(|_| alphabet[(rnd() as usize) % alphabet.len()]).collect();
                if rnd() % 3 == 0 {
                    // a well-formed line, sometimes with quoted fields
                    line = (0..N)
                        .map(|_| {
                            let v: Vec<u8> = (0..(rnd() % 12)).map(|_| b'a' + (rnd() % 26) as u8).collect();
                            if rnd() % 5 == 0 { [&b"\""[..], &v, &b"\""[..]].concat() } else { v }
                        })
                        .collect::<Vec<_>>()
                        .join(&b',');
                }
                buf.extend_from_slice(&line);
                buf.push(b'\n');
            }
            if rnd() % 2 == 0 {
                buf.pop(); // last line without trailing newline
            }
            let mut got: Vec<Vec<Vec<u8>>> = Vec::new();
            for_each_line::<N, _>(&buf, |f| got.push(f.iter().map(|x| x.to_vec()).collect()));
            assert_eq!(got, ref_lines::<N>(&buf), "buf {:?}", String::from_utf8_lossy(&buf));
        }
    }

    #[test]
    fn tokenizer_matches_reference() {
        for seed in 1..=20u64 {
            check_tokenizer::<2>(seed * 7919);
            check_tokenizer::<4>(seed * 104729);
            check_tokenizer::<7>(seed * 1299709);
        }
    }
    #[test]
    fn chunk_lines_partition_every_line_exactly_once() {
        let mut x: u64 = 0x1234_5678_9ABC_DEF1;
        let mut rnd = move || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        for _ in 0..20_000 {
            let mut buf = Vec::new();
            for _ in 0..(rnd() % 10) {
                for _ in 0..(rnd() % 30) {
                    buf.push(b'a' + (rnd() % 3) as u8);
                }
                buf.push(b'\n');
            }
            if rnd() % 2 == 0 && !buf.is_empty() {
                buf.pop();
            }
            if buf.is_empty() {
                continue;
            }
            let expect: &[u8] = match buf.iter().position(|&c| c == b'\n') {
                Some(p) => &buf[p + 1..],
                None => &[],
            };
            let size = 1 + (rnd() % 40) as usize;
            let mut got = Vec::new();
            let mut start = 0;
            while start < buf.len() {
                let end = (start + size).min(buf.len());
                got.extend_from_slice(chunk_lines(&buf, Chunk { file: 0, start, end }));
                start = end;
            }
            assert_eq!(got, expect, "size {size} buf {:?}", String::from_utf8_lossy(&buf));
        }
    }
    #[test]
    fn long_id_keys_do_not_collide() {
        let mut x: u64 = 0xDEAD_BEEF_1234_5678;
        let mut rnd = move || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        let mut seen = std::collections::HashSet::with_capacity(3_000_000);
        for i in 0..3_000_000u64 {
            let prefix = [b'u', b'e', b'k', b'U'][(i % 4) as usize];
            // random 32-hex-digit suffix, and every 16th one differs from the previous
            // value in a single digit (near-identical inputs)
            let (h, l) = (rnd(), rnd());
            let mut id = format!("{}{:016x}{:016x}", prefix as char, h, l).into_bytes();
            if i % 16 == 0 {
                let j = 1 + (rnd() % 32) as usize;
                id[j] = if id[j] == b'0' { b'1' } else { b'0' };
            }
            let k = key_of(&id);
            assert!(k.is_long());
            seen.insert((k.hi, k.lo, id));
        }
        let keys: std::collections::HashSet<(u64, u64)> = seen.iter().map(|t| (t.0, t.1)).collect();
        let ids: std::collections::HashSet<&Vec<u8>> = seen.iter().map(|t| &t.2).collect();
        assert_eq!(keys.len(), ids.len(), "hash collision among long IDs");
        // short keys never look long
        for n in 0..=15usize {
            let v = vec![0xFFu8; n];
            assert!(!key_of(&v).is_long());
        }
    }
}
