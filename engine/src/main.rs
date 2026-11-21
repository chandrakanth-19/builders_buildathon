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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use arrow::array::{ArrayRef, Date32Array, Int64Array, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use memmap2::Mmap;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::{EnabledStatistics, WriterProperties};

const DAY_MS: i64 = 86_400_000;
const WEEK_MS: i64 = 7 * DAY_MS;
const GAP_MS: i64 = 1_800_000;
const CHUNK: usize = 2 << 20;

const E_BITS: u32 = 10; // event-id buckets
const P_BITS: u32 = 10; // person buckets
const NE: usize = 1 << E_BITS;
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

struct LongTable {
    map: HashMap<Vec<u8>, u64>,
    strs: Vec<Vec<u8>>,
}

static LONG: OnceLock<Mutex<LongTable>> = OnceLock::new();

fn long_table() -> &'static Mutex<LongTable> {
    LONG.get_or_init(|| {
        Mutex::new(LongTable {
            map: HashMap::new(),
            strs: Vec::new(),
        })
    })
}

#[cold]
#[inline(never)]
fn long_key(b: &[u8]) -> Key {
    let mut t = long_table().lock().unwrap();
    let id = match t.map.get(b) {
        Some(&id) => id,
        None => {
            let id = t.strs.len() as u64;
            t.strs.push(b.to_vec());
            t.map.insert(b.to_vec(), id);
            id
        }
    };
    let mut h = [0u8; 8];
    h.copy_from_slice(&b[..8]);
    Key {
        hi: u64::from_be_bytes(h),
        lo: (id << 8) | 0xFF,
    }
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

fn key_bytes(k: Key) -> Vec<u8> {
    if k.is_long() {
        let t = long_table().lock().unwrap();
        return t.strs[(k.lo >> 8) as usize].clone();
    }
    let mut buf = [0u8; 16];
    buf[0..8].copy_from_slice(&k.hi.to_be_bytes());
    buf[8..16].copy_from_slice(&k.lo.to_be_bytes());
    let n = buf[15] as usize;
    buf[..n].to_vec()
}

#[inline(always)]
fn write_key(k: Key, out: &mut Vec<u8>) {
    if k.is_long() {
        out.extend_from_slice(&key_bytes(k));
        return;
    }
    let mut buf = [0u8; 16];
    buf[0..8].copy_from_slice(&k.hi.to_be_bytes());
    buf[8..16].copy_from_slice(&k.lo.to_be_bytes());
    let n = buf[15] as usize;
    out.extend_from_slice(&buf[..n]);
}

/// Byte-wise string order of the IDs behind two keys.
#[inline(always)]
fn key_cmp(a: Key, b: Key) -> std::cmp::Ordering {
    if !a.is_long() && !b.is_long() {
        a.cmp(&b)
    } else {
        key_bytes(a).cmp(&key_bytes(b))
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
fn ebucket(h: u64) -> usize {
    ((h >> 40) as usize) & (NE - 1)
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

// ---------------------------------------------------------------------------
// CSV tokenizer: iterates lines of a chunk, yielding exactly-N unquoted fields.
// ---------------------------------------------------------------------------

/// Calls `f(fields)` for every well-formed line of `buf` with exactly N fields.
/// Malformed lines (wrong field count, bad quotes, empty) are skipped.
#[inline(always)]
fn for_each_line<const N: usize, F: FnMut(&[&[u8]; N])>(buf: &[u8], mut f: F) {
    let mut fields: [&[u8]; N] = [&[]; N];
    let mut line_start = 0usize;
    let mut fstart = 0usize;
    let mut fidx = 0usize;
    let mut bad = false;
    let mut qn = 0u32;
    let mut q0 = 0usize;
    let mut q1 = 0usize;

    macro_rules! end_field {
        ($e:expr) => {{
            let e = $e;
            if fidx >= N {
                bad = true;
            } else if qn == 0 {
                fields[fidx] = &buf[fstart..e];
            } else if qn == 2 && q0 == fstart && q1 + 1 == e && e - fstart >= 2 {
                fields[fidx] = &buf[fstart + 1..e - 1];
            } else {
                bad = true;
            }
            fidx += 1;
            qn = 0;
        }};
    }

    for p in memchr::memchr3_iter(b',', b'\n', b'"', buf) {
        match buf[p] {
            b',' => {
                end_field!(p);
                fstart = p + 1;
            }
            b'"' => {
                if qn == 0 {
                    q0 = p;
                } else {
                    q1 = p;
                }
                qn += 1;
            }
            _ => {
                // newline
                if p > line_start {
                    end_field!(p);
                    if !bad && fidx == N {
                        f(&fields);
                    }
                }
                line_start = p + 1;
                fstart = p + 1;
                fidx = 0;
                bad = false;
                qn = 0;
            }
        }
    }
    if buf.len() > line_start {
        end_field!(buf.len());
        if !bad && fidx == N {
            f(&fields);
        }
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

#[inline(always)]
fn civil_secs(s: &[u8]) -> Option<i64> {
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
// Records
// ---------------------------------------------------------------------------

const F_VALID: u8 = 1;
const F_PURCHASE: u8 = 2;

#[derive(Clone, Copy)]
struct ERec {
    eid: Key,
    user: Key,
    ts: i64,
    ingest: i64,
    price: u32,
    cat: u16,
    flags: u8,
}

#[derive(Clone, Copy)]
struct PRec {
    person: Key,
    ts: i64,
    price: u32,
    cat: u16,
    flags: u8,
}

#[derive(Clone, Copy)]
struct CRec {
    person: Key,
    click: Key,
    ts: i64,
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

fn make_chunks(maps: &[Mmap], kind_of: &[u8], out: &mut Vec<(u8, Chunk)>) {
    for (fi, m) in maps.iter().enumerate() {
        let b = &m[..];
        // skip header
        let mut start = match memchr::memchr(b'\n', b) {
            Some(p) => p + 1,
            None => continue,
        };
        while start < b.len() {
            let mut end = (start + CHUNK).min(b.len());
            if end < b.len() {
                end = match memchr::memchr(b'\n', &b[end..]) {
                    Some(p) => end + p + 1,
                    None => b.len(),
                };
            }
            out.push((kind_of[fi], Chunk { file: fi, start, end }));
            start = end;
        }
    }
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
    let mut cat_names: Vec<Vec<u8>> = vec![b"unknown".to_vec()];
    let mut cat_ids: HashMap<Vec<u8>, u16> = HashMap::new();
    cat_ids.insert(b"unknown".to_vec(), 0);
    let mut product_cat: KMap<u16> = kmap(1 << 16);
    for (pid, cat) in read_small_csv2(&data.join("products.csv")) {
        let id = match cat_ids.get(&cat) {
            Some(&id) => id,
            None => {
                let id = cat_names.len() as u16;
                cat_names.push(cat.clone());
                cat_ids.insert(cat, id);
                id
            }
        };
        product_cat.insert(key_of(&pid), id);
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

    // --- 3. links -> person names -------------------------------------------
    const LP: usize = 64;
    let lpart = |h: u64| ((h >> 40) as usize) & (LP - 1);

    // parse pairs; also scatter endpoint keys by partition
    let link_states = pool(
        nt,
        link_chunks.len(),
        || (Vec::<(Key, Key)>::new(), (0..LP).map(|_| Vec::<Key>::new()).collect::<Vec<_>>()),
        |st, i| {
            let c = link_chunks[i];
            let b = &maps[c.file][c.start..c.end];
            for_each_line::<2, _>(b, |f| {
                if f[0].is_empty() || f[1].is_empty() {
                    return;
                }
                let a = key_of(f[0]);
                let bk = key_of(f[1]);
                st.0.push((a, bk));
                st.1[lpart(khash(a))].push(a);
                st.1[lpart(khash(bk))].push(bk);
            });
        },
    );

    // per partition: unique keys -> local index
    let part_keys: Vec<Mutex<(KMap<u32>, Vec<Key>)>> = (0..LP).map(|_| Mutex::new((kmap(0), Vec::new()))).collect();
    pool(nt, LP, || (), |_, p| {
        let total: usize = link_states.iter().map(|s| s.1[p].len()).sum();
        let mut m: KMap<u32> = kmap(total / 2 + 16);
        let mut keys = Vec::new();
        for s in &link_states {
            for &k in &s.1[p] {
                m.entry(k).or_insert_with(|| {
                    keys.push(k);
                    (keys.len() - 1) as u32
                });
            }
        }
        *part_keys[p].lock().unwrap() = (m, keys);
    });
    let mut part_keys: Vec<(KMap<u32>, Vec<Key>)> = part_keys.into_iter().map(|m| m.into_inner().unwrap()).collect();
    let mut offs = vec![0u32; LP + 1];
    for p in 0..LP {
        offs[p + 1] = offs[p] + part_keys[p].1.len() as u32;
    }
    let nnodes = offs[LP] as usize;
    let gidx = |pk: &Vec<(KMap<u32>, Vec<Key>)>, k: Key| -> u32 {
        let p = lpart(khash(k));
        offs[p] + pk[p].0[&k]
    };

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
    {
        let pk = &part_keys;
        let idx_pairs: Vec<Vec<(u32, u32)>> = pool(nt, link_states.len(), Vec::new, |v: &mut Vec<Vec<(u32, u32)>>, i| {
            v.push(link_states[i].0.iter().map(|&(a, b)| (gidx(pk, a), gidx(pk, b))).collect());
        })
        .into_iter()
        .flatten()
        .collect();
        for v in &idx_pairs {
            for &(a, b) in v {
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
        }
    }
    drop(link_states);
    // node keys in global index order
    let mut node_key: Vec<Key> = Vec::with_capacity(nnodes);
    for p in 0..LP {
        node_key.extend_from_slice(&part_keys[p].1);
    }
    let mut root_min: Vec<Key> = vec![Key::default(); nnodes];
    let mut has_min = vec![false; nnodes];
    let mut roots: Vec<u32> = vec![0; nnodes];
    for i in 0..nnodes {
        let r = find(&mut parent, i as u32);
        roots[i] = r;
        let k = node_key[i];
        let r = r as usize;
        if !has_min[r] || key_cmp(k, root_min[r]) == std::cmp::Ordering::Less {
            root_min[r] = k;
            has_min[r] = true;
        }
    }
    // person maps: user key -> person key (only for linked IDs whose person differs)
    let person_maps: Vec<KMap<Key>> = {
        let pk = &part_keys;
        let cells: Vec<Mutex<KMap<Key>>> = (0..LP).map(|_| Mutex::new(kmap(0))).collect();
        pool(nt, LP, || (), |_, p| {
            let (_, keys) = &pk[p];
            let mut m: KMap<Key> = kmap(keys.len());
            for (li, &k) in keys.iter().enumerate() {
                let gi = offs[p] as usize + li;
                let name = root_min[roots[gi] as usize];
                if name != k {
                    m.insert(k, name);
                }
            }
            *cells[p].lock().unwrap() = m;
        });
        cells.into_iter().map(|c| c.into_inner().unwrap()).collect()
    };
    part_keys.clear();
    let person_of = |u: Key| -> Key {
        match person_maps[lpart(khash(u))].get(&u) {
            Some(&p) => p,
            None => u,
        }
    };
    lg(&format!("links done: {} ids", nnodes));

    // --- 4. parse events + clicks -------------------------------------------
    struct PState {
        ev: Vec<Vec<ERec>>,
        ck: Vec<Vec<CRec>>,
    }
    let pstates = pool(
        nt,
        data_chunks.len(),
        || PState {
            ev: (0..NE).map(|_| Vec::new()).collect(),
            ck: (0..NP).map(|_| Vec::new()).collect(),
        },
        |st, i| {
            let (kind, c) = data_chunks[i];
            let b = &maps[c.file][c.start..c.end];
            if kind == 1 {
                for_each_line::<7, _>(b, |f| {
                    if f[0].is_empty() {
                        return;
                    }
                    let ingest = match parse_ts(f[3]) {
                        Some(v) => v,
                        None => return,
                    };
                    let eid = key_of(f[0]);
                    let mut rec = ERec {
                        eid,
                        user: Key::default(),
                        ts: 0,
                        ingest,
                        price: 0,
                        cat: 0,
                        flags: 0,
                    };
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
                                            rec.price = p;
                                            rec.cat = product_cat.get(&key_of(f[5])).copied().unwrap_or(0);
                                            rec.flags |= F_PURCHASE;
                                        }
                                        None => valid = false,
                                    }
                                }
                                if valid {
                                    rec.flags |= F_VALID;
                                    rec.ts = ts;
                                    rec.user = key_of(f[1]);
                                }
                            }
                        }
                    }
                    st.ev[ebucket(khash(eid))].push(rec);
                });
            } else {
                for_each_line::<4, _>(b, |f| {
                    if f[0].is_empty() || f[1].is_empty() {
                        return;
                    }
                    let camp = match camp_idx.get(&key_of(f[2])) {
                        Some(&c) => c,
                        None => return,
                    };
                    let ts = match parse_ts(f[3]) {
                        Some(t) if t >= click_start && t < win_end => t,
                        _ => return,
                    };
                    let person = person_of(key_of(f[1]));
                    st.ck[pbucket(khash(person))].push(CRec {
                        person,
                        click: key_of(f[0]),
                        ts,
                        camp,
                    });
                });
            }
        },
    );
    lg("parse done");

    // --- 5. dedup events per event bucket -> person buckets -----------------
    let dstates = pool(
        nt,
        NE,
        || ((0..NP).map(|_| Vec::<PRec>::new()).collect::<Vec<_>>(), kmap::<(i64, u32, u32)>(0)),
        |st, b| {
            let (out, table) = st;
            table.clear();
            let total: usize = pstates.iter().map(|s| s.ev[b].len()).sum();
            table.reserve(total);
            for (si, s) in pstates.iter().enumerate() {
                for (ri, r) in s.ev[b].iter().enumerate() {
                    match table.entry(r.eid) {
                        std::collections::hash_map::Entry::Vacant(v) => {
                            v.insert((r.ingest, si as u32, ri as u32));
                        }
                        std::collections::hash_map::Entry::Occupied(mut o) => {
                            if r.ingest < o.get().0 {
                                o.insert((r.ingest, si as u32, ri as u32));
                            }
                        }
                    }
                }
            }
            for &(_, si, ri) in table.values() {
                let r = &pstates[si as usize].ev[b][ri as usize];
                if r.flags & F_VALID == 0 {
                    continue;
                }
                let person = person_of(r.user);
                out[pbucket(khash(person))].push(PRec {
                    person,
                    ts: r.ts,
                    price: r.price,
                    cat: r.cat,
                    flags: r.flags,
                });
            }
        },
    );
    lg("dedup done");

    // --- 6. sessionize per person bucket ------------------------------------
    struct SState {
        rows: Vec<URow>,
        camp: Vec<[i64; 4]>, // [sessions, purchasing, revenue, assisted] per (camp, day)
        stamp: Vec<u32>,
        sess_ctr: u32,
        ev: Vec<PRec>,
        ck: Vec<CRec>,
        catacc: Vec<(u16, i64)>,
    }
    let nci = ncamp + 1;
    let sstates = pool(
        nt,
        NP,
        || SState {
            rows: Vec::new(),
            camp: vec![[0i64; 4]; nci * ndays],
            stamp: vec![0u32; nci],
            sess_ctr: 0,
            ev: Vec::new(),
            ck: Vec::new(),
            catacc: Vec::new(),
        },
        |st, b| {
            let mut ev = std::mem::take(&mut st.ev);
            let mut ck = std::mem::take(&mut st.ck);
            ev.clear();
            ck.clear();
            for d in &dstates {
                ev.extend_from_slice(&d.0[b]);
            }
            for s in &pstates {
                ck.extend_from_slice(&s.ck[b]);
            }
            ev.sort_unstable_by(|a, b| (a.person, a.ts).cmp(&(b.person, b.ts)));
            ck.sort_unstable_by(|a, b| {
                (a.person, a.ts)
                    .cmp(&(b.person, b.ts))
                    .then_with(|| key_cmp(a.click, b.click))
            });
            ck.dedup_by(|a, b| a.person == b.person && a.click == b.click);

            let mut ci = 0usize;
            let mut i = 0usize;
            let n = ev.len();
            while i < n {
                let person = ev[i].person;
                let ps = i;
                while i < n && ev[i].person == person {
                    i += 1;
                }
                let pev = &ev[ps..i];
                while ci < ck.len() && ck[ci].person < person {
                    ci += 1;
                }
                let cs = ci;
                while ci < ck.len() && ck[ci].person == person {
                    ci += 1;
                }
                let pck = &ck[cs..ci];

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
                    let start = pev[j].ts;
                    let mut last = start;
                    let mut cnt = 0i64;
                    let mut rev = 0i64;
                    let mut purch = false;
                    let js = j;
                    while j < m && pev[j].ts - last <= GAP_MS {
                        last = pev[j].ts;
                        cnt += 1;
                        if pev[j].flags & F_PURCHASE != 0 {
                            purch = true;
                            rev += pev[j].price as i64;
                        }
                        j += 1;
                    }
                    let day = start.div_euclid(DAY_MS);
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
                            if e.flags & F_PURCHASE != 0 {
                                match st.catacc.iter_mut().find(|x| x.0 == e.cat) {
                                    Some(x) => x.1 += e.price as i64,
                                    None => st.catacc.push((e.cat, e.price as i64)),
                                }
                            }
                        }
                    }

                    // clicks window [start - 7d, start]; session starts are increasing
                    let wlo = start - WEEK_MS;
                    while lo < pck.len() && pck[lo].ts < wlo {
                        lo += 1;
                    }
                    if hi < lo {
                        hi = lo;
                    }
                    while hi < pck.len() && pck[hi].ts <= start {
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
                    let a = &mut st.camp[credited as usize * ndays + di];
                    a[0] += 1;
                    a[1] += purch as i64;
                    a[2] += rev;
                    if hi > lo {
                        st.sess_ctr = st.sess_ctr.wrapping_add(1);
                        if st.sess_ctr == 0 {
                            st.stamp.iter_mut().for_each(|x| *x = 0);
                            st.sess_ctr = 1;
                        }
                        let sc = st.sess_ctr;
                        for c in &pck[lo..hi] {
                            let ca = c.camp as usize;
                            if st.stamp[ca] != sc {
                                st.stamp[ca] = sc;
                                st.camp[ca * ndays + di][3] += 1;
                            }
                        }
                    }
                }
                if cur_day != i64::MIN {
                    drow.top = top_cat(&st.catacc, &cat_rank);
                    st.rows.push(drow);
                }
            }
            st.ev = ev;
            st.ck = ck;
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
        .build();

    let user_schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("events", DataType::Int64, false),
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
                        let a = agg[c * ndays + di];
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
                s.push(r.sessions);
                e.push(r.events);
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
                    Arc::new(Int64Array::from(s)),
                    Arc::new(Int64Array::from(e)),
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
    Ok(())
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
