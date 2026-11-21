use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use ahash::{AHashMap, AHashSet, RandomState};
use arrow::array::{ArrayRef, Date32Array, Int64Array, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use memmap2::Mmap;
use parquet::arrow::ArrowWriter;
use rayon::prelude::*;

const NUM_SHARDS: usize = 32;

#[inline(always)]
fn get_shard(hash: u64) -> usize {
    (hash as usize) & (NUM_SHARDS - 1)
}

// ---------------------------------------------------------------------------
// Zero-Heap InlineStr (32 bytes, Copy, zero allocations)
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Default)]
#[repr(C, align(32))]
struct InlineStr {
    buf: [u8; 31],
    len: u8,
}

impl InlineStr {
    #[inline(always)]
    fn new() -> Self {
        Self {
            buf: [0u8; 31],
            len: 0,
        }
    }

    #[inline(always)]
    fn from_bytes(bytes: &[u8]) -> Self {
        let len = bytes.len().min(31);
        let mut buf = [0u8; 31];
        buf[..len].copy_from_slice(&bytes[..len]);
        Self {
            buf,
            len: len as u8,
        }
    }

    #[inline(always)]
    fn as_bytes(&self) -> &[u8] {
        &self.buf[..self.len as usize]
    }

    #[inline(always)]
    fn as_str(&self) -> &str {
        std::str::from_utf8(&self.buf[..self.len as usize]).unwrap_or("")
    }

    #[inline(always)]
    fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl PartialEq for InlineStr {
    #[inline(always)]
    fn eq(&self, other: &Self) -> bool {
        self.len == other.len && self.as_bytes() == other.as_bytes()
    }
}

impl Eq for InlineStr {}

impl PartialOrd for InlineStr {
    #[inline(always)]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for InlineStr {
    #[inline(always)]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_bytes().cmp(other.as_bytes())
    }
}

impl std::hash::Hash for InlineStr {
    #[inline(always)]
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_bytes().hash(state);
    }
}

// ---------------------------------------------------------------------------
// Helpers: CSV dialect and fields (Zero Allocation)
// ---------------------------------------------------------------------------

#[inline(always)]
fn parse_field(field: &[u8]) -> Option<&[u8]> {
    if field.is_empty() {
        return Some(b"");
    }
    if field[0] == b'"' {
        let n = field.len();
        if n >= 2 && field[n - 1] == b'"' {
            let inner = &field[1..n - 1];
            if memchr::memchr(b'"', inner).is_none() {
                return Some(inner);
            }
        }
        return None;
    }
    if memchr::memchr(b'"', field).is_some() {
        return None;
    }
    Some(field)
}

#[inline(always)]
fn split_line_7<'a>(line: &'a [u8]) -> Option<[&'a [u8]; 7]> {
    if line.is_empty() {
        return None;
    }
    let mut fields: [&'a [u8]; 7] = [&[]; 7];
    let mut field_idx = 0;
    let mut start = 0;

    if memchr::memchr(b'"', line).is_none() {
        for (i, &b) in line.iter().enumerate() {
            if b == b',' {
                if field_idx >= 6 {
                    return None;
                }
                fields[field_idx] = &line[start..i];
                field_idx += 1;
                start = i + 1;
            }
        }
        if field_idx != 6 {
            return None;
        }
        fields[6] = &line[start..];
        return Some(fields);
    }

    for (i, &b) in line.iter().enumerate() {
        if b == b',' {
            if field_idx >= 6 {
                return None;
            }
            fields[field_idx] = parse_field(&line[start..i])?;
            field_idx += 1;
            start = i + 1;
        }
    }
    if field_idx != 6 {
        return None;
    }
    fields[6] = parse_field(&line[start..])?;
    Some(fields)
}

#[inline(always)]
fn split_line_4<'a>(line: &'a [u8]) -> Option<[&'a [u8]; 4]> {
    if line.is_empty() {
        return None;
    }
    let mut fields: [&'a [u8]; 4] = [&[]; 4];
    let mut field_idx = 0;
    let mut start = 0;

    if memchr::memchr(b'"', line).is_none() {
        for (i, &b) in line.iter().enumerate() {
            if b == b',' {
                if field_idx >= 3 {
                    return None;
                }
                fields[field_idx] = &line[start..i];
                field_idx += 1;
                start = i + 1;
            }
        }
        if field_idx != 3 {
            return None;
        }
        fields[3] = &line[start..];
        return Some(fields);
    }

    for (i, &b) in line.iter().enumerate() {
        if b == b',' {
            if field_idx >= 3 {
                return None;
            }
            fields[field_idx] = parse_field(&line[start..i])?;
            field_idx += 1;
            start = i + 1;
        }
    }
    if field_idx != 3 {
        return None;
    }
    fields[3] = parse_field(&line[start..])?;
    Some(fields)
}

#[inline(always)]
fn split_line_2<'a>(line: &'a [u8]) -> Option<[&'a [u8]; 2]> {
    if line.is_empty() {
        return None;
    }
    let mut fields: [&'a [u8]; 2] = [&[]; 2];
    let mut field_idx = 0;
    let mut start = 0;

    if memchr::memchr(b'"', line).is_none() {
        for (i, &b) in line.iter().enumerate() {
            if b == b',' {
                if field_idx >= 1 {
                    return None;
                }
                fields[field_idx] = &line[start..i];
                field_idx += 1;
                start = i + 1;
            }
        }
        if field_idx != 1 {
            return None;
        }
        fields[1] = &line[start..];
        return Some(fields);
    }

    for (i, &b) in line.iter().enumerate() {
        if b == b',' {
            if field_idx >= 1 {
                return None;
            }
            fields[field_idx] = parse_field(&line[start..i])?;
            field_idx += 1;
            start = i + 1;
        }
    }
    if field_idx != 1 {
        return None;
    }
    fields[1] = parse_field(&line[start..])?;
    Some(fields)
}

// ---------------------------------------------------------------------------
// Branchless Fast Timestamp Parser (Howard Hinnant's Algorithm)
// ---------------------------------------------------------------------------

#[inline(always)]
fn parse_digits(s: &[u8]) -> Option<u32> {
    let mut v: u32 = 0;
    for &b in s {
        if b < b'0' || b > b'9' {
            return None;
        }
        v = v * 10 + (b - b'0') as u32;
    }
    Some(v)
}

#[inline(always)]
fn is_valid_date(y: i32, m: u32, d: u32) -> bool {
    if m == 0 || m > 12 || d == 0 || d > 31 {
        return false;
    }
    if d <= 28 {
        return true;
    }
    if m == 4 || m == 6 || m == 9 || m == 11 {
        return d <= 30;
    }
    if m == 2 {
        let is_leap = (y % 4 == 0) && (y % 100 != 0 || y % 400 == 0);
        return if is_leap { d <= 29 } else { d <= 28 };
    }
    true
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
fn parse_ts(s: &[u8]) -> Option<i64> {
    if s.is_empty() {
        return None;
    }

    // Form F1: YYYY-MM-DDTHH:MM:SSZ (len 20)
    if s.len() == 20
        && s[10] == b'T'
        && s[19] == b'Z'
        && s[4] == b'-'
        && s[7] == b'-'
        && s[13] == b':'
        && s[16] == b':'
    {
        let y = parse_digits(&s[0..4])? as i32;
        let m = parse_digits(&s[5..7])?;
        let d = parse_digits(&s[8..10])?;
        let h = parse_digits(&s[11..13])?;
        let min = parse_digits(&s[14..16])?;
        let sec = parse_digits(&s[17..19])?;

        if !is_valid_date(y, m, d) || h > 23 || min > 59 || sec > 59 {
            return None;
        }

        let days = days_from_civil(y, m, d);
        let secs = days * 86400 + (h as i64 * 3600 + min as i64 * 60 + sec as i64);
        return Some(secs * 1000);
    }

    // Form F2: YYYY-MM-DDTHH:MM:SS[+-]HH:MM (len 25)
    if s.len() == 25
        && s[10] == b'T'
        && s[4] == b'-'
        && s[7] == b'-'
        && s[13] == b':'
        && s[16] == b':'
        && (s[19] == b'+' || s[19] == b'-')
        && s[22] == b':'
    {
        let y = parse_digits(&s[0..4])? as i32;
        let m = parse_digits(&s[5..7])?;
        let d = parse_digits(&s[8..10])?;
        let h = parse_digits(&s[11..13])?;
        let min = parse_digits(&s[14..16])?;
        let sec = parse_digits(&s[17..19])?;

        if !is_valid_date(y, m, d) || h > 23 || min > 59 || sec > 59 {
            return None;
        }

        let off_h = parse_digits(&s[20..22])?;
        let off_m = parse_digits(&s[23..25])?;
        if off_h > 23 || off_m > 59 {
            return None;
        }

        let days = days_from_civil(y, m, d);
        let base_secs = days * 86400 + (h as i64 * 3600 + min as i64 * 60 + sec as i64);
        let sign: i64 = if s[19] == b'+' { 1 } else { -1 };
        let off_secs = sign * (off_h as i64 * 3600 + off_m as i64 * 60);
        return Some((base_secs - off_secs) * 1000);
    }

    // Form F3: Digits only
    if s.iter().all(|&c| c >= b'0' && c <= b'9') {
        let mut v: i64 = 0;
        for &b in s {
            v = v.checked_mul(10)?.checked_add((b - b'0') as i64)?;
        }
        return Some(v);
    }

    None
}

#[inline(always)]
fn epoch_ms_to_date_days(epoch_ms: i64) -> i32 {
    epoch_ms.div_euclid(86_400_000) as i32
}

// ---------------------------------------------------------------------------
// Union-Find (Using InlineStr)
// ---------------------------------------------------------------------------

struct DisjointSet {
    parent: Vec<u32>,
    name: Vec<InlineStr>,
    map: AHashMap<InlineStr, u32>,
}

impl DisjointSet {
    fn new() -> Self {
        Self {
            parent: Vec::new(),
            name: Vec::new(),
            map: AHashMap::new(),
        }
    }

    fn get_or_insert(&mut self, s: InlineStr) -> u32 {
        if let Some(&id) = self.map.get(&s) {
            return id;
        }
        let id = self.parent.len() as u32;
        self.parent.push(id);
        self.name.push(s);
        self.map.insert(s, id);
        id
    }

    fn find(&mut self, mut x: u32) -> u32 {
        let mut root = x;
        while self.parent[root as usize] != root {
            root = self.parent[root as usize];
        }
        while self.parent[x as usize] != root {
            let nxt = self.parent[x as usize];
            self.parent[x as usize] = root;
            x = nxt;
        }
        root
    }

    fn union(&mut self, x: u32, y: u32) {
        let root_x = self.find(x);
        let root_y = self.find(y);
        if root_x != root_y {
            self.parent[root_y as usize] = root_x;
        }
    }
}

// ---------------------------------------------------------------------------
// Pipeline Structs
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
struct CompactRawEvent {
    event_id: InlineStr,     // 32 bytes
    user_id: InlineStr,      // 32 bytes (stores canonical person)
    ingest_ms: i64,          // 8 bytes
    ts_ms: i64,              // 8 bytes
    price: i32,              // 4 bytes
    cat_id: u8,              // 1 byte
    ev_type: u8,             // 1 byte
    is_valid_syntax: bool,   // 1 byte
}

#[derive(Clone, Copy)]
struct ValidEvent {
    ts: i64,
    price: i32,
    cat_id: u8,
    is_purchase: bool,
}

#[derive(Clone, Copy)]
struct ValidClick {
    click_id: InlineStr,
    campaign_id: InlineStr,
    ts: i64,
}

#[derive(Clone, Copy)]
struct UserDayAgg {
    sessions: i64,
    events: i64,
    revenue_cents: i64,
    cat_rev: [i64; 32],
}

#[derive(Default, Clone, Copy)]
struct CampDayAgg {
    sessions: i64,
    purchasing_sessions: i64,
    revenue_cents: i64,
    assisted_sessions: i64,
}

// ---------------------------------------------------------------------------
// Main Pipeline
// ---------------------------------------------------------------------------

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let t0 = Instant::now();
    eprintln!("[0.00s] Starting Shopping Sessions engine (Rust sub-10s extreme)");

    let data_dir = std::env::var("DATA_DIR").unwrap_or_else(|_| "dev".to_string());
    let out_dir = std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "out".to_string());

    let hasher = RandomState::new();

    // 1. Parse products.csv
    let products_file = File::open(Path::new(&data_dir).join("products.csv"))?;
    let mut prod_reader = BufReader::new(products_file);
    let mut line = Vec::new();
    prod_reader.read_until(b'\n', &mut line)?; // skip header

    let mut product_to_cat: AHashMap<InlineStr, u8> = AHashMap::new();
    let mut category_names: Vec<InlineStr> = vec![InlineStr::from_bytes(b"unknown")];
    let mut cat_name_to_id: AHashMap<InlineStr, u8> = AHashMap::new();
    cat_name_to_id.insert(category_names[0], 0);

    line.clear();
    while prod_reader.read_until(b'\n', &mut line)? > 0 {
        let trimmed = if line.ends_with(b"\n") {
            &line[..line.len() - 1]
        } else {
            &line[..]
        };
        if let Some(fields) = split_line_2(trimmed) {
            let pid = fields[0];
            let cat = fields[1];
            if !pid.is_empty() && !cat.is_empty() {
                let cat_str = InlineStr::from_bytes(cat);
                let cat_id = match cat_name_to_id.get(&cat_str) {
                    Some(&id) => id,
                    None => {
                        let id = category_names.len() as u8;
                        category_names.push(cat_str);
                        cat_name_to_id.insert(cat_str, id);
                        id
                    }
                };
                product_to_cat.insert(InlineStr::from_bytes(pid), cat_id);
            }
        }
        line.clear();
    }

    // 2. Parse campaigns.csv
    let campaigns_file = File::open(Path::new(&data_dir).join("campaigns.csv"))?;
    let mut camp_reader = BufReader::new(campaigns_file);
    camp_reader.read_until(b'\n', &mut line)?; // skip header

    let mut campaigns: AHashMap<InlineStr, InlineStr> = AHashMap::new();
    let mut valid_campaigns: AHashSet<InlineStr> = AHashSet::new();
    line.clear();
    while camp_reader.read_until(b'\n', &mut line)? > 0 {
        let trimmed = if line.ends_with(b"\n") {
            &line[..line.len() - 1]
        } else {
            &line[..]
        };
        if let Some(fields) = split_line_2(trimmed) {
            let cid = fields[0];
            let channel = fields[1];
            if !cid.is_empty() && !channel.is_empty() {
                let c_str = InlineStr::from_bytes(cid);
                campaigns.insert(c_str, InlineStr::from_bytes(channel));
                valid_campaigns.insert(c_str);
            }
        }
        line.clear();
    }

    // 3. Parse manifest.json for event window
    let manifest_path = Path::new(&data_dir).join("manifest.json");
    let manifest_bytes = std::fs::read(&manifest_path)?;
    let manifest_val: serde_json::Value = serde_json::from_slice(&manifest_bytes)?;
    let win_start_str = manifest_val["window_start"]
        .as_str()
        .or_else(|| manifest_val["event_window"]["start"].as_str())
        .unwrap();
    let win_end_str = manifest_val["window_end"]
        .as_str()
        .or_else(|| manifest_val["event_window"]["end"].as_str())
        .unwrap();

    let win_start_ms = parse_ts(win_start_str.as_bytes()).unwrap();
    let win_end_ms = parse_ts(win_end_str.as_bytes()).unwrap();
    let click_win_start_ms = win_start_ms - 7 * 86_400_000;

    // 4. Process links (Connected Components)
    eprintln!("[{:.2}s] Processing links...", t0.elapsed().as_secs_f64());
    let links_dir = Path::new(&data_dir).join("links");
    let mut dsu = DisjointSet::new();

    if links_dir.exists() {
        let mut link_files: Vec<_> = std::fs::read_dir(&links_dir)?
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map_or(false, |ext| ext == "csv"))
            .map(|e| e.path())
            .collect();
        link_files.sort();

        for lf in link_files {
            let file = File::open(lf)?;
            let mut reader = BufReader::new(file);
            line.clear();
            reader.read_until(b'\n', &mut line)?; // header
            line.clear();
            while reader.read_until(b'\n', &mut line)? > 0 {
                let trimmed = if line.ends_with(b"\n") {
                    &line[..line.len() - 1]
                } else {
                    &line[..]
                };
                if let Some(fields) = split_line_2(trimmed) {
                    let u1 = fields[0];
                    let u2 = fields[1];
                    if !u1.is_empty() && !u2.is_empty() {
                        let id1 = dsu.get_or_insert(InlineStr::from_bytes(u1));
                        let id2 = dsu.get_or_insert(InlineStr::from_bytes(u2));
                        dsu.union(id1, id2);
                    }
                }
                line.clear();
            }
        }
    }

    let n_nodes = dsu.parent.len();
    let mut root_to_min_name: Vec<Option<InlineStr>> = vec![None; n_nodes];
    for i in 0..n_nodes {
        let root = dsu.find(i as u32) as usize;
        let node_name = dsu.name[i];
        match root_to_min_name[root] {
            None => root_to_min_name[root] = Some(node_name),
            Some(curr) => {
                if node_name < curr {
                    root_to_min_name[root] = Some(node_name);
                }
            }
        }
    }

    let mut canonical_map: AHashMap<InlineStr, InlineStr> = AHashMap::with_capacity(n_nodes);
    for i in 0..n_nodes {
        let root = dsu.find(i as u32) as usize;
        let canon = root_to_min_name[root].unwrap();
        canonical_map.insert(dsu.name[i], canon);
    }

    let get_person = |user_id: InlineStr| -> InlineStr {
        if canonical_map.is_empty() {
            user_id
        } else {
            canonical_map.get(&user_id).copied().unwrap_or(user_id)
        }
    };

    // 5. Parallel Event Reading with Memory Mapping & In-Flight Parsing
    eprintln!("[{:.2}s] Parallel reading events with mmap...", t0.elapsed().as_secs_f64());
    let events_dir = Path::new(&data_dir).join("events");
    let mut event_files: Vec<_> = std::fs::read_dir(&events_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "csv"))
        .map(|e| e.path())
        .collect();
    event_files.sort();

    let file_event_shards: Vec<Vec<Vec<CompactRawEvent>>> = event_files
        .par_iter()
        .map(|ef| {
            let file = match File::open(ef) {
                Ok(f) => f,
                Err(_) => return (0..NUM_SHARDS).map(|_| Vec::new()).collect(),
            };
            let mmap = match unsafe { Mmap::map(&file) } {
                Ok(m) => m,
                Err(_) => return (0..NUM_SHARDS).map(|_| Vec::new()).collect(),
            };
            let content = &mmap[..];
            let mut lines = content.split(|&b| b == b'\n');
            lines.next(); // header

            let est_lines = (content.len() / 75).max(1024);
            let est_per_shard = (est_lines / NUM_SHARDS) + 64;
            let mut local_shards: Vec<Vec<CompactRawEvent>> = (0..NUM_SHARDS)
                .map(|_| Vec::with_capacity(est_per_shard))
                .collect();

            for line in lines {
                if let Some(f) = split_line_7(line) {
                    let event_id_bytes = f[0];
                    if event_id_bytes.is_empty() {
                        continue;
                    }
                    if let Some(ingest_ms) = parse_ts(f[3]) {
                        let ev_type = match f[4] {
                            b"view" => 0,
                            b"cart" => 1,
                            b"purchase" => 2,
                            _ => 3,
                        };

                        let ts_ms = parse_ts(f[2]).unwrap_or(-1);
                        let in_win_ts = if ts_ms >= win_start_ms && ts_ms < win_end_ms { ts_ms } else { -1 };

                        let mut price_val: i32 = 0;
                        let mut cat_id: u8 = 0;
                        let mut price_valid = true;

                        if ev_type == 2 {
                            let p_bytes = f[6];
                            if p_bytes.is_empty() || !p_bytes.iter().all(|&c| c >= b'0' && c <= b'9') {
                                price_valid = false;
                            } else {
                                match parse_digits(p_bytes) {
                                    Some(p) if p >= 1 && p <= 10_000_000 => {
                                        price_val = p as i32;
                                        let prod_str = InlineStr::from_bytes(f[5]);
                                        cat_id = product_to_cat.get(&prod_str).copied().unwrap_or(0);
                                    }
                                    _ => price_valid = false,
                                }
                            }
                        }

                        let is_prod_non_empty = !f[5].is_empty();
                        let is_user_non_empty = !f[1].is_empty();

                        let is_valid_syntax = is_prod_non_empty
                            && is_user_non_empty
                            && ev_type != 3
                            && in_win_ts != -1
                            && price_valid;

                        let event_id = InlineStr::from_bytes(event_id_bytes);
                        let user_id = InlineStr::from_bytes(f[1]);
                        let person = get_person(user_id);

                        let sid = get_shard(hasher.hash_one(&event_id));
                        local_shards[sid].push(CompactRawEvent {
                            event_id,
                            user_id: person, // holds canonical person directly
                            ingest_ms,
                            ts_ms: in_win_ts,
                            price: price_val,
                            cat_id,
                            ev_type,
                            is_valid_syntax,
                        });
                    }
                }
            }
            local_shards
        })
        .collect();

    // Parallel merge of file_event_shards into global_event_shards
    let global_event_shards: Vec<Vec<CompactRawEvent>> = (0..NUM_SHARDS)
        .into_par_iter()
        .map(|sid| {
            let total_len: usize = file_event_shards.iter().map(|fs| fs[sid].len()).sum();
            let mut shard = Vec::with_capacity(total_len);
            for fs in &file_event_shards {
                shard.extend_from_slice(&fs[sid]);
            }
            shard
        })
        .collect();

    // 6. Parallel Dedup & Validation -> Shard by hash(person)
    eprintln!("[{:.2}s] Parallel deduplicating & validating events...", t0.elapsed().as_secs_f64());
    let person_event_buffers: Vec<Vec<Vec<(InlineStr, ValidEvent)>>> = global_event_shards
        .into_par_iter()
        .map(|mut shard_items| {
            // In-place sort by (event_id, ingest_ms)
            shard_items.sort_unstable_by(|a, b| {
                a.event_id.cmp(&b.event_id).then_with(|| a.ingest_ms.cmp(&b.ingest_ms))
            });

            let est_per_p_shard = (shard_items.len() / NUM_SHARDS) + 32;
            let mut person_shards: Vec<Vec<(InlineStr, ValidEvent)>> = (0..NUM_SHARDS)
                .map(|_| Vec::with_capacity(est_per_p_shard))
                .collect();

            let n = shard_items.len();
            let mut i = 0;
            while i < n {
                let rec = &shard_items[i];
                let ev_id = rec.event_id;
                let mut j = i + 1;
                while j < n && shard_items[j].event_id == ev_id {
                    j += 1;
                }
                i = j;

                if !rec.is_valid_syntax {
                    continue;
                }
                if rec.ingest_ms - rec.ts_ms > 86_400_000 {
                    continue;
                }

                let person = rec.user_id;
                let p_shard = get_shard(hasher.hash_one(&person));
                person_shards[p_shard].push((person, ValidEvent {
                    ts: rec.ts_ms,
                    price: rec.price,
                    cat_id: rec.cat_id,
                    is_purchase: rec.ev_type == 2,
                }));
            }
            person_shards
        })
        .collect();

    // Parallel merge & sort into 64 person event shards
    let person_event_shards: Vec<Vec<(InlineStr, ValidEvent)>> = (0..NUM_SHARDS)
        .into_par_iter()
        .map(|target_shard| {
            let total_len: usize = person_event_buffers.iter().map(|sb| sb[target_shard].len()).sum();
            let mut shard = Vec::with_capacity(total_len);
            for sb in &person_event_buffers {
                shard.extend_from_slice(&sb[target_shard]);
            }
            // Sort by (person, ts)
            shard.sort_unstable_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.ts.cmp(&b.1.ts)));
            shard
        })
        .collect();

    // 7. Parallel Click Reading & Validation directly into person shards
    eprintln!("[{:.2}s] Parallel reading & validating clicks with mmap...", t0.elapsed().as_secs_f64());
    let clicks_dir = Path::new(&data_dir).join("clicks");
    let mut click_files: Vec<_> = std::fs::read_dir(&clicks_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "csv"))
        .map(|e| e.path())
        .collect();
    click_files.sort();

    let file_click_shards: Vec<Vec<Vec<(InlineStr, ValidClick)>>> = click_files
        .par_iter()
        .map(|cf| {
            let file = match File::open(cf) {
                Ok(f) => f,
                Err(_) => return (0..NUM_SHARDS).map(|_| Vec::new()).collect(),
            };
            let mmap = match unsafe { Mmap::map(&file) } {
                Ok(m) => m,
                Err(_) => return (0..NUM_SHARDS).map(|_| Vec::new()).collect(),
            };
            let content = &mmap[..];
            let mut lines = content.split(|&b| b == b'\n');
            lines.next(); // header

            let est_lines = (content.len() / 60).max(512);
            let est_per_shard = (est_lines / NUM_SHARDS) + 32;
            let mut local_shards: Vec<Vec<(InlineStr, ValidClick)>> = (0..NUM_SHARDS)
                .map(|_| Vec::with_capacity(est_per_shard))
                .collect();

            for line in lines {
                if let Some(f) = split_line_4(line) {
                    let click_id_bytes = f[0];
                    if click_id_bytes.is_empty() {
                        continue;
                    }
                    let user_bytes = f[1];
                    if user_bytes.is_empty() {
                        continue;
                    }
                    let camp_bytes = f[2];
                    let camp_str = InlineStr::from_bytes(camp_bytes);
                    if !valid_campaigns.contains(&camp_str) {
                        continue;
                    }
                    let ts_ms = match parse_ts(f[3]) {
                        Some(t) if t >= click_win_start_ms && t < win_end_ms => t,
                        _ => continue,
                    };

                    let user_id = InlineStr::from_bytes(user_bytes);
                    let person = get_person(user_id);
                    let click_id = InlineStr::from_bytes(click_id_bytes);

                    let sid = get_shard(hasher.hash_one(&person));
                    local_shards[sid].push((person, ValidClick {
                        click_id,
                        campaign_id: camp_str,
                        ts: ts_ms,
                    }));
                }
            }
            local_shards
        })
        .collect();

    // Parallel merge & sort & dedup into 64 person click shards
    let person_click_shards: Vec<Vec<(InlineStr, ValidClick)>> = (0..NUM_SHARDS)
        .into_par_iter()
        .map(|sid| {
            let total_len: usize = file_click_shards.iter().map(|fs| fs[sid].len()).sum();
            let mut shard = Vec::with_capacity(total_len);
            for fs in &file_click_shards {
                shard.extend_from_slice(&fs[sid]);
            }
            // Sort by (person, ts, click_id)
            shard.sort_unstable_by(|a, b| {
                a.0.cmp(&b.0)
                    .then_with(|| a.1.ts.cmp(&b.1.ts))
                    .then_with(|| a.1.click_id.cmp(&b.1.click_id))
            });
            // In-place dedup identical click_ids for this person
            shard.dedup_by(|a, b| a.0 == b.0 && a.1.click_id == b.1.click_id);
            shard
        })
        .collect();

    // 8. Parallel Sessionization & Attribution across Person Shards
    eprintln!("[{:.2}s] Parallel sessionizing & attributing across {} shards...", t0.elapsed().as_secs_f64(), NUM_SHARDS);
    let organic_id = InlineStr::from_bytes(b"organic");

    let shard_results: Vec<(Vec<((InlineStr, i32), UserDayAgg)>, AHashMap<(InlineStr, i32), CampDayAgg>)> = person_event_shards
        .into_par_iter()
        .zip(person_click_shards.into_par_iter())
        .map(|(events, clicks)| {
            let mut local_user_daily: Vec<((InlineStr, i32), UserDayAgg)> = Vec::with_capacity(events.len() / 4 + 16);
            let mut local_camp_daily: AHashMap<(InlineStr, i32), CampDayAgg> = AHashMap::new();

            let mut ev_idx = 0;
            let n_events = events.len();

            let mut clk_cursor = 0;
            let n_clicks = clicks.len();

            while ev_idx < n_events {
                let person = events[ev_idx].0;
                let p_start = ev_idx;
                while ev_idx < n_events && events[ev_idx].0 == person {
                    ev_idx += 1;
                }
                let person_events = &events[p_start..ev_idx];

                // Fast cursor advance to find this person's clicks
                while clk_cursor < n_clicks && clicks[clk_cursor].0 < person {
                    clk_cursor += 1;
                }
                let c_start = clk_cursor;
                while clk_cursor < n_clicks && clicks[clk_cursor].0 == person {
                    clk_cursor += 1;
                }
                let person_clicks = &clicks[c_start..clk_cursor];

                // Sessionize person_events
                let mut i = 0;
                let m = person_events.len();

                let mut cur_day = -1;
                let mut cur_day_agg = UserDayAgg {
                    sessions: 0,
                    events: 0,
                    revenue_cents: 0,
                    cat_rev: [0i64; 32],
                };

                while i < m {
                    let session_start = person_events[i].1.ts;
                    let session_day = epoch_ms_to_date_days(session_start);

                    let mut session_events_count: i64 = 0;
                    let mut session_revenue: i64 = 0;
                    let mut session_has_purchase = false;
                    let mut session_cat_rev = [0i64; 32];

                    let mut cur_ts = session_start;
                    while i < m {
                        let ev = &person_events[i].1;
                        if ev.ts - cur_ts > 1_800_000 {
                            break;
                        }
                        cur_ts = ev.ts;
                        session_events_count += 1;
                        if ev.is_purchase {
                            session_has_purchase = true;
                            let p = ev.price as i64;
                            session_revenue += p;
                            session_cat_rev[ev.cat_id as usize] += p;
                        }
                        i += 1;
                    }

                    if cur_day != session_day {
                        if cur_day != -1 {
                            local_user_daily.push(((person, cur_day), cur_day_agg));
                        }
                        cur_day = session_day;
                        cur_day_agg = UserDayAgg {
                            sessions: 0,
                            events: 0,
                            revenue_cents: 0,
                            cat_rev: [0i64; 32],
                        };
                    }
                    cur_day_agg.sessions += 1;
                    cur_day_agg.events += session_events_count;
                    cur_day_agg.revenue_cents += session_revenue;
                    for c_idx in 0..32 {
                        cur_day_agg.cat_rev[c_idx] += session_cat_rev[c_idx];
                    }

                    // Attribution: binary search in person_clicks
                    let win_start = session_start - 604_800_000;
                    let left = person_clicks.partition_point(|c| c.1.ts < win_start);
                    let right = person_clicks.partition_point(|c| c.1.ts <= session_start);
                    let window_clicks = &person_clicks[left..right];

                    let credited_camp = if !window_clicks.is_empty() {
                        let max_ts = window_clicks.last().unwrap().1.ts;
                        let tie_start = window_clicks.partition_point(|c| c.1.ts < max_ts);
                        window_clicks[tie_start].1.campaign_id
                    } else {
                        organic_id
                    };

                    let c_agg = local_camp_daily.entry((credited_camp, session_day)).or_default();
                    c_agg.sessions += 1;
                    c_agg.revenue_cents += session_revenue;
                    if session_has_purchase {
                        c_agg.purchasing_sessions += 1;
                    }

                    // Assists: distinct campaigns in window_clicks
                    if !window_clicks.is_empty() {
                        let mut seen_camps: [InlineStr; 16] = [InlineStr::new(); 16];
                        let mut num_seen = 0;
                        let mut fallback_set: Option<AHashSet<InlineStr>> = None;

                        for clk in window_clicks {
                            let c_id = clk.1.campaign_id;
                            let mut already_seen = false;
                            if let Some(set) = &fallback_set {
                                if set.contains(&c_id) {
                                    already_seen = true;
                                }
                            } else {
                                for sc in &seen_camps[..num_seen] {
                                    if *sc == c_id {
                                        already_seen = true;
                                        break;
                                    }
                                }
                            }

                            if !already_seen {
                                if num_seen < 16 {
                                    seen_camps[num_seen] = c_id;
                                    num_seen += 1;
                                } else {
                                    if fallback_set.is_none() {
                                        let mut s = AHashSet::new();
                                        for sc in &seen_camps[..num_seen] {
                                            s.insert(*sc);
                                        }
                                        fallback_set = Some(s);
                                    }
                                    if let Some(set) = &mut fallback_set {
                                        set.insert(c_id);
                                    }
                                }
                                local_camp_daily.entry((c_id, session_day)).or_default().assisted_sessions += 1;
                            }
                        }
                    }
                }

                if cur_day != -1 {
                    local_user_daily.push(((person, cur_day), cur_day_agg));
                }
            }

            (local_user_daily, local_camp_daily)
        })
        .collect();

    // 9 & 10. Write Parquet: user_daily and campaign_daily in Parallel
    eprintln!("[{:.2}s] Writing user_daily and campaign_daily Parquet in parallel...", t0.elapsed().as_secs_f64());

    let write_user_parquet = || -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let user_out_dir = Path::new(&out_dir).join("user_daily");
        std::fs::create_dir_all(&user_out_dir)?;

        let total_user_rows: usize = shard_results.iter().map(|(u, _)| u.len()).sum();

        let mut u_user_builder = StringBuilder::with_capacity(total_user_rows, total_user_rows * 12);
        let mut u_days = Vec::with_capacity(total_user_rows);
        let mut u_sessions = Vec::with_capacity(total_user_rows);
        let mut u_events = Vec::with_capacity(total_user_rows);
        let mut u_revenue = Vec::with_capacity(total_user_rows);
        let mut u_top_cat_builder = StringBuilder::with_capacity(total_user_rows, total_user_rows * 12);

        for (user_items, _) in &shard_results {
            for ((person, day_days), agg) in user_items {
                u_user_builder.append_value(person.as_str());
                u_days.push(*day_days);
                u_sessions.push(agg.sessions);
                u_events.push(agg.events);
                u_revenue.push(agg.revenue_cents);

                let mut top_cat_id: Option<u8> = None;
                let mut best_rev: i64 = 0;

                for (cat_idx, &rev) in agg.cat_rev.iter().enumerate() {
                    if rev > 0 {
                        match top_cat_id {
                            None => {
                                top_cat_id = Some(cat_idx as u8);
                                best_rev = rev;
                            }
                            Some(curr_id) => {
                                let curr_name = category_names[curr_id as usize].as_bytes();
                                let cand_name = category_names[cat_idx].as_bytes();
                                if rev > best_rev || (rev == best_rev && cand_name < curr_name) {
                                    top_cat_id = Some(cat_idx as u8);
                                    best_rev = rev;
                                }
                            }
                        }
                    }
                }

                if let Some(id) = top_cat_id {
                    u_top_cat_builder.append_value(category_names[id as usize].as_str());
                } else {
                    u_top_cat_builder.append_null();
                }
            }
        }

        let user_schema = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("day", DataType::Date32, false),
            Field::new("sessions", DataType::Int64, false),
            Field::new("events", DataType::Int64, false),
            Field::new("revenue_cents", DataType::Int64, false),
            Field::new("top_category", DataType::Utf8, true),
        ]));

        let user_batch = RecordBatch::try_new(
            user_schema.clone(),
            vec![
                Arc::new(u_user_builder.finish()) as ArrayRef,
                Arc::new(Date32Array::from(u_days)) as ArrayRef,
                Arc::new(Int64Array::from(u_sessions)) as ArrayRef,
                Arc::new(Int64Array::from(u_events)) as ArrayRef,
                Arc::new(Int64Array::from(u_revenue)) as ArrayRef,
                Arc::new(u_top_cat_builder.finish()) as ArrayRef,
            ],
        )?;

        let user_parquet_path = user_out_dir.join("part-0.parquet");
        let user_file = File::create(user_parquet_path)?;
        let mut writer = ArrowWriter::try_new(user_file, user_schema, None)?;
        writer.write(&user_batch)?;
        writer.close()?;
        Ok(())
    };

    let write_camp_parquet = || -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut global_camp_daily: AHashMap<(InlineStr, i32), CampDayAgg> = AHashMap::new();
        for (_, camp_map) in &shard_results {
            for (&k, v) in camp_map {
                let entry = global_camp_daily.entry(k).or_default();
                entry.sessions += v.sessions;
                entry.purchasing_sessions += v.purchasing_sessions;
                entry.revenue_cents += v.revenue_cents;
                entry.assisted_sessions += v.assisted_sessions;
            }
        }

        let camp_out_dir = Path::new(&out_dir).join("campaign_daily");
        std::fs::create_dir_all(&camp_out_dir)?;

        let n_camp_rows = global_camp_daily.len();
        let mut c_id_builder = StringBuilder::with_capacity(n_camp_rows, n_camp_rows * 12);
        let mut c_channel_builder = StringBuilder::with_capacity(n_camp_rows, n_camp_rows * 12);
        let mut c_days = Vec::with_capacity(n_camp_rows);
        let mut c_sessions = Vec::with_capacity(n_camp_rows);
        let mut c_purchasing = Vec::with_capacity(n_camp_rows);
        let mut c_revenue = Vec::with_capacity(n_camp_rows);
        let mut c_assisted = Vec::with_capacity(n_camp_rows);

        let org_str = InlineStr::from_bytes(b"organic");
        for ((camp_id, day_days), agg) in global_camp_daily {
            let channel_str = if camp_id == org_str {
                "organic"
            } else {
                campaigns.get(&camp_id)
                    .map(|ch| ch.as_str())
                    .unwrap_or("organic")
            };

            c_id_builder.append_value(camp_id.as_str());
            c_channel_builder.append_value(channel_str);
            c_days.push(day_days);
            c_sessions.push(agg.sessions);
            c_purchasing.push(agg.purchasing_sessions);
            c_revenue.push(agg.revenue_cents);
            c_assisted.push(agg.assisted_sessions);
        }

        let camp_schema = Arc::new(Schema::new(vec![
            Field::new("campaign_id", DataType::Utf8, false),
            Field::new("channel", DataType::Utf8, false),
            Field::new("day", DataType::Date32, false),
            Field::new("sessions", DataType::Int64, false),
            Field::new("purchasing_sessions", DataType::Int64, false),
            Field::new("revenue_cents", DataType::Int64, false),
            Field::new("assisted_sessions", DataType::Int64, false),
        ]));

        let camp_batch = RecordBatch::try_new(
            camp_schema.clone(),
            vec![
                Arc::new(c_id_builder.finish()) as ArrayRef,
                Arc::new(c_channel_builder.finish()) as ArrayRef,
                Arc::new(Date32Array::from(c_days)) as ArrayRef,
                Arc::new(Int64Array::from(c_sessions)) as ArrayRef,
                Arc::new(Int64Array::from(c_purchasing)) as ArrayRef,
                Arc::new(Int64Array::from(c_revenue)) as ArrayRef,
                Arc::new(Int64Array::from(c_assisted)) as ArrayRef,
            ],
        )?;

        let camp_parquet_path = camp_out_dir.join("part-0.parquet");
        let camp_file = File::create(camp_parquet_path)?;
        let mut writer = ArrowWriter::try_new(camp_file, camp_schema, None)?;
        writer.write(&camp_batch)?;
        writer.close()?;
        Ok(())
    };

    let (res_u, res_c) = rayon::join(write_user_parquet, write_camp_parquet);
    if let Err(e) = res_u {
        return Err(e);
    }
    if let Err(e) = res_c {
        return Err(e);
    }

    eprintln!("[{:.2}s] Pipeline finished successfully!", t0.elapsed().as_secs_f64());
    Ok(())
}
