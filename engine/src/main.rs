use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use ahash::{AHashMap, AHashSet, RandomState};
use arrow::array::{ArrayRef, Date32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, NaiveDate};
use parquet::arrow::ArrowWriter;
use rayon::prelude::*;

const NUM_SHARDS: usize = 64;

#[inline(always)]
fn get_shard(hash: u64) -> usize {
    (hash as usize) & (NUM_SHARDS - 1)
}

// ---------------------------------------------------------------------------
// Helpers: CSV dialect and fields
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
            if !inner.contains(&b'"') {
                return Some(inner);
            }
        }
        return None;
    }
    if field.contains(&b'"') {
        return None;
    }
    Some(field)
}

#[inline(always)]
fn split_line<'a>(line: &'a [u8], expected_fields: usize) -> Option<Vec<&'a [u8]>> {
    if line.is_empty() {
        return None;
    }
    let mut fields = Vec::with_capacity(expected_fields);
    for part in line.split(|&b| b == b',') {
        let clean = parse_field(part)?;
        fields.push(clean);
    }
    if fields.len() == expected_fields {
        Some(fields)
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// Fast Timestamp parser: F1, F2, F3
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

        let date = NaiveDate::from_ymd_opt(y, m, d)?;
        let time = date.and_hms_opt(h, min, sec)?;
        return Some(time.and_utc().timestamp_millis());
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

        let date = NaiveDate::from_ymd_opt(y, m, d)?;
        let time = date.and_hms_opt(h, min, sec)?;
        let base_ms = time.and_utc().timestamp_millis();

        let off_h = parse_digits(&s[20..22])?;
        let off_m = parse_digits(&s[23..25])?;
        if off_h > 23 || off_m > 59 {
            return None;
        }
        let sign: i64 = if s[19] == b'+' { 1 } else { -1 };
        let off_ms = sign * (off_h as i64 * 3600 + off_m as i64 * 60) * 1000;
        return Some(base_ms - off_ms);
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
    let secs = epoch_ms.div_euclid(1000);
    let dt = DateTime::from_timestamp(secs, 0).unwrap_or_default();
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    (dt.date_naive() - epoch).num_days() as i32
}

// ---------------------------------------------------------------------------
// Union-Find
// ---------------------------------------------------------------------------

struct DisjointSet {
    parent: Vec<u32>,
    name: Vec<Vec<u8>>,
    map: AHashMap<Vec<u8>, u32>,
}

impl DisjointSet {
    fn new() -> Self {
        Self {
            parent: Vec::new(),
            name: Vec::new(),
            map: AHashMap::new(),
        }
    }

    fn get_or_insert(&mut self, s: &[u8]) -> u32 {
        if let Some(&id) = self.map.get(s) {
            return id;
        }
        let id = self.parent.len() as u32;
        self.parent.push(id);
        self.name.push(s.to_vec());
        self.map.insert(s.to_vec(), id);
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
        let rx = self.find(x);
        let ry = self.find(y);
        if rx != ry {
            if self.name[rx as usize] < self.name[ry as usize] {
                self.parent[ry as usize] = rx;
            } else {
                self.parent[rx as usize] = ry;
            }
        }
    }

    fn get_canonical_name(&mut self, s: &[u8]) -> Vec<u8> {
        if let Some(&id) = self.map.get(s) {
            let root = self.find(id);
            self.name[root as usize].clone()
        } else {
            s.to_vec()
        }
    }
}

// ---------------------------------------------------------------------------
// Compact Data Structures
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct CompactRawEvent {
    event_id: Vec<u8>,
    user_id: Vec<u8>,
    ingest_ms: i64,
    ts_ms: i64,       // -1 if invalid or outside window
    price: i32,       // -1 if invalid
    cat_id: u8,
    ev_type: u8,      // 0 = view, 1 = cart, 2 = purchase, 3 = invalid
    is_valid_syntax: bool,
}

#[derive(Clone)]
struct RawClickItem {
    click_id: Vec<u8>,
    user_id: Vec<u8>,
    campaign_id: Vec<u8>,
    ts_bytes: Vec<u8>,
}

#[derive(Clone, Copy)]
struct ValidEvent {
    ts: i64,
    price: i32,
    cat_id: u8,
    is_purchase: bool,
}

#[derive(Clone)]
struct ValidClick {
    click_id: Vec<u8>,
    campaign_id: Vec<u8>,
    ts: i64,
}

// Aggregations
struct UserDayAgg {
    sessions: i64,
    events: i64,
    revenue_cents: i64,
    cat_rev: [i64; 32], // up to 32 categories, zero heap allocation
}

#[derive(Default)]
struct CampDayAgg {
    sessions: i64,
    purchasing_sessions: i64,
    revenue_cents: i64,
    assisted_sessions: i64,
}

// ---------------------------------------------------------------------------
// Main pipeline
// ---------------------------------------------------------------------------

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let t0 = Instant::now();
    let args: Vec<String> = std::env::args().collect();
    let data_dir = args.get(1)
        .cloned()
        .or_else(|| std::env::var("DATA_DIR").ok())
        .unwrap_or_else(|| "/data".to_string());
    let out_dir = args.get(2)
        .cloned()
        .or_else(|| std::env::var("OUTPUT_DIR").ok())
        .unwrap_or_else(|| "/output".to_string());

    if let Ok(ncpu_str) = std::env::var("NCPU") {
        if let Ok(ncpu) = ncpu_str.parse::<usize>() {
            let _ = rayon::ThreadPoolBuilder::new().num_threads(ncpu).build_global();
        }
    }

    let hasher = RandomState::new();

    eprintln!("[{:.2}s] Starting Shopping Sessions engine (Rust extreme)", t0.elapsed().as_secs_f64());

    // 1. Manifest
    let manifest_path = Path::new(&data_dir).join("manifest.json");
    let manifest_bytes = std::fs::read(&manifest_path)?;
    let manifest_json: serde_json::Value = serde_json::from_slice(&manifest_bytes)?;
    let win_start_str = manifest_json["window_start"].as_str().unwrap();
    let win_end_str = manifest_json["window_end"].as_str().unwrap();
    let win_start_ms = parse_ts(win_start_str.as_bytes()).unwrap();
    let win_end_ms = parse_ts(win_end_str.as_bytes()).unwrap();
    let click_win_start_ms = win_start_ms - 7 * 86_400_000;

    // 2. Categories & Products
    // There are only ~20 categories. Map each category string to a u8 ID.
    let mut category_names: Vec<Vec<u8>> = Vec::new();
    let mut cat_name_to_id: AHashMap<Vec<u8>, u8> = AHashMap::new();

    // category 0 is always "unknown"
    category_names.push(b"unknown".to_vec());
    cat_name_to_id.insert(b"unknown".to_vec(), 0);

    let mut product_to_cat: AHashMap<Vec<u8>, u8> = AHashMap::new();
    let products_path = Path::new(&data_dir).join("products.csv");
    if products_path.exists() {
        let content = std::fs::read(&products_path)?;
        let mut lines = content.split(|&b| b == b'\n');
        lines.next(); // header
        for line in lines {
            if let Some(f) = split_line(line, 2) {
                if !f[0].is_empty() && !f[1].is_empty() {
                    let cat_id = *cat_name_to_id.entry(f[1].to_vec()).or_insert_with(|| {
                        let id = category_names.len() as u8;
                        category_names.push(f[1].to_vec());
                        id
                    });
                    product_to_cat.insert(f[0].to_vec(), cat_id);
                }
            }
        }
    }
    let product_to_cat = Arc::new(product_to_cat);
    let category_names = Arc::new(category_names);

    // 3. Campaigns
    let mut campaigns = AHashMap::new();
    let campaigns_path = Path::new(&data_dir).join("campaigns.csv");
    if campaigns_path.exists() {
        let content = std::fs::read(&campaigns_path)?;
        let mut lines = content.split(|&b| b == b'\n');
        lines.next(); // header
        for line in lines {
            if let Some(f) = split_line(line, 2) {
                if !f[0].is_empty() && !f[1].is_empty() {
                    campaigns.insert(f[0].to_vec(), f[1].to_vec());
                }
            }
        }
    }
    let campaigns = Arc::new(campaigns);

    // 4. Links
    eprintln!("[{:.2}s] Processing links...", t0.elapsed().as_secs_f64());
    let mut dsu = DisjointSet::new();
    let links_dir = Path::new(&data_dir).join("links");
    if links_dir.exists() {
        let mut link_files: Vec<_> = std::fs::read_dir(&links_dir)?
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map_or(false, |ext| ext == "csv"))
            .map(|e| e.path())
            .collect();
        link_files.sort();

        for lf in link_files {
            let content = std::fs::read(&lf)?;
            let mut lines = content.split(|&b| b == b'\n');
            lines.next(); // header
            for line in lines {
                if let Some(f) = split_line(line, 2) {
                    if !f[0].is_empty() && !f[1].is_empty() {
                        let id_a = dsu.get_or_insert(f[0]);
                        let id_b = dsu.get_or_insert(f[1]);
                        dsu.union(id_a, id_b);
                    }
                }
            }
        }
    }

    // Pre-resolve all known names in DSU to make lookup lock-free
    let mut canonical_map: AHashMap<Vec<u8>, Vec<u8>> = AHashMap::new();
    let dsu_keys: Vec<Vec<u8>> = dsu.map.keys().cloned().collect();
    for k in dsu_keys {
        let canon = dsu.get_canonical_name(&k);
        canonical_map.insert(k, canon);
    }
    let canonical_map = Arc::new(canonical_map);

    let get_person = {
        let canonical_map = Arc::clone(&canonical_map);
        move |user_id: &[u8]| -> Vec<u8> {
            canonical_map.get(user_id).cloned().unwrap_or_else(|| user_id.to_vec())
        }
    };

    // 5. Parallel Event Reading & Immediate In-Flight Parsing
    eprintln!("[{:.2}s] Parallel reading events...", t0.elapsed().as_secs_f64());
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
            let content = std::fs::read(ef).unwrap_or_default();
            let mut lines = content.split(|&b| b == b'\n');
            lines.next(); // header
            let mut local_shards: Vec<Vec<CompactRawEvent>> = (0..NUM_SHARDS).map(|_| Vec::new()).collect();
            for line in lines {
                if let Some(f) = split_line(line, 7) {
                    let event_id = f[0];
                    if event_id.is_empty() {
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
                        let mut price_valid = true;
                        if ev_type == 2 {
                            let p_bytes = f[6];
                            if p_bytes.is_empty() || !p_bytes.iter().all(|&c| c >= b'0' && c <= b'9') {
                                price_valid = false;
                            } else {
                                match parse_digits(p_bytes) {
                                    Some(p) if p >= 1 && p <= 10_000_000 => price_val = p as i32,
                                    _ => price_valid = false,
                                }
                            }
                        }

                        let is_prod_non_empty = !f[5].is_empty();
                        let is_user_non_empty = !f[1].is_empty();
                        let cat_id = product_to_cat.get(f[5]).copied().unwrap_or(0);

                        let is_valid_syntax = is_prod_non_empty
                            && is_user_non_empty
                            && ev_type != 3
                            && in_win_ts != -1
                            && price_valid;

                        let sid = get_shard(hasher.hash_one(event_id));
                        local_shards[sid].push(CompactRawEvent {
                            event_id: event_id.to_vec(),
                            user_id: f[1].to_vec(),
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

    let mut global_event_shards: Vec<Vec<CompactRawEvent>> = (0..NUM_SHARDS).map(|_| Vec::new()).collect();
    for local_shards in file_event_shards {
        for (sid, items) in local_shards.into_iter().enumerate() {
            global_event_shards[sid].extend(items);
        }
    }

    // 6. Parallel Dedup & Validation -> Shard by hash(person)
    eprintln!("[{:.2}s] Parallel deduplicating & validating events...", t0.elapsed().as_secs_f64());
    let person_event_buffers: Vec<Vec<Vec<(Vec<u8>, ValidEvent)>>> = global_event_shards
        .into_par_iter()
        .map(|shard_items| {
            let mut events_by_id: AHashMap<Vec<u8>, CompactRawEvent> = AHashMap::with_capacity(shard_items.len());
            for item in shard_items {
                match events_by_id.get_mut(&item.event_id) {
                    Some(existing) => {
                        if item.ingest_ms < existing.ingest_ms {
                            *existing = item;
                        }
                    }
                    None => {
                        let eid = item.event_id.clone();
                        events_by_id.insert(eid, item);
                    }
                }
            }

            let mut person_shards: Vec<Vec<(Vec<u8>, ValidEvent)>> = (0..NUM_SHARDS).map(|_| Vec::new()).collect();
            for (_, rec) in events_by_id {
                if !rec.is_valid_syntax {
                    continue;
                }
                // Late event: ingest_ts - ts > 24 hours
                if rec.ingest_ms - rec.ts_ms > 86_400_000 {
                    continue;
                }

                let person = get_person(&rec.user_id);
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

    let mut person_event_shards: Vec<AHashMap<Vec<u8>, Vec<ValidEvent>>> = (0..NUM_SHARDS).map(|_| AHashMap::new()).collect();
    for sub_shards in person_event_buffers {
        for (sid, items) in sub_shards.into_iter().enumerate() {
            for (person, ev) in items {
                person_event_shards[sid].entry(person).or_default().push(ev);
            }
        }
    }

    // 7. Parallel Click Reading & Validation
    eprintln!("[{:.2}s] Parallel reading & validating clicks...", t0.elapsed().as_secs_f64());
    let clicks_dir = Path::new(&data_dir).join("clicks");
    let mut click_files: Vec<_> = std::fs::read_dir(&clicks_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "csv"))
        .map(|e| e.path())
        .collect();
    click_files.sort();

    let file_click_shards: Vec<Vec<Vec<RawClickItem>>> = click_files
        .par_iter()
        .map(|cf| {
            let content = std::fs::read(cf).unwrap_or_default();
            let mut lines = content.split(|&b| b == b'\n');
            lines.next(); // header
            let mut local_shards: Vec<Vec<RawClickItem>> = (0..NUM_SHARDS).map(|_| Vec::new()).collect();
            for line in lines {
                if let Some(f) = split_line(line, 4) {
                    let click_id = f[0];
                    if click_id.is_empty() {
                        continue;
                    }
                    let sid = get_shard(hasher.hash_one(click_id));
                    local_shards[sid].push(RawClickItem {
                        click_id: click_id.to_vec(),
                        user_id: f[1].to_vec(),
                        campaign_id: f[2].to_vec(),
                        ts_bytes: f[3].to_vec(),
                    });
                }
            }
            local_shards
        })
        .collect();

    let mut global_click_shards: Vec<Vec<RawClickItem>> = (0..NUM_SHARDS).map(|_| Vec::new()).collect();
    for local_shards in file_click_shards {
        for (sid, items) in local_shards.into_iter().enumerate() {
            global_click_shards[sid].extend(items);
        }
    }

    let person_click_buffers: Vec<Vec<Vec<(Vec<u8>, ValidClick)>>> = global_click_shards
        .into_par_iter()
        .map(|shard_items| {
            let mut clicks_by_id: AHashMap<Vec<u8>, RawClickItem> = AHashMap::with_capacity(shard_items.len());
            for item in shard_items {
                clicks_by_id.entry(item.click_id.clone()).or_insert(item);
            }

            let mut person_shards: Vec<Vec<(Vec<u8>, ValidClick)>> = (0..NUM_SHARDS).map(|_| Vec::new()).collect();
            for (_, item) in clicks_by_id {
                if item.user_id.is_empty() || item.campaign_id.is_empty() {
                    continue;
                }
                if !campaigns.contains_key(&item.campaign_id) {
                    continue;
                }
                let ts_ms = match parse_ts(&item.ts_bytes) {
                    Some(t) if t >= click_win_start_ms && t < win_end_ms => t,
                    _ => continue,
                };

                let person = get_person(&item.user_id);
                let p_shard = get_shard(hasher.hash_one(&person));
                person_shards[p_shard].push((person, ValidClick {
                    click_id: item.click_id,
                    campaign_id: item.campaign_id,
                    ts: ts_ms,
                }));
            }
            person_shards
        })
        .collect();

    let mut person_click_shards: Vec<AHashMap<Vec<u8>, Vec<ValidClick>>> = (0..NUM_SHARDS).map(|_| AHashMap::new()).collect();
    for sub_shards in person_click_buffers {
        for (sid, items) in sub_shards.into_iter().enumerate() {
            for (person, clk) in items {
                person_click_shards[sid].entry(person).or_default().push(clk);
            }
        }
    }

    // 8. Parallel Sessionization & Attribution across Person Shards
    eprintln!("[{:.2}s] Parallel sessionizing & attributing across {} shards...", t0.elapsed().as_secs_f64(), NUM_SHARDS);

    let shard_results: Vec<(Vec<((Vec<u8>, i32), UserDayAgg)>, AHashMap<(Vec<u8>, i32), CampDayAgg>)> = person_event_shards
        .into_par_iter()
        .zip(person_click_shards.into_par_iter())
        .map(|(events_map, mut clicks_map)| {
            for clks in clicks_map.values_mut() {
                clks.sort_by(|a, b| a.ts.cmp(&b.ts).then_with(|| a.click_id.cmp(&b.click_id)));
            }

            let mut local_user_daily: Vec<((Vec<u8>, i32), UserDayAgg)> = Vec::new();
            let mut local_camp_daily: AHashMap<(Vec<u8>, i32), CampDayAgg> = AHashMap::new();

            for (person, mut events) in events_map {
                events.sort_by_key(|e| e.ts);

                let mut i = 0;
                let n = events.len();
                let person_clk = clicks_map.get(&person);

                let mut person_day_aggs: AHashMap<i32, UserDayAgg> = AHashMap::new();

                while i < n {
                    let session_start = events[i].ts;
                    let session_day = epoch_ms_to_date_days(session_start);

                    let mut session_events_count: i64 = 0;
                    let mut session_revenue: i64 = 0;
                    let mut session_has_purchase = false;
                    let mut session_cat_rev = [0i64; 32];

                    let mut cur_ts = session_start;
                    while i < n {
                        let ev = &events[i];
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

                    let u_agg = person_day_aggs.entry(session_day).or_insert_with(|| UserDayAgg {
                        sessions: 0,
                        events: 0,
                        revenue_cents: 0,
                        cat_rev: [0i64; 32],
                    });
                    u_agg.sessions += 1;
                    u_agg.events += session_events_count;
                    u_agg.revenue_cents += session_revenue;
                    for c_idx in 0..32 {
                        u_agg.cat_rev[c_idx] += session_cat_rev[c_idx];
                    }

                    // Attribution & Assists
                    let mut credited_camp = b"organic".to_vec();
                    let mut assisting_camps = AHashSet::new();

                    if let Some(clks) = person_clk {
                        let min_click_ts = session_start - 604_800_000;
                        let start_idx = clks.partition_point(|c| c.ts < min_click_ts);
                        let end_idx = clks.partition_point(|c| c.ts <= session_start);

                        if start_idx < end_idx {
                            let window_clicks = &clks[start_idx..end_idx];
                            let mut best_click: Option<&ValidClick> = None;
                            for c in window_clicks {
                                assisting_camps.insert(&c.campaign_id);
                                match best_click {
                                    None => best_click = Some(c),
                                    Some(curr) => {
                                        if c.ts > curr.ts || (c.ts == curr.ts && c.click_id < curr.click_id) {
                                            best_click = Some(c);
                                        }
                                    }
                                }
                            }
                            if let Some(best) = best_click {
                                credited_camp = best.campaign_id.clone();
                            }
                        }
                    }

                    let c_agg = local_camp_daily.entry((credited_camp, session_day)).or_default();
                    c_agg.sessions += 1;
                    if session_has_purchase {
                        c_agg.purchasing_sessions += 1;
                    }
                    c_agg.revenue_cents += session_revenue;

                    for ac in assisting_camps {
                        local_camp_daily.entry((ac.clone(), session_day)).or_default().assisted_sessions += 1;
                    }
                }

                for (day_days, agg) in person_day_aggs {
                    local_user_daily.push(((person.clone(), day_days), agg));
                }
            }

            (local_user_daily, local_camp_daily)
        })
        .collect();

    // 9. Write Parquet: user_daily
    eprintln!("[{:.2}s] Writing user_daily Parquet...", t0.elapsed().as_secs_f64());
    let user_out_dir = Path::new(&out_dir).join("user_daily");
    std::fs::create_dir_all(&user_out_dir)?;

    let total_user_rows: usize = shard_results.iter().map(|(u, _)| u.len()).sum();
    let mut u_user_ids = Vec::with_capacity(total_user_rows);
    let mut u_days = Vec::with_capacity(total_user_rows);
    let mut u_sessions = Vec::with_capacity(total_user_rows);
    let mut u_events = Vec::with_capacity(total_user_rows);
    let mut u_revenue = Vec::with_capacity(total_user_rows);
    let mut u_top_cats: Vec<Option<String>> = Vec::with_capacity(total_user_rows);

    for (user_items, _) in &shard_results {
        for ((person, day_days), agg) in user_items {
            let user_id_str = String::from_utf8_lossy(person).into_owned();
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
                            let curr_name = &category_names[curr_id as usize];
                            let cand_name = &category_names[cat_idx];
                            if rev > best_rev || (rev == best_rev && cand_name < curr_name) {
                                top_cat_id = Some(cat_idx as u8);
                                best_rev = rev;
                            }
                        }
                    }
                }
            }

            let top_cat_str = top_cat_id.map(|id| String::from_utf8_lossy(&category_names[id as usize]).into_owned());

            u_user_ids.push(user_id_str);
            u_days.push(*day_days);
            u_sessions.push(agg.sessions);
            u_events.push(agg.events);
            u_revenue.push(agg.revenue_cents);
            u_top_cats.push(top_cat_str);
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

    let u_top_cat_array: ArrayRef = Arc::new(StringArray::from(u_top_cats.iter().map(|s| s.as_deref()).collect::<Vec<_>>()));
    let user_batch = RecordBatch::try_new(
        user_schema.clone(),
        vec![
            Arc::new(StringArray::from(u_user_ids)) as ArrayRef,
            Arc::new(Date32Array::from(u_days)) as ArrayRef,
            Arc::new(Int64Array::from(u_sessions)) as ArrayRef,
            Arc::new(Int64Array::from(u_events)) as ArrayRef,
            Arc::new(Int64Array::from(u_revenue)) as ArrayRef,
            u_top_cat_array,
        ],
    )?;

    let user_parquet_path = user_out_dir.join("part-0.parquet");
    let user_file = File::create(user_parquet_path)?;
    let mut writer = ArrowWriter::try_new(user_file, user_schema, None)?;
    writer.write(&user_batch)?;
    writer.close()?;

    // 10. Merge & Write Parquet: campaign_daily
    eprintln!("[{:.2}s] Merging & writing campaign_daily Parquet...", t0.elapsed().as_secs_f64());
    let mut global_camp_daily: AHashMap<(Vec<u8>, i32), CampDayAgg> = AHashMap::new();
    for (_, camp_map) in shard_results {
        for (k, v) in camp_map {
            let entry = global_camp_daily.entry(k).or_default();
            entry.sessions += v.sessions;
            entry.purchasing_sessions += v.purchasing_sessions;
            entry.revenue_cents += v.revenue_cents;
            entry.assisted_sessions += v.assisted_sessions;
        }
    }

    let camp_out_dir = Path::new(&out_dir).join("campaign_daily");
    std::fs::create_dir_all(&camp_out_dir)?;

    let mut c_ids = Vec::with_capacity(global_camp_daily.len());
    let mut c_channels = Vec::with_capacity(global_camp_daily.len());
    let mut c_days = Vec::with_capacity(global_camp_daily.len());
    let mut c_sessions = Vec::with_capacity(global_camp_daily.len());
    let mut c_purchasing = Vec::with_capacity(global_camp_daily.len());
    let mut c_revenue = Vec::with_capacity(global_camp_daily.len());
    let mut c_assisted = Vec::with_capacity(global_camp_daily.len());

    for ((camp_id, day_days), agg) in global_camp_daily {
        let channel = if camp_id == b"organic" {
            "organic".to_string()
        } else {
            campaigns.get(&camp_id)
                .map(|ch| String::from_utf8_lossy(ch).into_owned())
                .unwrap_or_else(|| "organic".to_string())
        };

        c_ids.push(String::from_utf8_lossy(&camp_id).into_owned());
        c_channels.push(channel);
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
            Arc::new(StringArray::from(c_ids)) as ArrayRef,
            Arc::new(StringArray::from(c_channels)) as ArrayRef,
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

    eprintln!("[{:.2}s] Pipeline finished successfully!", t0.elapsed().as_secs_f64());
    Ok(())
}
