use std::collections::BTreeMap;
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use ahash::{AHashMap, AHashSet};
use arrow::array::{
    ArrayRef, Date32Array, Int64Array, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::{Datelike, NaiveDate, NaiveDateTime, Timelike};
use memmap2::Mmap;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use rayon::prelude::*;

// ---------------------------------------------------------------------------
// Helpers: CSV dialect and fields
// ---------------------------------------------------------------------------

#[inline(always)]
fn parse_field(field: &[u8]) -> Option<&[u8]> {
    if field.is_empty() {
        return Some(b"");
    }
    if field[0] == b'"' {
        if field.len() >= 2 && field[field.len() - 1] == b'"' {
            let inner = &field[1..field.len() - 1];
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
// Timestamp parser: F1, F2, F3
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

use chrono::DateTime;

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
// Event and Click Data Structures
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct RawEventRecord {
    ingest_ms: i64,
    user_id: Vec<u8>,
    ts_bytes: Vec<u8>,
    ev_type: Vec<u8>,
    product_id: Vec<u8>,
    price_bytes: Vec<u8>,
}

struct ValidEvent {
    person: Vec<u8>,
    ts: i64,
    is_purchase: bool,
    price_cents: i64,
    category: Vec<u8>,
}

struct ValidClick {
    click_id: Vec<u8>,
    person: Vec<u8>,
    campaign_id: Vec<u8>,
    ts: i64,
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

    eprintln!("[{:.2}s] Starting Shopping Sessions engine (Rust)", t0.elapsed().as_secs_f64());
    eprintln!("DATA_DIR: {}, OUTPUT_DIR: {}", data_dir, out_dir);

    // 1. Manifest
    let manifest_path = Path::new(&data_dir).join("manifest.json");
    let manifest_bytes = std::fs::read(&manifest_path)?;
    let manifest_json: serde_json::Value = serde_json::from_slice(&manifest_bytes)?;
    let win_start_str = manifest_json["window_start"].as_str().unwrap();
    let win_end_str = manifest_json["window_end"].as_str().unwrap();
    let win_start_ms = parse_ts(win_start_str.as_bytes()).unwrap();
    let win_end_ms = parse_ts(win_end_str.as_bytes()).unwrap();
    let click_win_start_ms = win_start_ms - 7 * 86_400_000;

    // 2. Products
    let mut products = AHashMap::new();
    let products_path = Path::new(&data_dir).join("products.csv");
    if products_path.exists() {
        let content = std::fs::read(&products_path)?;
        let mut lines = content.split(|&b| b == b'\n');
        lines.next(); // header
        for line in lines {
            if let Some(f) = split_line(line, 2) {
                if !f[0].is_empty() && !f[1].is_empty() {
                    products.insert(f[0].to_vec(), f[1].to_vec());
                }
            }
        }
    }

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

    // 5. Events
    eprintln!("[{:.2}s] Reading events...", t0.elapsed().as_secs_f64());
    let events_dir = Path::new(&data_dir).join("events");
    let mut event_files: Vec<_> = std::fs::read_dir(&events_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "csv"))
        .map(|e| e.path())
        .collect();
    event_files.sort();

    // Parallel parse of event files
    let parsed_event_batches: Vec<Vec<(Vec<u8>, RawEventRecord)>> = event_files
        .par_iter()
        .map(|ef| {
            let content = std::fs::read(ef).unwrap_or_default();
            let mut lines = content.split(|&b| b == b'\n');
            lines.next(); // header
            let mut batch = Vec::new();
            for line in lines {
                if let Some(f) = split_line(line, 7) {
                    let event_id = f[0];
                    if event_id.is_empty() {
                        continue;
                    }
                    if let Some(ingest_ms) = parse_ts(f[3]) {
                        batch.push((
                            event_id.to_vec(),
                            RawEventRecord {
                                ingest_ms,
                                user_id: f[1].to_vec(),
                                ts_bytes: f[2].to_vec(),
                                ev_type: f[4].to_vec(),
                                product_id: f[5].to_vec(),
                                price_bytes: f[6].to_vec(),
                            },
                        ));
                    }
                }
            }
            batch
        })
        .collect();

    eprintln!("[{:.2}s] Deduplicating and validating events...", t0.elapsed().as_secs_f64());
    // Dedup events on event_id keeping smallest ingest_ts
    let mut events_by_id: AHashMap<Vec<u8>, RawEventRecord> = AHashMap::new();
    for batch in parsed_event_batches {
        for (eid, rec) in batch {
            match events_by_id.get_mut(&eid) {
                Some(existing) => {
                    if rec.ingest_ms < existing.ingest_ms {
                        *existing = rec;
                    }
                }
                None => {
                    events_by_id.insert(eid, rec);
                }
            }
        }
    }

    // Validate kept copies
    let mut person_events: AHashMap<Vec<u8>, Vec<ValidEvent>> = AHashMap::new();
    for (_, rec) in events_by_id {
        if rec.user_id.is_empty() || rec.product_id.is_empty() {
            continue;
        }
        if rec.ev_type != b"view" && rec.ev_type != b"cart" && rec.ev_type != b"purchase" {
            continue;
        }
        let ts_ms = match parse_ts(&rec.ts_bytes) {
            Some(t) if t >= win_start_ms && t < win_end_ms => t,
            _ => continue,
        };

        let mut price_val: i64 = 0;
        let is_purchase = rec.ev_type == b"purchase";
        if is_purchase {
            if rec.price_bytes.is_empty() || !rec.price_bytes.iter().all(|&c| c >= b'0' && c <= b'9') {
                continue;
            }
            price_val = match parse_digits(&rec.price_bytes) {
                Some(p) if p >= 1 && p <= 10_000_000 => p as i64,
                _ => continue,
            };
        }

        // Late event check: ingest_ts - ts > 24 hours (86_400_000 ms)
        if rec.ingest_ms - ts_ms > 86_400_000 {
            continue;
        }

        let person = dsu.get_canonical_name(&rec.user_id);
        let category = products.get(&rec.product_id)
            .cloned()
            .unwrap_or_else(|| b"unknown".to_vec());

        person_events.entry(person.clone()).or_default().push(ValidEvent {
            person,
            ts: ts_ms,
            is_purchase,
            price_cents: price_val,
            category,
        });
    }

    // 6. Clicks
    eprintln!("[{:.2}s] Reading and validating clicks...", t0.elapsed().as_secs_f64());
    let clicks_dir = Path::new(&data_dir).join("clicks");
    let mut click_files: Vec<_> = std::fs::read_dir(&clicks_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "csv"))
        .map(|e| e.path())
        .collect();
    click_files.sort();

    let parsed_click_batches: Vec<Vec<(Vec<u8>, (Vec<u8>, Vec<u8>, Vec<u8>))>> = click_files
        .par_iter()
        .map(|cf| {
            let content = std::fs::read(cf).unwrap_or_default();
            let mut lines = content.split(|&b| b == b'\n');
            lines.next(); // header
            let mut batch = Vec::new();
            for line in lines {
                if let Some(f) = split_line(line, 4) {
                    let click_id = f[0];
                    if click_id.is_empty() {
                        continue;
                    }
                    batch.push((
                        click_id.to_vec(),
                        (f[1].to_vec(), f[2].to_vec(), f[3].to_vec()),
                    ));
                }
            }
            batch
        })
        .collect();

    let mut clicks_by_id: AHashMap<Vec<u8>, (Vec<u8>, Vec<u8>, Vec<u8>)> = AHashMap::new();
    for batch in parsed_click_batches {
        for (cid, rec) in batch {
            clicks_by_id.entry(cid).or_insert(rec);
        }
    }

    let mut person_clicks: AHashMap<Vec<u8>, Vec<ValidClick>> = AHashMap::new();
    for (click_id, (user_id, campaign_id, ts_bytes)) in clicks_by_id {
        if user_id.is_empty() || campaign_id.is_empty() {
            continue;
        }
        if !campaigns.contains_key(&campaign_id) {
            continue;
        }
        let ts_ms = match parse_ts(&ts_bytes) {
            Some(t) if t >= click_win_start_ms && t < win_end_ms => t,
            _ => continue,
        };

        let person = dsu.get_canonical_name(&user_id);
        person_clicks.entry(person.clone()).or_default().push(ValidClick {
            click_id,
            person,
            campaign_id,
            ts: ts_ms,
        });
    }

    // Sort clicks per person by ts ASC, then click_id ASC
    for clicks in person_clicks.values_mut() {
        clicks.sort_by(|a, b| {
            a.ts.cmp(&b.ts).then_with(|| a.click_id.cmp(&b.click_id))
        });
    }

    // 7. Sessionization, Attribution & Assists per Person
    eprintln!("[{:.2}s] Sessionizing & attributing...", t0.elapsed().as_secs_f64());

    // Daily aggregations
    // Key: (user_id, day_days) -> (sessions, events, revenue_cents, top_cat_map)
    struct UserDayAgg {
        sessions: i64,
        events: i64,
        revenue_cents: i64,
        cat_rev: AHashMap<Vec<u8>, i64>,
    }
    let mut user_daily_agg: AHashMap<(Vec<u8>, i32), UserDayAgg> = AHashMap::new();

    // Key: (campaign_id, day_days) -> (sessions, purchasing_sessions, revenue_cents, assisted_sessions)
    #[derive(Default)]
    struct CampDayAgg {
        sessions: i64,
        purchasing_sessions: i64,
        revenue_cents: i64,
        assisted_sessions: i64,
    }
    let mut camp_daily_agg: AHashMap<(Vec<u8>, i32), CampDayAgg> = AHashMap::new();

    for (person, mut events) in person_events {
        events.sort_by_key(|e| e.ts);

        // Sessionize
        let mut i = 0;
        let n = events.len();
        let person_clk = person_clicks.get(&person);

        while i < n {
            let session_start = events[i].ts;
            let session_day = epoch_ms_to_date_days(session_start);

            let mut session_events_count: i64 = 0;
            let mut session_revenue: i64 = 0;
            let mut session_has_purchase = false;
            let mut session_cat_rev: AHashMap<Vec<u8>, i64> = AHashMap::new();

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
                    session_revenue += ev.price_cents;
                    *session_cat_rev.entry(ev.category.clone()).or_insert(0) += ev.price_cents;
                }
                i += 1;
            }

            // Update user daily
            let u_agg = user_daily_agg.entry((person.clone(), session_day)).or_insert_with(|| UserDayAgg {
                sessions: 0,
                events: 0,
                revenue_cents: 0,
                cat_rev: AHashMap::new(),
            });
            u_agg.sessions += 1;
            u_agg.events += session_events_count;
            u_agg.revenue_cents += session_revenue;
            for (c, rev) in session_cat_rev {
                *u_agg.cat_rev.entry(c).or_insert(0) += rev;
            }

            // Attribution & Assists
            let mut credited_camp = b"organic".to_vec();
            let mut assisting_camps = AHashSet::new();

            if let Some(clks) = person_clk {
                let min_click_ts = session_start - 604_800_000;
                // Binary search for window [min_click_ts, session_start]
                let start_idx = clks.partition_point(|c| c.ts < min_click_ts);
                let end_idx = clks.partition_point(|c| c.ts <= session_start);

                if start_idx < end_idx {
                    let window_clicks = &clks[start_idx..end_idx];
                    // Latest click has largest ts
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

            // Update campaign daily for credited campaign
            let c_agg = camp_daily_agg.entry((credited_camp, session_day)).or_default();
            c_agg.sessions += 1;
            if session_has_purchase {
                c_agg.purchasing_sessions += 1;
            }
            c_agg.revenue_cents += session_revenue;

            // Update campaign daily for assisting campaigns
            for ac in assisting_camps {
                camp_daily_agg.entry((ac.clone(), session_day)).or_default().assisted_sessions += 1;
            }
        }
    }

    // 8. Write Parquet: user_daily
    eprintln!("[{:.2}s] Writing user_daily Parquet...", t0.elapsed().as_secs_f64());
    let user_out_dir = Path::new(&out_dir).join("user_daily");
    std::fs::create_dir_all(&user_out_dir)?;

    let mut u_user_ids = Vec::with_capacity(user_daily_agg.len());
    let mut u_days = Vec::with_capacity(user_daily_agg.len());
    let mut u_sessions = Vec::with_capacity(user_daily_agg.len());
    let mut u_events = Vec::with_capacity(user_daily_agg.len());
    let mut u_revenue = Vec::with_capacity(user_daily_agg.len());
    let mut u_top_cats: Vec<Option<String>> = Vec::with_capacity(user_daily_agg.len());

    for ((person, day_days), agg) in user_daily_agg {
        let user_id_str = String::from_utf8_lossy(&person).into_owned();
        let top_cat = if agg.cat_rev.is_empty() {
            None
        } else {
            let mut best_cat: Option<(&Vec<u8>, i64)> = None;
            for (cat, rev) in &agg.cat_rev {
                match best_cat {
                    None => best_cat = Some((cat, *rev)),
                    Some((curr_cat, curr_rev)) => {
                        if *rev > curr_rev || (*rev == curr_rev && cat < curr_cat) {
                            best_cat = Some((cat, *rev));
                        }
                    }
                }
            }
            best_cat.map(|(cat, _)| String::from_utf8_lossy(cat).into_owned())
        };

        u_user_ids.push(user_id_str);
        u_days.push(day_days);
        u_sessions.push(agg.sessions);
        u_events.push(agg.events);
        u_revenue.push(agg.revenue_cents);
        u_top_cats.push(top_cat);
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

    // 9. Write Parquet: campaign_daily
    eprintln!("[{:.2}s] Writing campaign_daily Parquet...", t0.elapsed().as_secs_f64());
    let camp_out_dir = Path::new(&out_dir).join("campaign_daily");
    std::fs::create_dir_all(&camp_out_dir)?;

    let mut c_ids = Vec::with_capacity(camp_daily_agg.len());
    let mut c_channels = Vec::with_capacity(camp_daily_agg.len());
    let mut c_days = Vec::with_capacity(camp_daily_agg.len());
    let mut c_sessions = Vec::with_capacity(camp_daily_agg.len());
    let mut c_purchasing = Vec::with_capacity(camp_daily_agg.len());
    let mut c_revenue = Vec::with_capacity(camp_daily_agg.len());
    let mut c_assisted = Vec::with_capacity(camp_daily_agg.len());

    for ((camp_id, day_days), agg) in camp_daily_agg {
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
