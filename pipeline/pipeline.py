#!/usr/bin/env python3
"""
Shopping Sessions: High-performance DuckDB + Python pipeline.
Implements all rules from rules.md.
"""
import datetime
import glob
import json
import os
import sys
import tempfile

import duckdb
import pyarrow as pa

DATA = os.environ.get("DATA_DIR", "/data")
OUT = os.environ.get("OUTPUT_DIR", "/output")
SCRATCH = os.environ.get("SCRATCH_DIR", tempfile.gettempdir())
NCPU = int(os.environ.get("NCPU", os.cpu_count() or 1))
MEM_GB = int(os.environ.get("MEM_GB", "8"))

os.makedirs(os.path.join(SCRATCH, "duckdb_tmp"), exist_ok=True)
os.makedirs(os.path.join(OUT, "user_daily"), exist_ok=True)
os.makedirs(os.path.join(OUT, "campaign_daily"), exist_ok=True)

# ---------------------------------------------------------
# CSV & Dialect Helpers
# ---------------------------------------------------------

def parse_field(f: bytes):
    if not f:
        return True, b""
    if f.startswith(b'"'):
        if len(f) >= 2 and f.endswith(b'"') and f.count(b'"') == 2:
            return True, f[1:-1]
        return False, b""
    if b'"' in f:
        return False, b""
    return True, f

def read_raw_csv_lines(path: str):
    if not os.path.exists(path):
        return []
    with open(path, "rb") as f:
        content = f.read()
    if not content:
        return []
    lines = content.split(b"\n")
    if lines and lines[-1] == b"":
        lines.pop()
    if not lines:
        return []
    return lines[1:]  # Skip header row (line 0)

def parse_line(line: bytes, num_fields: int):
    if not line:
        return None
    parts = line.split(b",")
    if len(parts) != num_fields:
        return None
    cleaned = []
    for p in parts:
        ok, val = parse_field(p)
        if not ok:
            return None
        cleaned.append(val)
    return cleaned

def parse_ts(b: bytes):
    if not b:
        return None
    try:
        s = b.decode("ascii")
    except UnicodeDecodeError:
        return None

    # Form F1: 2026-10-03T23:58:10Z (len 20)
    if (len(s) == 20 and s[10] == "T" and s[19] == "Z" and
            s[4] == "-" and s[7] == "-" and s[13] == ":" and s[16] == ":"):
        try:
            y, m, d = int(s[0:4]), int(s[5:7]), int(s[8:10])
            H, M, S = int(s[11:13]), int(s[14:16]), int(s[17:19])
            dt = datetime.datetime(y, m, d, H, M, S, tzinfo=datetime.timezone.utc)
            return int(dt.timestamp() * 1000)
        except ValueError:
            return None

    # Form F2: 2026-10-03T19:58:10-04:00 (len 25)
    if (len(s) == 25 and s[10] == "T" and s[4] == "-" and s[7] == "-" and
            s[13] == ":" and s[16] == ":" and s[19] in ("+", "-") and s[22] == ":"):
        try:
            y, m, d = int(s[0:4]), int(s[5:7]), int(s[8:10])
            H, M, S = int(s[11:13]), int(s[14:16]), int(s[17:19])
            dt = datetime.datetime(y, m, d, H, M, S, tzinfo=datetime.timezone.utc)
            base_ms = int(dt.timestamp() * 1000)

            off_h = int(s[20:22])
            off_m = int(s[23:25])
            if not (0 <= off_h <= 23 and 0 <= off_m <= 59):
                return None
            sign = 1 if s[19] == "+" else -1
            off_ms = sign * (off_h * 3600 + off_m * 60) * 1000
            return base_ms - off_ms
        except ValueError:
            return None

    # Form F3: Digits only
    if b.isdigit():
        try:
            return int(s)
        except ValueError:
            return None

    return None

# ---------------------------------------------------------
# Load Manifest & Lookups
# ---------------------------------------------------------

with open(os.path.join(DATA, "manifest.json")) as f:
    manifest = json.load(f)
window_start_ms = parse_ts(manifest["window_start"].encode("ascii"))
window_end_ms = parse_ts(manifest["window_end"].encode("ascii"))
click_window_start_ms = window_start_ms - 7 * 86_400_000

products = {}
products_file = os.path.join(DATA, "products.csv")
if os.path.exists(products_file):
    for line in read_raw_csv_lines(products_file):
        parts = parse_line(line, 2)
        if parts and parts[0] and parts[1]:
            p_id = parts[0].decode("utf-8", "surrogateescape")
            cat = parts[1].decode("utf-8", "surrogateescape")
            products[p_id] = cat

campaigns = {}
campaigns_file = os.path.join(DATA, "campaigns.csv")
if os.path.exists(campaigns_file):
    for line in read_raw_csv_lines(campaigns_file):
        parts = parse_line(line, 2)
        if parts and parts[0] and parts[1]:
            c_id = parts[0].decode("utf-8", "surrogateescape")
            channel = parts[1].decode("utf-8", "surrogateescape")
            campaigns[c_id] = channel

# ---------------------------------------------------------
# Union-Find for Linked IDs
# ---------------------------------------------------------

parent = {}

def find(x):
    curr = x
    while parent.get(curr, curr) != curr:
        curr = parent[curr]
    p = x
    while p != curr:
        nxt = parent.get(p, p)
        parent[p] = curr
        p = nxt
    return curr

def union(x, y):
    rx = find(x)
    ry = find(y)
    if rx != ry:
        if rx < ry:
            parent[ry] = rx
        else:
            parent[rx] = ry

links_glob = sorted(glob.glob(os.path.join(DATA, "links", "*.csv")))
for lf in links_glob:
    for line in read_raw_csv_lines(lf):
        parts = parse_line(line, 2)
        if parts:
            u_a, u_b = parts[0], parts[1]
            if u_a and u_b:
                s_a = u_a.decode("utf-8", "surrogateescape")
                s_b = u_b.decode("utf-8", "surrogateescape")
                union(s_a, s_b)

# ---------------------------------------------------------
# Parse & Validate Events
# ---------------------------------------------------------

events_glob = sorted(glob.glob(os.path.join(DATA, "events", "*.csv")))
events_by_id = {}

for ef in events_glob:
    for line in read_raw_csv_lines(ef):
        parts = parse_line(line, 7)
        if parts:
            e_id = parts[0]
            if not e_id:
                continue
            ingest_ts_raw = parts[3]
            ingest_ms = parse_ts(ingest_ts_raw)
            if ingest_ms is None:
                continue
            
            # Dedup: keep copy with smallest ingest_ts
            if e_id not in events_by_id:
                events_by_id[e_id] = (ingest_ms, parts)
            else:
                if ingest_ms < events_by_id[e_id][0]:
                    events_by_id[e_id] = (ingest_ms, parts)

valid_events_person = []
valid_events_ts = []
valid_events_type = []
valid_events_cat = []
valid_events_price = []

for ingest_ms, parts in events_by_id.values():
    _, u_id, ts_raw, _, ev_type, prod_id, price_raw = parts
    if not u_id or not prod_id:
        continue
    if ev_type not in (b"view", b"cart", b"purchase"):
        continue
    ts_ms = parse_ts(ts_raw)
    if ts_ms is None or not (window_start_ms <= ts_ms < window_end_ms):
        continue
    
    price_val = 0
    if ev_type == b"purchase":
        if not price_raw.isdigit():
            continue
        try:
            price_val = int(price_raw.decode("ascii"))
        except ValueError:
            continue
        if not (1 <= price_val <= 10_000_000):
            continue
            
    # Late events: ingest_ts - ts > 24 hours (86,400,000 ms)
    if ingest_ms - ts_ms > 86_400_000:
        continue

    person = find(u_id.decode("utf-8", "surrogateescape"))
    p_str = prod_id.decode("utf-8", "surrogateescape")
    cat = products.get(p_str, "unknown")

    valid_events_person.append(person)
    valid_events_ts.append(ts_ms)
    valid_events_type.append(ev_type.decode("ascii"))
    valid_events_cat.append(cat)
    valid_events_price.append(price_val)

# ---------------------------------------------------------
# Parse & Validate Clicks
# ---------------------------------------------------------

clicks_glob = sorted(glob.glob(os.path.join(DATA, "clicks", "*.csv")))
clicks_by_id = {}

for cf in clicks_glob:
    for line in read_raw_csv_lines(cf):
        parts = parse_line(line, 4)
        if parts:
            c_id = parts[0]
            if not c_id:
                continue
            if c_id not in clicks_by_id:
                clicks_by_id[c_id] = parts

valid_clicks_id = []
valid_clicks_person = []
valid_clicks_camp = []
valid_clicks_ts = []

for c_id, parts in clicks_by_id.items():
    _, u_id, camp_id, ts_raw = parts
    if not u_id or not camp_id:
        continue
    camp_str = camp_id.decode("utf-8", "surrogateescape")
    if camp_str not in campaigns:
        continue
    ts_ms = parse_ts(ts_raw)
    if ts_ms is None or not (click_window_start_ms <= ts_ms < window_end_ms):
        continue

    person = find(u_id.decode("utf-8", "surrogateescape"))
    cid_str = c_id.decode("utf-8", "surrogateescape")

    valid_clicks_id.append(cid_str)
    valid_clicks_person.append(person)
    valid_clicks_camp.append(camp_str)
    valid_clicks_ts.append(ts_ms)

# ---------------------------------------------------------
# DuckDB Analytics & Output Tables
# ---------------------------------------------------------

con = duckdb.connect()
con.execute(f"SET threads = {NCPU}")
con.execute(f"SET memory_limit = '{max(1, int(MEM_GB * 0.8))}GB'")
con.execute(f"SET temp_directory = '{os.path.join(SCRATCH, 'duckdb_tmp')}'")
con.execute("SET preserve_insertion_order = false")

# Register Python data in DuckDB via PyArrow
events_schema = pa.schema([
    ('person', pa.string()),
    ('ts', pa.int64()),
    ('event_type', pa.string()),
    ('category', pa.string()),
    ('price', pa.int64())
])
events_tbl = pa.Table.from_pydict({
    'person': valid_events_person,
    'ts': valid_events_ts,
    'event_type': valid_events_type,
    'category': valid_events_cat,
    'price': valid_events_price
}, schema=events_schema)
con.register('raw_events', events_tbl)

clicks_schema = pa.schema([
    ('click_id', pa.string()),
    ('person', pa.string()),
    ('campaign_id', pa.string()),
    ('ts', pa.int64())
])
clicks_tbl = pa.Table.from_pydict({
    'click_id': valid_clicks_id,
    'person': valid_clicks_person,
    'campaign_id': valid_clicks_camp,
    'ts': valid_clicks_ts
}, schema=clicks_schema)
con.register('raw_clicks', clicks_tbl)

campaigns_schema = pa.schema([
    ('campaign_id', pa.string()),
    ('channel', pa.string())
])
campaigns_tbl = pa.Table.from_pydict({
    'campaign_id': list(campaigns.keys()),
    'channel': list(campaigns.values())
}, schema=campaigns_schema)
con.register('raw_campaigns', campaigns_tbl)

# 1. Sessionize Events
con.execute("""
    CREATE TABLE sessionized_events AS
    WITH ordered_events AS (
        SELECT
            person,
            ts,
            event_type,
            category,
            price,
            CASE 
                WHEN LAG(ts) OVER (PARTITION BY person ORDER BY ts) IS NULL THEN 1
                WHEN ts - LAG(ts) OVER (PARTITION BY person ORDER BY ts) > 1800000 THEN 1
                ELSE 0
            END AS is_new_session
        FROM raw_events
    ),
    with_session_id AS (
        SELECT
            person,
            ts,
            event_type,
            category,
            price,
            SUM(is_new_session) OVER (PARTITION BY person ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
        FROM ordered_events
    )
    SELECT
        person,
        session_id,
        MIN(ts) OVER (PARTITION BY person, session_id) AS session_start,
        epoch_ms(MIN(ts) OVER (PARTITION BY person, session_id))::DATE AS session_day,
        ts,
        event_type,
        category,
        price
    FROM with_session_id
""")

# Distinct Sessions Summary
con.execute("""
    CREATE TABLE sessions AS
    SELECT
        person,
        session_id,
        MIN(session_start) AS session_start,
        MIN(session_day) AS session_day,
        COUNT(*) AS event_count,
        SUM(price) AS revenue_cents,
        MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS has_purchase
    FROM sessionized_events
    GROUP BY person, session_id
""")

# Top Category per (user, day)
con.execute("""
    CREATE TABLE user_day_top_cat AS
    WITH cat_revenues AS (
        SELECT
            person,
            session_day,
            category,
            SUM(price) AS cat_revenue
        FROM sessionized_events
        WHERE event_type = 'purchase'
        GROUP BY person, session_day, category
    ),
    ranked_cats AS (
        SELECT
            person,
            session_day,
            category,
            ROW_NUMBER() OVER (
                PARTITION BY person, session_day 
                ORDER BY cat_revenue DESC, category ASC
            ) AS rnk
        FROM cat_revenues
    )
    SELECT person, session_day, category AS top_category
    FROM ranked_cats
    WHERE rnk = 1
""")

# User Daily Aggregations
con.execute(f"""
    COPY (
        WITH user_day_agg AS (
            SELECT
                person AS user_id,
                session_day AS day,
                COUNT(*) AS sessions,
                SUM(event_count) AS events,
                SUM(revenue_cents) AS revenue_cents
            FROM sessions
            GROUP BY person, session_day
        )
        SELECT
            u.user_id,
            u.day,
            u.sessions::BIGINT AS sessions,
            u.events::BIGINT AS events,
            u.revenue_cents::BIGINT AS revenue_cents,
            t.top_category
        FROM user_day_agg u
        LEFT JOIN user_day_top_cat t 
            ON u.user_id = t.person AND u.day = t.session_day
        ORDER BY u.day, u.user_id
    ) TO '{os.path.join(OUT, "user_daily", "part-0.parquet")}' (FORMAT parquet)
""")

# 2. Credit Sessions to Clicks
con.execute("""
    CREATE TABLE session_credited AS
    WITH matching_clicks AS (
        SELECT
            s.person,
            s.session_id,
            c.click_id,
            c.campaign_id,
            c.ts AS click_ts,
            ROW_NUMBER() OVER (
                PARTITION BY s.person, s.session_id
                ORDER BY c.ts DESC, c.click_id ASC
            ) AS rnk
        FROM sessions s
        JOIN raw_clicks c
            ON s.person = c.person
           AND c.ts <= s.session_start
           AND s.session_start - c.ts <= 604800000
    )
    SELECT
        s.person,
        s.session_id,
        s.session_day,
        s.has_purchase,
        s.revenue_cents,
        COALESCE(m.campaign_id, 'organic') AS campaign_id,
        CASE WHEN m.campaign_id IS NULL THEN 'organic' ELSE rc.channel END AS channel
    FROM sessions s
    LEFT JOIN matching_clicks m
        ON s.person = m.person AND s.session_id = m.session_id AND m.rnk = 1
    LEFT JOIN raw_campaigns rc
        ON m.campaign_id = rc.campaign_id
""")

# 3. Campaign Assists
con.execute("""
    CREATE TABLE session_assists AS
    SELECT DISTINCT
        c.campaign_id,
        rc.channel,
        s.session_day,
        s.person,
        s.session_id
    FROM sessions s
    JOIN raw_clicks c
        ON s.person = c.person
       AND c.ts <= s.session_start
       AND s.session_start - c.ts <= 604800000
    JOIN raw_campaigns rc
        ON c.campaign_id = rc.campaign_id
""")

# 4. Campaign Daily Output
con.execute(f"""
    COPY (
        WITH credited_summary AS (
            SELECT
                campaign_id,
                channel,
                session_day AS day,
                1 AS sessions,
                has_purchase AS purchasing_sessions,
                revenue_cents,
                0 AS assisted_sessions
            FROM session_credited
        ),
        assists_summary AS (
            SELECT
                campaign_id,
                channel,
                session_day AS day,
                0 AS sessions,
                0 AS purchasing_sessions,
                0 AS revenue_cents,
                1 AS assisted_sessions
            FROM session_assists
        ),
        all_campaign_rows AS (
            SELECT * FROM credited_summary
            UNION ALL
            SELECT * FROM assists_summary
        )
        SELECT
            campaign_id,
            MAX(channel) AS channel,
            day,
            SUM(sessions)::BIGINT AS sessions,
            SUM(purchasing_sessions)::BIGINT AS purchasing_sessions,
            SUM(revenue_cents)::BIGINT AS revenue_cents,
            SUM(assisted_sessions)::BIGINT AS assisted_sessions
        FROM all_campaign_rows
        GROUP BY campaign_id, day
        ORDER BY day, campaign_id
    ) TO '{os.path.join(OUT, "campaign_daily", "part-0.parquet")}' (FORMAT parquet)
""")

print("pipeline finished successfully")
