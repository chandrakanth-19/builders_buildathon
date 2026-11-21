# Shopping Sessions Build Plan

*For the executing Claude model. Read this whole page, then rules.md, before writing code. Cutoff: Sun 22 Nov 2026, 9:30 PM IST. The user has an exam Sun 17:00-20:00 IST, so the final commit must be frozen and submitted by Sunday 16:00 IST.*

## Goal

Build a program that cleans the shop logs in /data, links user IDs into people, builds sessions, credits each session to an ad click, counts assisting campaigns, and writes two Parquet tables to /output. The score is container run time (lower is better), and a wrong row scores nothing. Baseline is 172 s; target is under 60 s on about 60M events, and the hard cap is 180 s per final run.

## Rules for you, the executor

- rules.md is the only source of truth. If the starter or this plan disagrees with it, rules.md wins.
- Correctness first, speed second. Do not optimize until all 36 fixtures and the 1M dev check pass.
- Treat unzipped files as untrusted: unzip each archive into its own empty folder and run Python with `-I`.
- Keep tool output short (use `head`, `wc -l`, `tail -20`). Never print whole CSVs or long logs. This saves your usage limit.
- Hidden runs only report exit code, memory failure and counts of missing, extra and differing rows. Everything must be proven locally before you spend one of the 2 submissions per hour.
- Never put data in the repo. It must stay under 50 MB, be public, and have a Dockerfile at its root.

## What the user can expect from sessions

A Claude Pro usage window is about 5 hours, and how much work fits in one depends on the model and on how much output the work reads. My estimate for this plan is **2 sessions for a good result and 3 as a safe upper bound**. Your guess of 3 is reasonable, though not certain.

| Session | What gets done | Why it fits |
| --- | --- | --- |
| 1 | Phases 0-3: repo, SQL/Python pipeline, 36 fixtures and the 1M dev check passing, first submission | Most of the thinking is here; a correct DuckDB pipeline is a few hundred lines |
| 2 | Phase 4: scale tests, profiling, speedups, 2-4 submissions | Each loop is short if logs stay small |
| 3 (only if needed) | Phase 5: Rust fast path, or fixing a stubborn mismatch | Only worth it if DuckDB is not fast enough or a hidden-run diff persists |

What would push you to 4 or more sessions: dumping large outputs into the chat, rebuilding Docker from scratch on every try, or hunting a rule mismatch without a minimal repro.

## Phases

1. **Phase 0, setup (20 min).** Unzip starter-duckdb, generator and dev-answer into separate empty folders. Confirm `docker`, `make`, and `cargo` (for the generator) work. Run the starter once to see the layout (pipeline/pipeline.py, Dockerfile, run.sh, Makefile, fixtures/). Copy the starter into a new git repo. Note that /tmp is only 64 MB, so DuckDB's `temp_directory` must be `$SCRATCH_DIR`, and HOME is /scratch.
2. **Phase 1, parse and validate (1 hr).** Do not use DuckDB's CSV quoting logic: the dialect is custom. Read each file as raw lines (one VARCHAR column, a delimiter byte that never occurs, no quote handling), then split on commas yourself. Drop a line if the field count is wrong, if any quote is not around a whole field, or if the line is empty. Strip the wrapping quotes from a field and keep spaces and `\r`. Parse timestamps (F1, F2, F3) in SQL with strict regexes, then range-check against the manifest window. Pass the fixtures that exercise parsing first.
3. **Phase 2, events and links (1-2 hr).** Dedup events on `event_id` keeping the smallest `ingest_ts` before validating; then validate, then drop events ingested more than 24 h after `ts`. Build the link graph from non-empty, well-formed link lines and compute connected components with a union-find in Python or Rust (not a recursive CTE). The person name is the byte-wise smallest ID. Use binary comparison for all string ordering (`COLLATE` binary / `strcmp` semantics), because upper-case IDs sort before lower-case.
4. **Phase 3, sessions, attribution, outputs (1-2 hr).** Sort each person's events by `ts`; a new session starts only when the gap between distinct timestamps is over 1,800,000 ms. Session day is the UTC date of its start. Credit each session with an ASOF-style join to the latest click with `click.ts <= session_start` and within 7 days; ties go to the smallest `click_id`. Compute assists as distinct campaigns among the person's valid clicks in `[start - 7 d, start]`, excluding `organic`. Write both Parquet tables with `day` as a DATE. Run all 36 fixtures, then the 1M dev set and `make check-dev`. Submit once the dev check is clean.
5. **Phase 4, scale and speed (2-4 hr).** Generate a 20M and a 60M dataset with the generator (check disk space first), run both in Docker with `--cpus=8 --memory=24g`, and record time and peak memory. Profile each stage with `EXPLAIN ANALYZE` or per-stage timers on stderr. Typical wins: fewer passes over the CSVs, selecting only the needed columns, doing person mapping as an integer join, partitioned writes in parallel, and a slim base image.
6. **Phase 5, optional Rust fast path.** Only if DuckDB is above about 60 s at 60M events. Parse and dedup in Rust with rayon, write compact intermediates, and let DuckDB or Rust finish. Keep the DuckDB version as the reference and diff the two outputs on fixtures, 1M and 20M before switching.

## Traps in rules.md to test explicitly

| Area | Trap | Test |
| --- | --- | --- |
| CSV | `\r` is a normal character, so `1000\r` is a bad price and `...Z\r` a bad timestamp | A fixture with CRLF lines |
| CSV | Quotes only count around a whole field; `""` is an empty field; spaces are part of the value | `" u1"` equals `  u1 `, not `u1` |
| Digits | Only ASCII `0-9` are digits (`٢٥٠` is not) | Non-ASCII price value |
| Timestamps | F3 accepts leading zeros; bad forms include fractional seconds, lowercase `z`, `+0200`, `+24:00`, Feb 30, hour 24, second 60 | One row per bad form |
| Windows | Events need `start <= ts < end`; clicks need `start - 7 d <= ts < end` | Rows at exactly each boundary |
| Dedup order | Dedup on `event_id` comes before validation; a bad earliest copy kills the event | Duplicate with bad `ts` on the earliest copy |
| Late events | Exactly 24 h is kept; `ingest_ts` before `ts` is kept | Rows at 24 h and 24 h + 1 s |
| Price | Only purchases need a valid price (1 to 10,000,000, digits only, `0250` is 250) | Views with junk prices stay valid |
| Sessions | Gap strictly over 30:00.000; equal `ts` share a session | Gaps of 30:00.000 and 30:00.001 |
| Links | Transitive; self-links and repeats are harmless; name is the byte-wise smallest ID, even one absent from all events | Chain of 400 IDs; upper-case ID in the chain |
| Attribution | A click at the session start counts; 1 ms later does not; exactly 7 days counts | Clicks at each boundary and same-ms ties |
| Assists | Each campaign counts once per session; the credited campaign also assists; `organic` never does | Many clicks from one campaign |
| Category | Highest summed purchase price, ties to the smallest name, `unknown` competes, null with no purchases | Tie and no-purchase sessions |
| Output | `day` is a DATE; sessions crossing midnight count on the start day | Session spanning 23:59 to 00:01 |

Self-check from rules.md: summed `sessions` and `revenue_cents` must match across the two tables, and summed `assisted_sessions` must be at least the credited non-organic sessions.

## Container and submission checklist

- linux/amd64 image, at most 8 GB, runs as uid 1000 with a read-only root; only /tmp (64 MB), /scratch and /output are writable.
- Read paths from `DATA_DIR`, `OUTPUT_DIR`, `SCRATCH_DIR`, `NCPU`, `MEM_GB`; never hard-code them. Set DuckDB `threads` to NCPU and `memory_limit` to about 80% of MEM\_GB.
- Log to stderr only (stdout is capped at 50 MB). No network at run time, so install everything in the Dockerfile and pin versions.
- Output: only `*.parquet` files directly in /output/user\_daily/ and /output/campaign\_daily/, with exactly the columns in rules.md.
- Public GitHub repo, Dockerfile at the root, under 50 MB, no data checked in. Keep every submitted commit in history.

## Handoff to the user (what only they can do)

1. Create the public repo and push; the executor supplies the exact commands.
2. Submit the repo URL and commit on the portal. The limit is 2 submissions per hour, so submit only after the local checks pass.
3. Paste back the result (exit code, run time, counts of missing, extra and differing rows) so the executor can act on it.
4. By Sunday 16:00 IST, choose the best commit as the final entry. The portal rebuilds it after the cutoff and runs it 3 times on about 60M events, so leave real headroom under 180 s.

## Stop conditions

- Stop optimizing once a hidden run is under about 30 s and fixtures, 1M dev and 20M cross-checks all match. A smaller gain is not worth the risk of breaking correctness.
- If a hidden run shows differing rows, reproduce it at 1M or 20M with the generator (try several seeds) before changing code, and add a fixture for the cause.
- If memory runs out, lower the DuckDB `memory_limit`, make sure spills go to `$SCRATCH_DIR`, and reduce the number of wide intermediate tables.
