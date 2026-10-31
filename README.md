# Dataset Arrival & Completion SLA Monitor

Tracks the **expected arrival time** and **completion SLA** of many datasets, detects **missing / late / failed / incomplete loads**,
computes **freshness**, and emits **actionable alerts** with a full audit trail. Pure Python standard library + SQLite:
nothing to install, runs from a clean checkout.

```bash
python3 -m unittest discover -s tests -t .          # 68 tests, ~1.5 s
python3 -m sla_monitor generate --out data/sample    # reproducible sample (seed 42) + separate answer key
python3 -m sla_monitor run --as-of 2026-01-15T07:40:00Z   # ingest -> validate -> evaluate -> reports  (exit 0)
bash scripts/demo.sh                                 # rerun, partial failure, retry, replay walkthrough
```
Requires Python ≥ 3.10 (developed on 3.12; SQLite ≥ 3.24 for `ON CONFLICT … DO UPDATE`). `make test|sample|run|demo|benchmark|large` are shortcuts.

---

## 1. Business problem → how it is modelled

| Business need | Model |
|---|---|
| "Dataset X should land by 02:00 and be ready by 03:30" | `dataset_registry`: per-dataset `frequency` (hourly/daily), `expected_offset_minutes`, `grace_minutes`, `completion_sla_minutes`, `min_rows`, `severity` |
| "Did it?" | Append-only `load_events` (`ARRIVED`, `COMPLETED`, `FAILED`) per `(dataset_id, partition_key)` |
| "Is each partition on time?" | `partition_sla_status`: `arrival_status`, `completion_status`, `sla_status` |
| "Is the dataset stale?" | `dataset_freshness` |
| "Who do I tell and what should they do?" | `alerts` (OPEN → RESOLVED lifecycle, `suggested_action` text, severity from the registry) |
| "Can I trust it?" | `pipeline_runs`, `file_ingest_log`, `rejects`, `validation_report.md` |

**Time rules** (all UTC): `expected_arrival = partition_start + offset`, `arrival_deadline = expected + grace`,
`completion_deadline = expected + completion_sla`. Boundaries are **inclusive** (arriving exactly at the deadline is on time).

| Status | Meaning |
|---|---|
| arrival_status | `ON_TIME` · `LATE` · `MISSING` (no arrival after deadline) · `PENDING` (deadline not yet passed) |
| completion_status | `ON_TIME` · `LATE` · `FAILED` (FAILED event, never completed) · `OVERDUE` · `PENDING` |
| sla_status | `MET` (both on time) · `BREACHED` (any late/missing/overdue) · `PENDING` |

Alert types: `MISSING_LOAD`, `LATE_ARRIVAL`, `LATE_COMPLETION`, `INCOMPLETE_LOAD`, `LOAD_FAILED`, `LOW_ROW_COUNT`, `STALE_DATASET`.
Completion alerts are suppressed while the arrival itself is missing (one root cause → one alert).

## 2. Architecture

```
 data/sample/                                   output/
 ├─ dataset_registry.csv ─┐                     ├─ partition_sla_status.csv   per-partition result
 ├─ landing/load_events_*.csv ─┐                ├─ sla_summary_by_dataset.csv compliance %
 │   (producer logs, shuffled,  │               ├─ dataset_freshness.csv
 │    with injected defects)    ▼               ├─ alerts.csv                 OPEN + RESOLVED history
 │            ┌───────────────────────────┐    ├─ rejects.csv                quarantine, file+line+raw
 │            │ 1 registry load           │    ├─ file_ingest_log.csv / run_history.csv
 │            │   validate, version history│   ├─ validation_report.md       reconciliation + answer-key check
 │            │ 2 per-file ingest (1 txn)  │    ├─ run_summary.json           metrics of the last run
 │            │   parse → validate → dedupe│    ├─ pipeline.log
 │            │   → INSERT OR IGNORE       │    └─ sla_monitor.db             SQLite (all tables)
 │            │   rejects → quarantine     │
 │            │ 3 evaluate(as_of)          │         load_events ──► partition_sla_status ──► alerts
 │            │   classify partitions      │         (append-only)     (snapshot)            (lifecycle)
 │            │   freshness, alert lifecycle│                        └► dataset_freshness
 │            │ 4 reports + reconciliation │
 │            └───────────────────────────┘
 └─ expected/  ← ANSWER KEY (labels, injected defects). Never read by the pipeline; only by `report`/tests.
```

| Module | Responsibility |
|---|---|
| `schedule.py` | `DatasetDef`, partition keys, deadlines, evaluation window (pure) |
| `validation.py` | Row-level rules → one deterministic reason code per bad record (pure) |
| `ingest.py` | CSV parsing, file-level failures, atomic per-file load, registry versioning |
| `evaluate.py` | `classify_partition` (pure SLA rules), alert rules, snapshot + alert lifecycle |
| `pipeline.py` | Run orchestration, run ledger, restart recovery, metrics/logging |
| `generator.py` | Deterministic data + answer key + fault files |
| `report.py` | CSV exports, 11 reconciliation checks, answer-key comparison, sample queries |
| `benchmark.py` | Large-volume timings + measured optimisation |

## 3. Running it

```bash
# Generate data (fixed seed → byte-identical output).  Small starter sample:
python3 -m sla_monitor generate --out data/sample                       # 8 datasets × 7 days, 700 partitions, 1,442 rows
# Large run (136k rows, 67.5k partitions):
python3 -m sla_monitor generate --out /tmp/large --datasets 60 --days 90
python3 -m sla_monitor run --data-dir /tmp/large --db /tmp/large/o/l.db --output-dir /tmp/large/o \
        --as-of 2026-01-15T07:40:00Z --lookback-hours 2160

# Operate
python3 -m sla_monitor run      --as-of <ISO-8601 with tz>     # default: now.   exit 0 SUCCESS / 2 PARTIAL_SUCCESS / 1 FAILED
python3 -m sla_monitor evaluate --as-of <ISO> [--lookback-hours N]   # re-evaluate stored events only (replay)
python3 -m sla_monitor report                                    # re-export CSVs + validation_report.md
python3 -m sla_monitor benchmark --datasets 60 --days 90
```
`--lookback-hours` (default 168) is the evaluation window: partitions whose expected arrival is in `(as_of − lookback, as_of]`.
Use `days × 24` when you want to evaluate the whole generated history. Paths are configurable (`--data-dir`, `--registry`, `--landing`, `--db`, `--output-dir`).

## 4. Naming and ID conventions
* Tables/columns `snake_case`; timestamps ISO-8601 UTC `YYYY-MM-DDTHH:MM:SSZ` (columns end in `_ts`/`_at`; inputs with other offsets are normalised, **naive timestamps are rejected**).
* `dataset_id` snake_case (`ds_orders`); `partition_key` `YYYY-MM-DD` (daily) / `YYYY-MM-DDTHH` (hourly).
* Deterministic IDs = prefix + first 16 hex of SHA-1 of the natural key: `evt_` (dataset, partition, type, event_ts), `alr_` (type, dataset, partition), `rej_` (file, file sha, line, reason).
* `run_id = run_<UTC start>_<6 hex>` (suffix `_2`, `_3`… if two runs start in the same second). Every event/reject carries `run_id`, `source_file`, `source_line`.

## 5. Idempotency, retry, restart, backfill
| Situation | Behaviour |
|---|---|
| Same input re-run | File `(name, sha256)` already `LOADED` → skipped. Even if re-delivered, `event_id` is content-derived and inserts are `INSERT OR IGNORE`. Snapshot tables are a pure function of `(events, registry, as_of, lookback)`. Tested: full-table snapshots identical before/after a rerun. |
| Same event in a *different* file (replay) | Counted `rows_already_loaded` – not an error, not a duplicate. |
| Crash/exception mid-file | One transaction per file → rolled back, nothing partial. File logged `FAILED`; rest of the run continues (`PARTIAL_SUCCESS`). Next run retries it. |
| Process killed | Run left `RUNNING` → next run marks it `ABANDONED`. |
| Producer fixes a bad file | New sha256 → processed; unchanged files stay skipped. |
| Backfill / replay | `evaluate --as-of T` (or `run --as-of T`) recomputes from stored events; events with `event_ts > as_of` are invisible, so the past can be re-derived exactly. |
| Alerts | Upserted by deterministic id; a re-detected alert keeps `first_detected_as_of`; a condition that clears becomes `RESOLVED` (history kept); alerts for partitions that *aged out* of the window are left OPEN rather than silently resolved. |
| SLA definition change | Registry change appends a row to `dataset_registry_history` (versioned); current definition applies on next evaluation. |

## 6. Required edge cases → where handled and tested
| Edge case | Handling | Test |
|---|---|---|
| Malformed input | Wrong field count → `MALFORMED_ROW`; non-UTF8/NUL file → file `FAILED` | `test_malformed_missing_invalid_records_…`, `test_zero_byte_and_non_utf8_files_fail_cleanly` |
| Missing required field / nulls | `MISSING_REQUIRED_FIELD` (incl. `row_count` for `COMPLETED`) | `test_missing_required_each_column`, `…_quarantined_and_traceable` |
| Duplicate key/event | In-file → `DUPLICATE_EVENT` (first wins, conflicting `row_count` called out); cross-file → already-loaded | `test_duplicates_within_file_…`, `test_event_replayed_in_another_file_…` |
| Unexpected schema change | Missing required column → whole file `FAILED/SCHEMA_MISMATCH`; extra column → warning, ignored | `test_schema_change_fails_that_file_only_…`, `test_unexpected_extra_column_…` |
| Empty batch | Header-only = valid, 0 rows, warning; no files = SUCCESS (absence is evaluated → MISSING alerts) | `test_empty_batch_header_only`, `test_empty_directory_…` |
| Partial failure & retry | Per-file atomicity, fault-injection hook, retry on next run | `test_crash_mid_file_rolls_back_then_retry_loads_everything`, `test_failed_file_is_retried_…` |
| Late / out-of-order data | Evaluation is order-independent (sorts by event time); late arrival → `LATE`; future events hidden until `as_of` | `test_out_of_order_events_give_same_result`, `test_future_events_are_invisible_…`, `test_input_file_order_…` |
| Invalid reference key | `UNKNOWN_DATASET`; invalid `partition_key` for the dataset's frequency | `test_event_type_unknown_dataset_timestamp_partition` |
| Rerun of same input | See §5 | `test_rerun_same_input_changes_nothing`, `test_rerun_after_full_load_is_a_noop` |
| **Domain:** completion w/o arrival event | Completion implies arrival (`arrival_inferred=1`) | `test_completion_without_arrival_event_implies_arrival` |
| **Domain:** FAILED then retry succeeds | Completion wins; FAILED alone = `FAILED` (breach only after deadline) | `test_failed_then_retry_completed_…` |
| **Domain:** deadline boundaries, midnight rollover | Inclusive deadlines; hourly partition completing after midnight | `test_arrival_boundary_…`, `test_hourly_partition_across_midnight`, `test_window_month_and_year_rollover` |
| **Domain:** low row count, SLA changes, stale/never-loaded | `LOW_ROW_COUNT` (SLA still MET); versioned registry; freshness states | `test_low_row_count_*`, `RegistryHistoryTests`, `test_freshness_*` |

## 7. Evidence (all reproducible; see `evidence/`)
* `evidence/test_results.txt` – 68 tests, all passing. Includes an end-to-end test that compares pipeline output to the generator's answer key and a **mutation test** proving that comparison fails if an SLA rule is broken.
* `output/validation_report.md` – 11 reconciliation checks (all PASS), answer-key comparison (**700/700** partitions, **83/83** injected bad records (80 events + 3 registry rows) quarantined at the exact file+line+reason), sample queries with results.
* `evidence/large_run_validation_report.md` – same at scale: **67,500/67,500** partitions correct, **3,613/3,613** injected defects exact, all checks PASS.
* `evidence/demo_transcript.txt` – clean run, no-op rerun, partial failure (exit 2), retry, replay.

### Performance (`evidence/benchmark.json`, one laptop-class CPU, single process)
| 136,396 input rows · 67,500 partitions · 91 files | |
|---|---|
| Ingest (parse + validate + dedupe + load) | 7.3 s ≈ 18,700 rows/s |
| Evaluate all 67,500 partitions + alerts | 5.6 s ≈ 12,000 partitions/s |
| Whole pipeline | ≈ 13 s · DB 63 MB · peak RSS 95 MB |
| Rerun of identical input | all 91 files skipped, 0 rows loaded |

**Optimisation implemented and measured:** inserts are batched (`executemany`, 5,000/chunk) inside one transaction per file instead of autocommit row-at-a-time:
**123k vs 4.7k rows/s (26×)** in the micro-benchmark. Next obvious optimisation (not done): make evaluation incremental per dataset/new-events instead of rebuilding the window.

## 8. Assumptions
* Datasets are periodic (hourly or daily, UTC). One expected partition per period. No holidays/calendars/cron expressions.
* Producers emit `ARRIVED` / `COMPLETED` / `FAILED` events (e.g. from a landing-zone watcher / orchestrator hooks); `COMPLETED` carries `row_count`.
* The **first** `COMPLETED` decides SLA compliance; the **latest** `COMPLETED` provides the row count (a corrected reload fixes row count but cannot undo a breach).
* Freshness threshold = one period + grace + completion SLA (documented in DESIGN.md).
* Source files are named so that lexical order = processing order (`load_events_YYYYMMDD.csv`).
* Synthetic data only; nothing proprietary, no secrets, no network or credentials needed.

## 9. Known limitations / next steps
* Alerts are rows/CSV only; no Slack/e-mail delivery (next: a notifier reading OPEN alerts, de-duplicated by `alert_id`).
* Single writer: two overlapping runs would contend on SQLite's lock (next: lock file / orchestrator concurrency=1).
* Evaluation rebuilds the window each run (fine to ~10⁵ partitions); incremental evaluation is the next scale step.
* SLA definition history is stored but the *current* definition is applied to the whole window (no point-in-time evaluation).
* One alert per partition can be noisy during a long outage; roll-up per dataset is a sensible next step.
* Static `min_rows`; a statistical row-count anomaly check (vs trailing median) would be better.
* See `docs/DESIGN.md` for trade-offs and `docs/DATA_DICTIONARY.md` for schemas.

## 10. Layout
```
sla_monitor/   package (see §2)        tests/   68 unittest tests        scripts/demo.sh   walkthrough
data/sample/   generated sample        output/  generated sample outputs  evidence/        test/benchmark/demo evidence
docs/          DATA_DICTIONARY.md · DESIGN.md · sample_queries.sql
```
