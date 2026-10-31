# Design and trade-off note

## Shape of the solution
Producers emit **events** (`ARRIVED`/`COMPLETED`/`FAILED`). The pipeline keeps them in an append-only table and **derives** SLA state by a pure function `classify_partition(definition, partition, events, as_of)`. Nothing is "updated in place" except snapshot tables that are fully rebuilt, so a run is a deterministic function of its inputs and `as_of`.

## Key decisions and why
| Decision | Why | Cost / alternative |
|---|---|---|
| **SQLite + stdlib only** | Zero setup, transactional, same SQL runs on Postgres; the evaluator needs a "clean environment" run | Single writer, not distributed. At real scale: Postgres/warehouse + orchestrator |
| **Event log, derived status** | Tolerates out-of-order/late data, retries and completed-before-arrived without special cases; history is never lost | Evaluation reads events each time (cheap with the `(dataset, partition, ts)` index) |
| **Explicit `as_of`** | Reproducible backfill/replay; tests need no clock mocking; events after `as_of` are invisible | Caller must pass it (default = now) |
| **`event_id` = hash of natural key, *excluding* source file** | Idempotent across reruns *and* re-deliveries in other files | Two genuinely distinct events with identical key+second collapse (acceptable; documented) |
| **One transaction per file, skip by (name, sha256)** | Crash-safe, cheap reruns, a fixed file is picked up automatically, one bad file doesn't block the rest | A partially-bad *file* is not atomic at row level by design: bad rows are quarantined, good rows load |
| **Row-level reject vs file-level fail** | Bad rows are data problems (quarantine + continue); bad header/encoding is a producer/contract problem (fail the file, alert operators) | Threshold-based "too many rejects ⇒ fail file" is a possible next rule |
| **Naive timestamps rejected** | Guessing a zone silently creates or hides SLA breaches | Stricter than many producers; error message says why |
| **Completion SLA measured from expected arrival** | One clock for both deadlines, easy to explain to owners | Duration-based SLAs ("≤ 30 min after arrival") would need another field |
| **First completion decides SLA; latest gives row count** | A late reload can't hide a breach, but a corrected load fixes the row-count check | |
| **Freshness threshold = period + grace + completion SLA** | A dataset is "stale" only after a whole cycle was missed plus the allowed slack | Configurable per dataset would be a one-column addition |
| **Alert id = (type, dataset, partition); OPEN→RESOLVED→OPEN** | De-duplicated across runs, history retained, resolution is automatic | One alert per partition can be noisy in a long outage (roll-up is a next step) |
| **Completion alerts suppressed when arrival is MISSING** | One root cause, one alert | A `FAILED` event with no `ARRIVED` event is reported as MISSING_LOAD only |
| **Aged-out OPEN alerts stay OPEN** | Leaving the lookback window must not look like a fix | Needs manual/explicit closure policy later |
| **Answer key in a separate directory, generated from the scenario that produced the data** | Tests the evaluator against independent truth rather than against itself; a mutation test shows the check can fail | Generator and evaluator share only the partition-window helper |

## Data-quality strategy
Rules are explicit and ordered (see DATA_DICTIONARY), each rejected record keeps `file + line + raw record + reason`, and 11 reconciliation checks are computed **from the database**, not from in-memory counters: per-file balance, source→target counts, reject counts and traceability, natural-key uniqueness, FK integrity, status coherence, alert↔status agreement, freshness coverage.

## Observability
Per run: `run_id`, start/end, durations (ingest/evaluate), files seen/loaded/skipped/failed, rows read/loaded/already-loaded/rejected, partitions evaluated, alerts open/new/resolved, `failure_reason` — in `pipeline_runs`, `run_summary.json`, and structured log lines. Exit code: 0/2/1.

## Performance reasoning
Profile (136k rows): ingest 7.3 s, evaluate 5.6 s. Ingest is dominated by per-row Python validation (parse timestamps, hashing); SQL inserts are cheap *because* they are batched — the measured optimisation (26× over autocommit row inserts). Evaluation is O(partitions in window) with one indexed query per dataset. Next steps if volume grows 100×: incremental evaluation (only datasets with new events or deadlines crossing `as_of`), and moving storage to Postgres.

## Limitations (honest list)
No notification channel; hourly/daily periods only (no cron/calendars/time zones); no concurrent-run lock; current SLA definition applied to the entire window (history stored, not applied point-in-time); static row-count floor; file-order dependence for the "first file wins" source lineage of an event (does not affect results).
