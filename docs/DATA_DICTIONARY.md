# Data dictionary

Conventions: `snake_case`; timestamps ISO-8601 UTC `YYYY-MM-DDTHH:MM:SSZ` (stored as TEXT; fixed width so string order = time order). PK = primary key, FK = foreign key.

## 1. Source files (inputs)

### 1.1 `dataset_registry.csv` — one row per dataset (grain: `dataset_id`)
| column | type | null? | rule |
|---|---|---|---|
| dataset_id | text | no | PK, unique in file, snake_case `^[a-z][a-z0-9_]*$` |
| dataset_name | text | no | |
| owner | text | no | team to notify |
| frequency | text | no | `hourly` \| `daily` (case-insensitive) |
| expected_offset_minutes | int ≥ 0 | no | minutes after partition start the data is expected. hourly 0–59, daily 0–1439 |
| grace_minutes | int ≥ 0 | no | arrival allowed this long after expected |
| completion_sla_minutes | int ≥ 0 | no | completion allowed this long after expected; must be ≥ grace_minutes |
| min_rows | int ≥ 0 | no | `COMPLETED` below this ⇒ `LOW_ROW_COUNT` (0 disables) |
| severity | text | no | `low` \| `medium` \| `high` \| `critical` (copied to alerts) |

Registry reject codes: `MISSING_REQUIRED_FIELD`, `INVALID_DATASET_ID`, `INVALID_FREQUENCY`, `INVALID_SEVERITY`, `INVALID_NUMBER`, `INVALID_SLA_DEFINITION`, `DUPLICATE_DATASET_ID`, `MALFORMED_ROW`.

### 1.2 `landing/load_events_*.csv` — one row per load event (grain: dataset, partition, event type, event time)
| column | type | null? | rule |
|---|---|---|---|
| dataset_id | text | no | FK → registry (`UNKNOWN_DATASET` otherwise) |
| partition_key | text | no | `YYYY-MM-DD` for daily, `YYYY-MM-DDTHH` for hourly; must be a real date/hour |
| event_type | text | no | `ARRIVED` \| `COMPLETED` \| `FAILED` (case-insensitive) |
| event_ts | text | no | ISO-8601 **with** timezone; normalised to UTC, sub-seconds truncated |
| row_count | int ≥ 0 | only `COMPLETED` requires it | digits only; `0` is valid |

Extra columns are ignored with a warning; any missing required column fails the whole file (`SCHEMA_MISMATCH`). Header names are trimmed/lower-cased; UTF-8 BOM and blank lines tolerated.

**Event validation order (fixed, so every bad row gets exactly one reason):** field count (`MALFORMED_ROW`) → required fields (`MISSING_REQUIRED_FIELD`) → `INVALID_EVENT_TYPE` → `UNKNOWN_DATASET` → `INVALID_TIMESTAMP` → `INVALID_PARTITION_KEY` → `INVALID_ROW_COUNT` → duplicate in file (`DUPLICATE_EVENT`).
File-level codes: `EMPTY_FILE`, `NOT_UTF8_TEXT`, `MALFORMED_FILE`, `SCHEMA_MISMATCH`, `UNEXPECTED_ERROR`.

## 2. Target tables (SQLite)

### `load_events` — append-only fact. Grain = natural key (dataset_id, partition_key, event_type, event_ts)
| column | type | null | notes |
|---|---|---|---|
| event_id | text | no | **PK**, `evt_`+sha1(natural key)[:16]; makes loads idempotent |
| dataset_id | text | no | FK → dataset_registry |
| partition_key / event_type / event_ts | text | no | as above (CHECK on event_type) |
| row_count | int | yes | CHECK ≥ 0 |
| source_file, source_line | text, int | no | lineage to the first file/line that delivered it |
| run_id | text | no | run that loaded it |

Indexes: `(dataset_id, partition_key, event_ts)`, `(dataset_id, event_type, event_ts)`.

### `dataset_registry` / `dataset_registry_history`
Current definition (PK `dataset_id`) plus `definition_hash`, `updated_run_id`. History: PK `(dataset_id, version)`, one row per definition change with `valid_from_run_id`, `valid_from_ts`. Datasets are never deleted.

### `rejects` — quarantine. PK `reject_id` = `rej_`+sha1(file, file sha, line, reason)[:16]
`entity` (`event`\|`registry`\|`file`), `source_file`, `source_line` (0 for file-level), `reason_code`, `reason_detail`, `raw_record` (JSON array of the original fields), `run_id`.

### `file_ingest_log` — PK `(source_file, file_sha256)`
`status` `LOADED`\|`FAILED`, `rows_read`, `rows_loaded`, `rows_already_loaded`, `rows_rejected`, `warnings`, `error`, `run_id`, `processed_at`.
Invariant: `rows_read = rows_loaded + rows_already_loaded + rows_rejected` (reconciliation check R1). Blank lines are not counted as rows.

### `pipeline_runs` — PK `run_id`
`started_at`, `finished_at`, `status` (`RUNNING`\|`SUCCESS`\|`PARTIAL_SUCCESS`\|`FAILED`\|`ABANDONED`), `as_of_ts`, file counts (`seen/loaded/skipped/failed`), row counts (`read/loaded/already_loaded/rejected`), `partitions_evaluated`, `alerts_open`, `duration_seconds`, `failure_reason`.

### `partition_sla_status` — snapshot (rebuilt on every evaluation). PK `(dataset_id, partition_key)`
| column | notes (derived fields marked *) |
|---|---|
| expected_arrival_ts*, arrival_deadline_ts*, completion_deadline_ts* | from registry + partition |
| arrived_ts* | earliest `ARRIVED` (or first `COMPLETED` if inferred) |
| completed_ts* | earliest `COMPLETED` |
| arrival_status*, completion_status*, sla_status* | see README §1 |
| arrival_delay_minutes*, completion_latency_minutes* | signed minutes vs expected arrival |
| row_count* | rows of the latest `COMPLETED` |
| low_row_count*, arrival_inferred*, timestamp_order_anomaly* | 0/1 flags |
| evaluated_as_of, run_id | provenance |

### `dataset_freshness` — snapshot. PK `dataset_id`
`last_completed_ts` (latest `COMPLETED` ≤ as_of), `freshness_minutes`, `max_staleness_minutes` (= period + grace + completion SLA), `freshness_status` `FRESH`\|`STALE`\|`NEVER_LOADED`.

### `alerts` — history. PK `alert_id` = `alr_`+sha1(type, dataset, partition)[:16]
`alert_type`, `severity`, `dataset_id`, `partition_key` (`''` for dataset-level), `status` `OPEN`\|`RESOLVED`, `first_detected_as_of`, `last_evaluated_as_of`, `resolved_as_of`, `message`, `suggested_action`, `detected_run_id`, `last_run_id`.

## 3. Referential integrity and uniqueness
* `load_events.dataset_id`, `partition_sla_status.dataset_id`, `dataset_freshness.dataset_id`, `alerts.dataset_id` → `dataset_registry` (SQLite `foreign_keys=ON`, checked again by reconciliation R6).
* Uniqueness: `event_id` (PK) ⇒ natural key unique (R5); one status row per partition; one alert row per (type, dataset, partition).

## 4. Generator answer key (`data/sample/expected/`, never read by the pipeline)
`expected_labels.csv` (per-partition expected statuses + scenario name), `expected_rejects.csv` (every injected bad record: entity, file, line, reason), `expected_summary.json` (expected row counts per file and total).
