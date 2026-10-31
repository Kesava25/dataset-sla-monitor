# Validation report

## Latest run

| field | value |
|---|---|
| run_id | run_20261030T082819_5edd12 |
| started_at | 2026-10-30T08:28:19Z |
| finished_at | 2026-10-30T08:28:33Z |
| status | SUCCESS |
| as_of_ts | 2026-01-15T07:40:00Z |
| files_seen | 91 |
| files_loaded | 91 |
| files_skipped | 0 |
| files_failed | 0 |
| rows_read | 136396 |
| rows_loaded | 132427 |
| rows_already_loaded | 359 |
| rows_rejected | 3610 |
| partitions_evaluated | 67500 |
| alerts_open | 7862 |
| duration_seconds | 13.952 |
| failure_reason | None |

## 1. Reconciliation checks (computed from the database)

| check | result | detail |
|---|---|---|
| R1 per-file: rows_read = loaded + already_loaded + rejected | PASS | 91 files checked, 0 violating |
| R2 source->target: SUM(file rows_loaded) = COUNT(load_events) | PASS | 132427 vs 132427 |
| R3 rejects: SUM(file rows_rejected) = COUNT(rejects[event]) | PASS | 3610 vs 3610 |
| R4 every reject traceable to file+line+raw record | PASS | 0 untraceable |
| R5 natural key unique in load_events | PASS | 0 duplicated keys |
| R6 referential integrity (FK check, orphan events) | PASS | fk violations=0, orphans=0 |
| R7 every event traceable to a LOADED source file | PASS | 0 untraceable |
| R8 sla_status coherent with arrival/completion status | PASS | 0 incoherent rows |
| R9 open alerts == partitions in the matching status (per alert type) | PASS | 6 alert types reconcile |
| R10 freshness row for every registered dataset | PASS | 60 of 60 |
| R11 STALE_DATASET alerts == non-fresh datasets | PASS | 1 vs 1 |

## 2. Ground-truth comparison (generator answer key vs pipeline output)

* Partitions with all 5 expected attributes matching: **67500 / 67500**
* Partitions in output that the answer key does not know: 0

| scenario | partitions | matched |
|---|---|---|
| FAILED | 1055 | 1055 |
| FUTURE_ARRIVAL | 306 | 306 |
| GRACE_ARRIVED | 2 | 2 |
| GRACE_DONE | 4 | 4 |
| GRACE_NONE | 2 | 2 |
| LATE_ARRIVAL | 2066 | 2066 |
| LATE_COMPLETION | 2036 | 2036 |
| LOW_ROW_COUNT | 681 | 681 |
| MISSING | 1044 | 1044 |
| NO_ARRIVAL_EVENT | 717 | 717 |
| ON_TIME | 57954 | 57954 |
| ON_TIME_EARLY | 16 | 16 |
| OVERDUE | 666 | 666 |
| PENDING_ARRIVED | 13 | 13 |
| PENDING_MISSING | 7 | 7 |
| RETRY_ON_TIME | 931 | 931 |

### Rejects: injected vs quarantined

* Expected 3613 injected bad records, quarantined 3613; exact (file, line, reason) match: **True**

### Row counts: expected vs actual (event files)

| metric | expected | actual | match |
|---|---|---|---|
| rows_read | 136396 | 136396 | yes |
| rows_loaded | 132427 | 132427 | yes |
| rows_already_loaded | 359 | 359 | yes |
| rows_rejected | 3610 | 3610 | yes |

## 3. Sample queries and results

**SLA compliance by dataset (evaluated = not PENDING)**

```sql
SELECT dataset_id,
       SUM(sla_status='MET') AS met, SUM(sla_status='BREACHED') AS breached,
       SUM(sla_status='PENDING') AS pending,
       ROUND(100.0*SUM(sla_status='MET')/NULLIF(SUM(sla_status!='PENDING'),0),1) AS compliance_pct
FROM partition_sla_status GROUP BY dataset_id ORDER BY compliance_pct, dataset_id LIMIT 5
```

| dataset_id | met | breached | pending | compliance_pct |
|---|---|---|---|---|
| ds_inventory_05 | 75 | 15 | 0 | 83.3 |
| ds_invoices_05 | 76 | 13 | 1 | 85.4 |
| ds_products_04 | 77 | 13 | 0 | 85.6 |
| ds_sessions_04 | 77 | 13 | 0 | 85.6 |
| ds_sessions_05 | 77 | 13 | 0 | 85.6 |

**Open alerts by type and severity**

```sql
SELECT alert_type, severity, COUNT(*) AS open_alerts FROM alerts WHERE status='OPEN'
GROUP BY alert_type, severity ORDER BY open_alerts DESC, alert_type LIMIT 8
```

| alert_type | severity | open_alerts |
|---|---|---|
| LATE_COMPLETION | medium | 608 |
| LATE_ARRIVAL | medium | 576 |
| LATE_ARRIVAL | high | 572 |
| LATE_COMPLETION | high | 532 |
| LATE_ARRIVAL | low | 515 |
| LATE_COMPLETION | low | 479 |
| LATE_COMPLETION | critical | 417 |
| LATE_ARRIVAL | critical | 403 |

**Datasets that are not fresh**

```sql
SELECT dataset_id, last_completed_ts, freshness_minutes, max_staleness_minutes, freshness_status
FROM dataset_freshness WHERE freshness_status!='FRESH' ORDER BY freshness_minutes DESC
```

| dataset_id | last_completed_ts | freshness_minutes | max_staleness_minutes | freshness_status |
|---|---|---|---|---|
| ds_inventory | 2026-01-15T05:25:28Z | 134.53 | 104 | STALE |

**Reject volume by reason**

```sql
SELECT entity, reason_code, COUNT(*) AS n FROM rejects GROUP BY entity, reason_code ORDER BY n DESC, reason_code
```

| entity | reason_code | n |
|---|---|---|
| event | DUPLICATE_EVENT | 722 |
| event | MISSING_REQUIRED_FIELD | 722 |
| event | INVALID_EVENT_TYPE | 361 |
| event | INVALID_PARTITION_KEY | 361 |
| event | INVALID_ROW_COUNT | 361 |
| event | INVALID_TIMESTAMP | 361 |
| event | MALFORMED_ROW | 361 |
| event | UNKNOWN_DATASET | 361 |
| registry | DUPLICATE_DATASET_ID | 1 |
| registry | INVALID_FREQUENCY | 1 |
| registry | INVALID_SLA_DEFINITION | 1 |

---
Overall: **PASS**
