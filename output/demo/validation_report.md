# Validation report

## Latest run

| field | value |
|---|---|
| run_id | run_20261031T105943_a1d721 |
| started_at | 2026-10-31T10:59:43Z |
| finished_at | 2026-10-31T10:59:43Z |
| status | SUCCESS |
| as_of_ts | 2026-01-15T07:40:00Z |
| files_seen | 8 |
| files_loaded | 0 |
| files_skipped | 8 |
| files_failed | 0 |
| rows_read | 0 |
| rows_loaded | 0 |
| rows_already_loaded | 0 |
| rows_rejected | 0 |
| partitions_evaluated | 700 |
| alerts_open | 110 |
| duration_seconds | 0.328 |
| failure_reason | None |

## 1. Reconciliation checks (computed from the database)

| check | result | detail |
|---|---|---|
| R1 per-file: rows_read = loaded + already_loaded + rejected | PASS | 8 files checked, 0 violating |
| R2 source->target: SUM(file rows_loaded) = COUNT(load_events) | PASS | 1355 vs 1355 |
| R3 rejects: SUM(file rows_rejected) = COUNT(rejects[event]) | PASS | 80 vs 80 |
| R4 every reject traceable to file+line+raw record | PASS | 0 untraceable |
| R5 natural key unique in load_events | PASS | 0 duplicated keys |
| R6 referential integrity (FK check, orphan events) | PASS | fk violations=0, orphans=0 |
| R7 every event traceable to a LOADED source file | PASS | 0 untraceable |
| R8 sla_status coherent with arrival/completion status | PASS | 0 incoherent rows |
| R9 open alerts == partitions in the matching status (per alert type) | PASS | 6 alert types reconcile |
| R10 freshness row for every registered dataset | PASS | 8 of 8 |
| R11 STALE_DATASET alerts == non-fresh datasets | PASS | 0 vs 0 |

## 2. Ground-truth comparison (generator answer key vs pipeline output)

* Partitions with all 5 expected attributes matching: **700 / 700**
* Partitions in output that the answer key does not know: 0

| scenario | partitions | matched |
|---|---|---|
| FAILED | 12 | 12 |
| FUTURE_ARRIVAL | 6 | 6 |
| GRACE_NONE | 1 | 1 |
| LATE_ARRIVAL | 34 | 34 |
| LATE_COMPLETION | 23 | 23 |
| LOW_ROW_COUNT | 11 | 11 |
| MISSING | 16 | 16 |
| NO_ARRIVAL_EVENT | 10 | 10 |
| ON_TIME | 567 | 567 |
| ON_TIME_EARLY | 1 | 1 |
| OVERDUE | 6 | 6 |
| PENDING_ARRIVED | 1 | 1 |
| PENDING_MISSING | 2 | 2 |
| RETRY_ON_TIME | 10 | 10 |

### Rejects: injected vs quarantined

* Expected 83 injected bad records, quarantined 83; exact (file, line, reason) match: **True**

### Row counts: expected vs actual (event files)

| metric | expected | actual | match |
|---|---|---|---|
| rows_read | 1442 | 1442 | yes |
| rows_loaded | 1355 | 1355 | yes |
| rows_already_loaded | 7 | 7 | yes |
| rows_rejected | 80 | 80 | yes |

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
| ds_orders | 134 | 34 | 0 | 79.8 |
| ds_payments | 144 | 24 | 0 | 85.7 |
| ds_products | 6 | 1 | 0 | 85.7 |
| ds_returns | 6 | 1 | 0 | 85.7 |
| ds_shipments | 6 | 1 | 0 | 85.7 |

**Open alerts by type and severity**

```sql
SELECT alert_type, severity, COUNT(*) AS open_alerts FROM alerts WHERE status='OPEN'
GROUP BY alert_type, severity ORDER BY open_alerts DESC, alert_type LIMIT 8
```

| alert_type | severity | open_alerts |
|---|---|---|
| LATE_ARRIVAL | high | 14 |
| LATE_ARRIVAL | medium | 14 |
| MISSING_LOAD | high | 11 |
| LATE_COMPLETION | critical | 9 |
| MISSING_LOAD | medium | 9 |
| LATE_COMPLETION | medium | 8 |
| LATE_ARRIVAL | critical | 6 |
| LATE_COMPLETION | high | 6 |

**Datasets that are not fresh**

```sql
SELECT dataset_id, last_completed_ts, freshness_minutes, max_staleness_minutes, freshness_status
FROM dataset_freshness WHERE freshness_status!='FRESH' ORDER BY freshness_minutes DESC
```

| dataset_id | last_completed_ts | freshness_minutes | max_staleness_minutes | freshness_status |
|---|---|---|---|---|

**Reject volume by reason**

```sql
SELECT entity, reason_code, COUNT(*) AS n FROM rejects GROUP BY entity, reason_code ORDER BY n DESC, reason_code
```

| entity | reason_code | n |
|---|---|---|
| event | DUPLICATE_EVENT | 16 |
| event | MISSING_REQUIRED_FIELD | 16 |
| event | INVALID_EVENT_TYPE | 8 |
| event | INVALID_PARTITION_KEY | 8 |
| event | INVALID_ROW_COUNT | 8 |
| event | INVALID_TIMESTAMP | 8 |
| event | MALFORMED_ROW | 8 |
| event | UNKNOWN_DATASET | 8 |
| registry | DUPLICATE_DATASET_ID | 1 |
| registry | INVALID_FREQUENCY | 1 |
| registry | INVALID_SLA_DEFINITION | 1 |

---
Overall: **PASS**
