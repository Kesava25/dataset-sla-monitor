-- Run with: sqlite3 output/sla_monitor.db < docs/sample_queries.sql   (or python sqlite3)

-- SLA compliance by dataset (evaluated = not PENDING)
SELECT dataset_id,
       SUM(sla_status='MET') AS met, SUM(sla_status='BREACHED') AS breached,
       SUM(sla_status='PENDING') AS pending,
       ROUND(100.0*SUM(sla_status='MET')/NULLIF(SUM(sla_status!='PENDING'),0),1) AS compliance_pct
FROM partition_sla_status GROUP BY dataset_id ORDER BY compliance_pct, dataset_id LIMIT 5;

-- Open alerts by type and severity
SELECT alert_type, severity, COUNT(*) AS open_alerts FROM alerts WHERE status='OPEN'
GROUP BY alert_type, severity ORDER BY open_alerts DESC, alert_type LIMIT 8;

-- Datasets that are not fresh
SELECT dataset_id, last_completed_ts, freshness_minutes, max_staleness_minutes, freshness_status
FROM dataset_freshness WHERE freshness_status!='FRESH' ORDER BY freshness_minutes DESC;

-- Reject volume by reason
SELECT entity, reason_code, COUNT(*) AS n FROM rejects GROUP BY entity, reason_code ORDER BY n DESC, reason_code;
