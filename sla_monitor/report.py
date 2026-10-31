"""Outputs: CSV exports, reconciliation checks, ground-truth comparison, validation_report.md."""
from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path

from .store import Store

SAMPLE_QUERIES = {
    "SLA compliance by dataset (evaluated = not PENDING)": """
SELECT dataset_id,
       SUM(sla_status='MET') AS met, SUM(sla_status='BREACHED') AS breached,
       SUM(sla_status='PENDING') AS pending,
       ROUND(100.0*SUM(sla_status='MET')/NULLIF(SUM(sla_status!='PENDING'),0),1) AS compliance_pct
FROM partition_sla_status GROUP BY dataset_id ORDER BY compliance_pct, dataset_id LIMIT 5""",
    "Open alerts by type and severity": """
SELECT alert_type, severity, COUNT(*) AS open_alerts FROM alerts WHERE status='OPEN'
GROUP BY alert_type, severity ORDER BY open_alerts DESC, alert_type LIMIT 8""",
    "Datasets that are not fresh": """
SELECT dataset_id, last_completed_ts, freshness_minutes, max_staleness_minutes, freshness_status
FROM dataset_freshness WHERE freshness_status!='FRESH' ORDER BY freshness_minutes DESC""",
    "Reject volume by reason": """
SELECT entity, reason_code, COUNT(*) AS n FROM rejects GROUP BY entity, reason_code ORDER BY n DESC, reason_code""",
}


@dataclass
class Check:
    name: str
    passed: bool
    detail: str


def export_csvs(store: Store, out_dir: str | Path) -> list[str]:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    exports = {
        "partition_sla_status.csv": "SELECT * FROM partition_sla_status ORDER BY dataset_id, partition_key",
        "dataset_freshness.csv": "SELECT * FROM dataset_freshness ORDER BY dataset_id",
        "alerts.csv": "SELECT * FROM alerts ORDER BY status DESC, CASE severity WHEN 'critical' THEN 0 WHEN 'high' THEN 1"
                      " WHEN 'medium' THEN 2 ELSE 3 END, dataset_id, partition_key, alert_type",
        "rejects.csv": "SELECT * FROM rejects ORDER BY source_file, source_line, reason_code",
        "file_ingest_log.csv": "SELECT * FROM file_ingest_log ORDER BY source_file",
        "run_history.csv": "SELECT * FROM pipeline_runs ORDER BY started_at, run_id",
        "sla_summary_by_dataset.csv": """
            SELECT p.dataset_id, r.frequency, r.severity, COUNT(*) AS partitions,
                   SUM(sla_status='MET') AS met, SUM(sla_status='BREACHED') AS breached, SUM(sla_status='PENDING') AS pending,
                   ROUND(100.0*SUM(sla_status='MET')/NULLIF(SUM(sla_status!='PENDING'),0),2) AS compliance_pct,
                   ROUND(AVG(arrival_delay_minutes),2) AS avg_arrival_delay_min,
                   SUM(arrival_status='MISSING') AS missing, SUM(arrival_status='LATE') AS late_arrivals,
                   SUM(completion_status='LATE') AS late_completions, SUM(low_row_count) AS low_row_count
            FROM partition_sla_status p JOIN dataset_registry r USING(dataset_id) GROUP BY p.dataset_id ORDER BY p.dataset_id""",
    }
    written = []
    for fname, sql in exports.items():
        cur = store.conn.execute(sql)
        cols = [c[0] for c in cur.description]
        with (out / fname).open("w", newline="") as f:
            w = csv.writer(f, lineterminator="\n")
            w.writerow(cols)
            w.writerows(cur.fetchall())
        written.append(fname)
    return written


# ---------------------------------------------------------------------------------------------
def reconcile(store: Store) -> list[Check]:
    q = store.scalar
    checks: list[Check] = []

    bad = q("SELECT COUNT(*) FROM file_ingest_log WHERE status='LOADED' AND rows_read != rows_loaded+rows_already_loaded+rows_rejected")
    n = q("SELECT COUNT(*) FROM file_ingest_log WHERE status='LOADED'")
    checks.append(Check("R1 per-file: rows_read = loaded + already_loaded + rejected", bad == 0, f"{n} files checked, {bad} violating"))

    loaded = q("SELECT COALESCE(SUM(rows_loaded),0) FROM file_ingest_log WHERE status='LOADED'")
    target = q("SELECT COUNT(*) FROM load_events")
    checks.append(Check("R2 source->target: SUM(file rows_loaded) = COUNT(load_events)", loaded == target, f"{loaded} vs {target}"))

    rej_log = q("SELECT COALESCE(SUM(rows_rejected),0) FROM file_ingest_log WHERE status='LOADED'")
    rej_tbl = q("SELECT COUNT(*) FROM rejects WHERE entity='event'")
    checks.append(Check("R3 rejects: SUM(file rows_rejected) = COUNT(rejects[event])", rej_log == rej_tbl, f"{rej_log} vs {rej_tbl}"))

    untraceable = q("SELECT COUNT(*) FROM rejects WHERE source_file IS NULL OR source_file='' OR "
                    "(entity!='file' AND (source_line<=0 OR raw_record IS NULL))")
    checks.append(Check("R4 every reject traceable to file+line+raw record", untraceable == 0, f"{untraceable} untraceable"))

    dups = q("SELECT COUNT(*) FROM (SELECT 1 FROM load_events GROUP BY dataset_id,partition_key,event_type,event_ts HAVING COUNT(*)>1)")
    checks.append(Check("R5 natural key unique in load_events", dups == 0, f"{dups} duplicated keys"))

    fk = len(store.conn.execute("PRAGMA foreign_key_check").fetchall())
    orphan = q("SELECT COUNT(*) FROM load_events e LEFT JOIN dataset_registry r USING(dataset_id) WHERE r.dataset_id IS NULL")
    checks.append(Check("R6 referential integrity (FK check, orphan events)", fk == 0 and orphan == 0, f"fk violations={fk}, orphans={orphan}"))

    nofile = q("SELECT COUNT(*) FROM load_events e LEFT JOIN file_ingest_log f ON f.source_file=e.source_file AND f.status='LOADED' "
               "WHERE f.source_file IS NULL")
    checks.append(Check("R7 every event traceable to a LOADED source file", nofile == 0, f"{nofile} untraceable"))

    incoh = q("""SELECT COUNT(*) FROM partition_sla_status WHERE
        (sla_status='MET' AND NOT (arrival_status='ON_TIME' AND completion_status='ON_TIME')) OR
        (sla_status='BREACHED' AND arrival_status NOT IN ('LATE','MISSING') AND completion_status NOT IN ('LATE','OVERDUE','FAILED')) OR
        (sla_status='PENDING' AND (arrival_status IN ('LATE','MISSING') OR completion_status IN ('LATE','OVERDUE')))""")
    checks.append(Check("R8 sla_status coherent with arrival/completion status", incoh == 0, f"{incoh} incoherent rows"))

    rules = {
        "MISSING_LOAD": "p.arrival_status='MISSING'",
        "LATE_ARRIVAL": "p.arrival_status='LATE'",
        "LATE_COMPLETION": "p.arrival_status IN ('ON_TIME','LATE') AND p.completion_status='LATE'",
        "INCOMPLETE_LOAD": "p.arrival_status IN ('ON_TIME','LATE') AND p.completion_status='OVERDUE'",
        "LOAD_FAILED": "p.arrival_status IN ('ON_TIME','LATE') AND p.completion_status='FAILED'",
        "LOW_ROW_COUNT": "p.low_row_count=1",
    }
    diffs = []
    for atype, cond in rules.items():
        want = q(f"SELECT COUNT(*) FROM partition_sla_status p WHERE {cond}")
        have = q("SELECT COUNT(*) FROM alerts a JOIN partition_sla_status p USING(dataset_id,partition_key) "
                 "WHERE a.status='OPEN' AND a.alert_type=?", (atype,))
        if want != have:
            diffs.append(f"{atype}: status rows={want}, open alerts={have}")
    checks.append(Check("R9 open alerts == partitions in the matching status (per alert type)", not diffs,
                        "; ".join(diffs) or f"{len(rules)} alert types reconcile"))

    nd, nf = q("SELECT COUNT(*) FROM dataset_registry"), q("SELECT COUNT(*) FROM dataset_freshness")
    checks.append(Check("R10 freshness row for every registered dataset", nd == nf, f"{nf} of {nd}"))

    stale_open = q("SELECT COUNT(*) FROM alerts WHERE alert_type='STALE_DATASET' AND status='OPEN'")
    stale = q("SELECT COUNT(*) FROM dataset_freshness WHERE freshness_status!='FRESH'")
    checks.append(Check("R11 STALE_DATASET alerts == non-fresh datasets", stale_open == stale, f"{stale_open} vs {stale}"))
    return checks


# ---------------------------------------------------------------------------------------------
def compare_labels(store: Store, expected_dir: str | Path) -> dict:
    """Compare evaluator output with the generator's answer key (kept in a separate directory)."""
    exp = Path(expected_dir)
    out: dict = {"available": False}
    lab_path = exp / "expected_labels.csv"
    if not lab_path.exists():
        return out
    out["available"] = True
    actual = {(r["dataset_id"], r["partition_key"]): r for r in store.conn.execute("SELECT * FROM partition_sla_status")}
    total = matched = 0
    mism: list[dict] = []
    seen = set()
    by_sc: dict[str, list[int]] = {}
    with lab_path.open() as f:
        for row in csv.DictReader(f):
            total += 1
            key = (row["dataset_id"], row["partition_key"])
            seen.add(key)
            a = actual.get(key)
            want = (row["expected_arrival_status"], row["expected_completion_status"], row["expected_sla_status"],
                    int(row["expected_low_row_count"]), int(row["expected_arrival_inferred"]))
            got = None if a is None else (a["arrival_status"], a["completion_status"], a["sla_status"],
                                          a["low_row_count"], a["arrival_inferred"])
            ok = want == got
            by_sc.setdefault(row["scenario"], [0, 0])
            by_sc[row["scenario"]][0] += 1
            if ok:
                matched += 1
                by_sc[row["scenario"]][1] += 1
            elif len(mism) < 20:
                mism.append({"partition": key, "scenario": row["scenario"], "expected": want, "actual": got})
    out.update(total=total, matched=matched, unexpected_partitions=len(set(actual) - seen),
               mismatches=mism, by_scenario={k: tuple(v) for k, v in sorted(by_sc.items())})

    rej_path = exp / "expected_rejects.csv"
    if rej_path.exists():
        with rej_path.open() as f:
            want_r = {(r["entity"], r["source_file"], int(r["source_line"]), r["reason_code"]) for r in csv.DictReader(f)}
        got_r = {(r[0], r[1], r[2], r[3]) for r in store.conn.execute(
            "SELECT entity,source_file,source_line,reason_code FROM rejects WHERE entity IN ('event','registry')")}
        out["rejects"] = {"expected": len(want_r), "actual": len(got_r), "missing": sorted(want_r - got_r)[:10],
                          "unexpected": sorted(got_r - want_r)[:10], "exact_match": want_r == got_r}
    sum_path = exp / "expected_summary.json"
    if sum_path.exists():
        tot = json.loads(sum_path.read_text())["totals"]
        row = store.conn.execute("SELECT COALESCE(SUM(rows_read),0) rows_read, COALESCE(SUM(rows_loaded),0) rows_loaded,"
                                 " COALESCE(SUM(rows_already_loaded),0) rows_already_loaded,"
                                 " COALESCE(SUM(rows_rejected),0) rows_rejected FROM file_ingest_log"
                                 " WHERE status='LOADED' AND source_file LIKE 'load_events_%'").fetchone()
        out["counts"] = {k: (tot[k], row[k]) for k in tot}
    return out


def _md_table(headers, rows) -> str:
    lines = ["| " + " | ".join(headers) + " |", "|" + "|".join("---" for _ in headers) + "|"]
    lines += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return "\n".join(lines)


def build_validation_report(store: Store, expected_dir: str | Path | None, as_of: str | None = None) -> tuple[str, bool]:
    checks = reconcile(store)
    cmp = compare_labels(store, expected_dir) if expected_dir else {"available": False}
    ok = all(c.passed for c in checks)
    md = ["# Validation report", ""]
    run = store.conn.execute("SELECT * FROM pipeline_runs ORDER BY started_at DESC, rowid DESC LIMIT 1").fetchone()
    if run:
        md += ["## Latest run", "", _md_table(["field", "value"], [(k, run[k]) for k in run.keys()]), ""]
    md += ["## 1. Reconciliation checks (computed from the database)", "",
           _md_table(["check", "result", "detail"], [(c.name, "PASS" if c.passed else "**FAIL**", c.detail) for c in checks]), ""]
    if cmp["available"]:
        md += ["## 2. Ground-truth comparison (generator answer key vs pipeline output)", "",
               f"* Partitions with all 5 expected attributes matching: **{cmp['matched']} / {cmp['total']}**",
               f"* Partitions in output that the answer key does not know: {cmp['unexpected_partitions']}", "",
               _md_table(["scenario", "partitions", "matched"], [(k, v[0], v[1]) for k, v in cmp["by_scenario"].items()]), ""]
        if cmp["mismatches"]:
            md += ["First mismatches:", ""] + [f"* `{m}`" for m in cmp["mismatches"]] + [""]
        if "rejects" in cmp:
            r = cmp["rejects"]
            md += ["### Rejects: injected vs quarantined", "",
                   f"* Expected {r['expected']} injected bad records, quarantined {r['actual']}; "
                   f"exact (file, line, reason) match: **{r['exact_match']}**", ""]
            if not r["exact_match"]:
                md += [f"* missing: `{r['missing']}`", f"* unexpected: `{r['unexpected']}`", ""]
        if "counts" in cmp:
            md += ["### Row counts: expected vs actual (event files)", "",
                   _md_table(["metric", "expected", "actual", "match"],
                             [(k, v[0], v[1], "yes" if v[0] == v[1] else "**NO**") for k, v in cmp["counts"].items()]), ""]
        ok = ok and cmp["matched"] == cmp["total"] and cmp["unexpected_partitions"] == 0 and \
            cmp.get("rejects", {}).get("exact_match", True) and all(v[0] == v[1] for v in cmp.get("counts", {}).values())
    md += ["## 3. Sample queries and results", ""]
    for title, sql in SAMPLE_QUERIES.items():
        cur = store.conn.execute(sql)
        cols = [c[0] for c in cur.description]
        md += [f"**{title}**", "", "```sql", sql.strip(), "```", "", _md_table(cols, cur.fetchall()), ""]
    md += ["---", f"Overall: **{'PASS' if ok else 'FAIL'}**", ""]
    return "\n".join(md), ok


def write_reports(store: Store, out_dir: str | Path, expected_dir: str | Path | None) -> tuple[Path, bool]:
    out = Path(out_dir)
    export_csvs(store, out)
    md, ok = build_validation_report(store, expected_dir)
    path = out / "validation_report.md"
    path.write_text(md)
    return path, ok
