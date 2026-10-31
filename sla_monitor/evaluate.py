"""SLA evaluation: classify each expected partition, compute freshness, maintain alert lifecycle.

`classify_partition` is a PURE function (no I/O) so every SLA rule is unit-testable.
"""
from __future__ import annotations

import hashlib
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime

from .schedule import DatasetDef
from .store import Store
from .timeutil import fmt_ts, from_iso_z

ALERT_ACTIONS = {
    "MISSING_LOAD": "Check the upstream producer/job and the landing path; re-trigger the extract if it did not run.",
    "LATE_ARRIVAL": "Data arrived after its arrival deadline: confirm with the producer why it was delayed; no reload needed.",
    "LATE_COMPLETION": "Load finished after the completion SLA: inspect processing duration/resources for this partition.",
    "INCOMPLETE_LOAD": "Data arrived but processing never completed: check the load job logs and restart/backfill the partition.",
    "LOAD_FAILED": "Load reported FAILED and has not completed since: read the job error, fix, and re-run the partition.",
    "LOW_ROW_COUNT": "Completed with fewer rows than the dataset minimum: verify the source extract is not truncated.",
    "STALE_DATASET": "No successful completion within the freshness threshold: downstream consumers are reading stale data.",
}


@dataclass
class PartitionResult:
    dataset_id: str
    partition_key: str
    expected_arrival: datetime
    arrival_deadline: datetime
    completion_deadline: datetime
    arrived_at: datetime | None
    completed_at: datetime | None
    arrival_status: str
    completion_status: str
    sla_status: str
    arrival_delay_minutes: float | None
    completion_latency_minutes: float | None
    row_count: int | None
    low_row_count: bool
    arrival_inferred: bool
    timestamp_order_anomaly: bool


def classify_partition(d: DatasetDef, key: str, events: list[tuple[str, str, int | None]],
                       as_of: datetime) -> PartitionResult:
    """events: (event_type, event_ts_iso, row_count) with event_ts <= as_of. Order-independent.

    Rules
      arrival:     ON_TIME if first ARRIVED <= arrival_deadline (boundary inclusive); LATE if after;
                   no arrival: PENDING while as_of <= deadline, else MISSING.
                   A COMPLETED event with no ARRIVED event *implies* arrival (arrival_inferred).
      completion:  first COMPLETED decides ON_TIME/LATE vs completion_deadline (inclusive);
                   no completion: FAILED if any FAILED event, else PENDING until the deadline, then OVERDUE.
      sla_status:  BREACHED if arrival LATE/MISSING or completion LATE/OVERDUE (or FAILED past deadline);
                   MET if both ON_TIME; otherwise PENDING.
    """
    exp, ad, cd = d.expected_arrival(key), d.arrival_deadline(key), d.completion_deadline(key)
    arrived = None
    completions: list[tuple[datetime, int | None]] = []
    failed = False
    for etype, ts, rc in events:
        t = from_iso_z(ts)
        if etype == "ARRIVED":
            arrived = t if arrived is None or t < arrived else arrived
        elif etype == "COMPLETED":
            completions.append((t, rc))
        elif etype == "FAILED":
            failed = True
    completions.sort(key=lambda x: x[0])
    first_completed = completions[0][0] if completions else None
    final_rows = completions[-1][1] if completions else None      # latest completion = corrected reload wins

    inferred = False
    anomaly = False
    if arrived is None and first_completed is not None:
        arrived, inferred = first_completed, True
    elif arrived is not None and first_completed is not None and first_completed < arrived:
        anomaly = True                                             # completion logged before arrival: clock skew?

    if arrived is not None:
        arrival_status = "ON_TIME" if arrived <= ad else "LATE"
    else:
        arrival_status = "PENDING" if as_of <= ad else "MISSING"

    if first_completed is not None:
        completion_status = "ON_TIME" if first_completed <= cd else "LATE"
    elif failed:
        completion_status = "FAILED"
    else:
        completion_status = "PENDING" if as_of <= cd else "OVERDUE"

    if (arrival_status in ("LATE", "MISSING") or completion_status in ("LATE", "OVERDUE")
            or (completion_status == "FAILED" and as_of > cd)):
        sla = "BREACHED"
    elif arrival_status == "ON_TIME" and completion_status == "ON_TIME":
        sla = "MET"
    else:
        sla = "PENDING"

    low = bool(first_completed is not None and d.min_rows > 0 and final_rows is not None and final_rows < d.min_rows)
    mins = lambda a, b: round((a - b).total_seconds() / 60.0, 2)  # noqa: E731
    return PartitionResult(
        d.dataset_id, key, exp, ad, cd, arrived, first_completed, arrival_status, completion_status, sla,
        mins(arrived, exp) if arrived else None, mins(first_completed, exp) if first_completed else None,
        final_rows, low, inferred, anomaly)


def alerts_for(d: DatasetDef, r: PartitionResult) -> list[tuple[str, str]]:
    """(alert_type, message) pairs for one partition. Completion alerts are suppressed when the
    arrival itself is MISSING, to avoid two alerts for one root cause."""
    out = []
    k = r.partition_key
    if r.arrival_status == "MISSING":
        out.append(("MISSING_LOAD", f"{d.dataset_id} partition {k}: no arrival by {fmt_ts(r.arrival_deadline)}"))
    elif r.arrival_status == "LATE":
        out.append(("LATE_ARRIVAL", f"{d.dataset_id} partition {k}: arrived {fmt_ts(r.arrived_at)}, deadline "
                                    f"{fmt_ts(r.arrival_deadline)} ({r.arrival_delay_minutes - d.grace_minutes:.0f} min late)"))
    if r.arrival_status in ("ON_TIME", "LATE"):
        if r.completion_status == "LATE":
            out.append(("LATE_COMPLETION", f"{d.dataset_id} partition {k}: completed {fmt_ts(r.completed_at)}, "
                                           f"deadline {fmt_ts(r.completion_deadline)}"))
        elif r.completion_status == "OVERDUE":
            out.append(("INCOMPLETE_LOAD", f"{d.dataset_id} partition {k}: arrived but not completed by "
                                           f"{fmt_ts(r.completion_deadline)}"))
        elif r.completion_status == "FAILED":
            out.append(("LOAD_FAILED", f"{d.dataset_id} partition {k}: load FAILED and not completed since"))
    if r.low_row_count:
        out.append(("LOW_ROW_COUNT", f"{d.dataset_id} partition {k}: {r.row_count} rows < minimum {d.min_rows}"))
    return out


def alert_id_for(alert_type: str, dataset_id: str, partition_key: str) -> str:
    return "alr_" + hashlib.sha1(f"{alert_type}|{dataset_id}|{partition_key}".encode()).hexdigest()[:16]


@dataclass
class EvalResult:
    partitions: int = 0
    alerts_open: int = 0
    alerts_new: int = 0
    alerts_resolved: int = 0
    stale_datasets: int = 0


def evaluate(store: Store, as_of: datetime, lookback_hours: int, run_id: str) -> EvalResult:
    """Rebuild the snapshot tables for `as_of` and reconcile the alert lifecycle. One transaction."""
    as_of_s = fmt_ts(as_of)
    datasets = store.load_datasets()
    status_rows: list[tuple] = []
    fresh_rows: list[tuple] = []
    active: dict[str, tuple] = {}
    scope: set[tuple[str, str]] = set()
    conn = store.conn

    for d in datasets.values():
        keys = d.partition_keys(as_of, lookback_hours)
        by_part: dict[str, list] = defaultdict(list)
        if keys:
            cur = conn.execute(
                "SELECT partition_key,event_type,event_ts,row_count FROM load_events"
                " WHERE dataset_id=? AND partition_key>=? AND event_ts<=?", (d.dataset_id, keys[0], as_of_s))
            for pk, et, ts, rc in cur:
                by_part[pk].append((et, ts, rc))
        for k in keys:
            r = classify_partition(d, k, by_part.get(k, []), as_of)
            scope.add((d.dataset_id, k))
            status_rows.append((
                d.dataset_id, k, fmt_ts(r.expected_arrival), fmt_ts(r.arrival_deadline), fmt_ts(r.completion_deadline),
                fmt_ts(r.arrived_at) if r.arrived_at else None, fmt_ts(r.completed_at) if r.completed_at else None,
                r.arrival_status, r.completion_status, r.sla_status, r.arrival_delay_minutes,
                r.completion_latency_minutes, r.row_count, int(r.low_row_count), int(r.arrival_inferred),
                int(r.timestamp_order_anomaly), as_of_s, run_id))
            for atype, msg in alerts_for(d, r):
                active[alert_id_for(atype, d.dataset_id, k)] = (atype, d.severity, d.dataset_id, k, msg)

        last = store.scalar("SELECT MAX(event_ts) FROM load_events WHERE dataset_id=? AND event_type='COMPLETED'"
                            " AND event_ts<=?", (d.dataset_id, as_of_s))
        thr = d.max_staleness_minutes()
        if last is None:
            fstat, fmin = "NEVER_LOADED", None
        else:
            fmin = round((as_of - from_iso_z(last)).total_seconds() / 60.0, 2)
            fstat = "FRESH" if fmin <= thr else "STALE"
        fresh_rows.append((d.dataset_id, last, fmin, thr, fstat, as_of_s, run_id))
        if fstat != "FRESH":
            msg = (f"{d.dataset_id}: no completed load ever recorded" if fstat == "NEVER_LOADED" else
                   f"{d.dataset_id}: last completion {last} is {fmin:.0f} min old (threshold {thr} min)")
            active[alert_id_for("STALE_DATASET", d.dataset_id, "")] = ("STALE_DATASET", d.severity, d.dataset_id, "", msg)

    res = EvalResult(partitions=len(status_rows), alerts_open=len(active),
                     stale_datasets=sum(1 for f in fresh_rows if f[4] != "FRESH"))
    with store.transaction() as c:
        c.execute("DELETE FROM partition_sla_status")
        c.executemany("INSERT INTO partition_sla_status VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", status_rows)
        c.execute("DELETE FROM dataset_freshness")
        c.executemany("INSERT INTO dataset_freshness VALUES (?,?,?,?,?,?,?)", fresh_rows)

        existing = {r["alert_id"]: r for r in c.execute("SELECT alert_id,status,dataset_id,partition_key,alert_type FROM alerts")}
        res.alerts_new = sum(1 for a in active if a not in existing)
        for aid, (atype, sev, ds, pk, msg) in active.items():
            c.execute(
                "INSERT INTO alerts(alert_id,alert_type,severity,dataset_id,partition_key,status,first_detected_as_of,"
                "last_evaluated_as_of,resolved_as_of,message,suggested_action,detected_run_id,last_run_id)"
                " VALUES (?,?,?,?,?,'OPEN',?,?,NULL,?,?,?,?) ON CONFLICT(alert_id) DO UPDATE SET status='OPEN',"
                "severity=excluded.severity,last_evaluated_as_of=excluded.last_evaluated_as_of,resolved_as_of=NULL,"
                "message=excluded.message,suggested_action=excluded.suggested_action,last_run_id=excluded.last_run_id",
                (aid, atype, sev, ds, pk, as_of_s, as_of_s, msg, ALERT_ACTIONS[atype], run_id, run_id))
        # Resolve OPEN alerts that no longer apply -- but ONLY if their partition was in this evaluation's
        # window (or they are dataset-level). Aged-out partitions keep their alert instead of vanishing.
        to_resolve = [a for a, r in existing.items() if r["status"] == "OPEN" and a not in active and
                      (r["alert_type"] == "STALE_DATASET" or (r["dataset_id"], r["partition_key"]) in scope)]
        res.alerts_resolved = len(to_resolve)
        c.executemany("UPDATE alerts SET status='RESOLVED', resolved_as_of=?, last_evaluated_as_of=?, last_run_id=?"
                      " WHERE alert_id=?", [(as_of_s, as_of_s, run_id, a) for a in to_resolve])
    return res
