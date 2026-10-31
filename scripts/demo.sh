#!/usr/bin/env bash
# End-to-end demo of the operational behaviour: clean run, idempotent rerun, partial failure + retry, as-of replay.
# Usage: bash scripts/demo.sh        (from the project root; needs only python3). Writes only to output/demo/.
set -u
PY=${PYTHON:-python3}
AS_OF=2026-01-15T07:40:00Z
D=data/sample
O=output/demo
DB=$O/demo.db
step() { printf '\n== %s\n' "$1"; }
sla() { "$PY" -m sla_monitor "$@" 2>&1; }

step "0. Generate reproducible sample data (seed 42) + answer key"
rm -rf "$O" && mkdir -p "$O"
sla generate --out "$D" | sed -n '2,8p'

step "1. First run: ingest 8 files, quarantine bad rows, evaluate SLAs, reconcile against the answer key"
sla run --data-dir "$D" --db "$DB" --output-dir "$O" --as-of "$AS_OF" | grep -vE ' INFO | WARNING '; echo "exit code: ${PIPESTATUS[0]}"

step "2. Re-run on identical input: every file skipped, zero rows loaded, outputs unchanged"
sla run --data-dir "$D" --db "$DB" --output-dir "$O" --as-of "$AS_OF" | grep -vE ' INFO | WARNING '; echo "exit code: ${PIPESTATUS[0]}"

step "3. Partial failure: drop in 3 broken files (schema change, 0-byte, non-UTF8) + 1 empty batch + 1 extra-column file"
rm -rf $O/landing && mkdir -p $O/landing && cp $D/landing/*.csv $O/landing/
cp $D/faults/*.csv $O/landing/
sla run --data-dir "$D" --landing $O/landing --db "$DB" --output-dir $O/partial --as-of "$AS_OF" --no-report | grep -E "ERROR|WARNING|^run "
echo "exit code: ${PIPESTATUS[0]}   (2 = PARTIAL_SUCCESS: good files loaded, broken ones reported, nothing half-loaded)"

step "4. Retry: producer fixes the schema and removes the junk -> ONLY the fixed file is processed; the rest are skipped"
sed -i 's/event_time/event_ts/' $O/landing/bad_schema.csv
rm $O/landing/empty_file.csv $O/landing/not_utf8.csv
sla run --data-dir "$D" --landing $O/landing --db "$DB" --output-dir $O/partial --as-of "$AS_OF" --no-report | grep -E "ERROR|^run |INFO file=bad_schema|INFO file=.*skipped" | sed 's/^[0-9: ,-]*Z //' | head -6
echo "exit code: ${PIPESTATUS[0]}"

step "5. Replay/backfill: re-evaluate the SAME stored events 4h later (window widened by 4h so it covers the same partitions)"
sla evaluate --db "$DB" --output-dir $O --as-of 2026-01-15T11:40:00Z --lookback-hours 172 | grep -v INFO
"$PY" - <<PYEOF
import sqlite3
con = sqlite3.connect("$DB")
print("alerts by status:", dict(con.execute("SELECT status, COUNT(*) FROM alerts GROUP BY status")))
print("example of a RESOLVED alert (history kept):")
for r in con.execute("SELECT alert_type, dataset_id, partition_key, first_detected_as_of, resolved_as_of FROM alerts WHERE status='RESOLVED' LIMIT 2"):
    print("  ", r)
PYEOF
