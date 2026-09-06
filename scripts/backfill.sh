#!/usr/bin/env bash
# Simulate N consecutive daily runs of the pipeline: one full
# extract -> map/load -> dbt run -> dbt test -> report cycle per batch date.
# This is what the Airflow DAG does on a schedule, run here without Airflow so
# the pipeline can be exercised and measured on its own.
set -euo pipefail

START="${1:-2024-01-08}"
DAYS="${2:-14}"
PY="${PY:-.venv/bin/python}"

cd "$(dirname "$0")/.."

for i in $(seq 0 $((DAYS - 1))); do
  BATCH_DATE=$(python3 -c "
from datetime import date, timedelta
print(date.fromisoformat('$START') + timedelta(days=$i))")
  echo "=== batch $BATCH_DATE ($((i + 1))/$DAYS) ==="
  $PY -m pipeline.cli run --batch-date "$BATCH_DATE" --batch-id "backfill-$BATCH_DATE"
done
