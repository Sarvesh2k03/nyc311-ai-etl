"""Central configuration. Everything is env-overridable so the same code runs
on the host (make targets) and inside the Airflow container without changes."""
from __future__ import annotations

import os
from pathlib import Path

# PROJECT_ROOT is the repo root both on the host and at /opt/project in Docker.
PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", Path(__file__).resolve().parents[1]))

DATA_DIR = Path(os.getenv("DATA_DIR", PROJECT_ROOT / "data"))
RAW_DIR = DATA_DIR / "raw"            # landed source files, partitioned by batch date
CANONICAL_DIR = DATA_DIR / "canonical"  # post-mapping, canonical-column files
REPORTS_DIR = Path(os.getenv("REPORTS_DIR", PROJECT_ROOT / "reports"))
WAREHOUSE_DIR = Path(os.getenv("WAREHOUSE_DIR", PROJECT_ROOT / "warehouse"))
DBT_PROJECT_DIR = Path(os.getenv("DBT_PROJECT_DIR", PROJECT_ROOT / "dbt" / "nyc311"))

# --- Source -----------------------------------------------------------------
SOCRATA_DATASET_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
SOCRATA_APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN")  # optional; raises rate limits
EXTRACT_PAGE_SIZE = int(os.getenv("EXTRACT_PAGE_SIZE", "50000"))
EXTRACT_ROW_LIMIT_PER_DAY = int(os.getenv("EXTRACT_ROW_LIMIT_PER_DAY", "40000"))

# --- Warehouse --------------------------------------------------------------
# "duckdb" (local, default) or "bigquery" (BigQuery sandbox). Selects both the
# load target and the dbt profile target, so they can never drift apart.
WAREHOUSE = os.getenv("WAREHOUSE", "duckdb").lower()
DUCKDB_PATH = Path(os.getenv("DUCKDB_PATH", WAREHOUSE_DIR / "nyc311.duckdb"))
BQ_PROJECT = os.getenv("BQ_PROJECT", "")
BQ_LOCATION = os.getenv("BQ_LOCATION", "US")
BQ_RAW_DATASET = os.getenv("BQ_RAW_DATASET", "nyc311_raw")

RAW_SCHEMA = os.getenv("RAW_SCHEMA", "raw")
RAW_TABLE = "service_requests_raw"

# --- AI schema mapper -------------------------------------------------------
AI_MAPPER_ENABLED = os.getenv("AI_MAPPER_ENABLED", "true").lower() == "true"
AI_MAPPER_MODEL = os.getenv("AI_MAPPER_MODEL", "claude-opus-5")
AI_MAPPER_MAX_TOKENS = int(os.getenv("AI_MAPPER_MAX_TOKENS", "8000"))
AI_MAPPER_TIMEOUT_S = float(os.getenv("AI_MAPPER_TIMEOUT_S", "90"))
# An LLM proposal below this confidence is discarded and handed to the fallback.
LLM_MIN_CONFIDENCE = float(os.getenv("LLM_MIN_CONFIDENCE", "0.70"))
# rapidfuzz score (0-1) below which the fallback refuses to guess.
FUZZY_MIN_SCORE = float(os.getenv("FUZZY_MIN_SCORE", "0.82"))

for _d in (RAW_DIR, CANONICAL_DIR, REPORTS_DIR, WAREHOUSE_DIR):
    _d.mkdir(parents=True, exist_ok=True)
