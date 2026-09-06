# nyc311-ai-etl - common tasks.
# Everything runs against WAREHOUSE (duckdb by default); set WAREHOUSE=bigquery
# to point the loader and dbt at your BigQuery sandbox in one move.

PY          ?= .venv/bin/python
DBT         ?= .venv/bin/dbt
WAREHOUSE   ?= duckdb
BATCH_DATE  ?= 2024-01-15
START_DATE  ?= 2024-01-08
DAYS        ?= 14
DBT_DIR      = dbt/nyc311

export WAREHOUSE
export DBT_TARGET = $(WAREHOUSE)
export DUCKDB_PATH = $(CURDIR)/warehouse/nyc311.duckdb
export DBT_PROFILES_DIR = $(CURDIR)/$(DBT_DIR)

.PHONY: help setup test lint run backfill extract load dbt-run dbt-test dbt-docs report \
        airflow-up airflow-down airflow-logs airflow-trigger clean reset

help:  ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

setup:  ## Create the venv and install dependencies
	python3 -m venv .venv
	$(PY) -m pip install --upgrade pip
	$(PY) -m pip install -r requirements-dev.txt

test:  ## Run the Python unit tests
	$(PY) -m pytest tests/ -v

run:  ## Run the full pipeline for one batch date (BATCH_DATE=YYYY-MM-DD)
	$(PY) -m pipeline.cli run --batch-date $(BATCH_DATE)

backfill:  ## Run DAYS consecutive daily batches starting at START_DATE
	./scripts/backfill.sh $(START_DATE) $(DAYS)

extract:  ## Extract one batch date into data/raw/
	$(PY) -m pipeline.cli extract --batch-date $(BATCH_DATE)

load:  ## AI-map headers and load one batch date into the warehouse
	$(PY) -m pipeline.cli map-and-load --batch-date $(BATCH_DATE) --batch-id make-$(BATCH_DATE)

dbt-run:  ## Build all dbt models
	cd $(DBT_DIR) && $(CURDIR)/$(DBT) run --profiles-dir .

dbt-test:  ## Run the dbt test suite
	cd $(DBT_DIR) && $(CURDIR)/$(DBT) test --profiles-dir .

dashboard:  ## Run the project dashboard on :8501
	$(PY) -m streamlit run app/streamlit_app.py

demo-data:  ## Refresh the committed demo snapshot from the live warehouse
	$(PY) scripts/export_demo_data.py

dbt-docs:  ## Generate and serve dbt documentation on :8081
	cd $(DBT_DIR) && $(CURDIR)/$(DBT) docs generate --profiles-dir . \
	  && $(CURDIR)/$(DBT) docs serve --profiles-dir . --port 8081

report:  ## Print the data quality report for BATCH_DATE
	$(PY) -m pipeline.cli report --batch-date $(BATCH_DATE)

airflow-up:  ## Start Airflow (http://localhost:8080, airflow/airflow)
	AIRFLOW_UID=$$(id -u) docker compose -f docker/docker-compose.yaml up -d --build
	@echo "Airflow starting at http://localhost:8080 (airflow / airflow)"

airflow-down:  ## Stop Airflow and remove its volumes
	docker compose -f docker/docker-compose.yaml down -v

airflow-logs:  ## Tail the scheduler log
	docker compose -f docker/docker-compose.yaml logs -f airflow-scheduler

airflow-trigger:  ## Unpause the DAG so its backfill window runs
	docker compose -f docker/docker-compose.yaml exec airflow-scheduler \
	  airflow dags unpause nyc311_ai_etl

clean:  ## Remove dbt artifacts and extracted raw files
	rm -rf $(DBT_DIR)/target $(DBT_DIR)/logs data/raw/*

reset: clean  ## Also drop the warehouse and all reports (destroys loaded data)
	rm -f warehouse/nyc311.duckdb
	rm -rf reports/*
