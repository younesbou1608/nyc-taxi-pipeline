.PHONY: setup lint test poc download transform load dbt-build pipeline rebuild clean \
	airflow-up airflow-down airflow-logs airflow-password airflow-test

YEAR ?= 2024
MONTH ?= 1
MONTHS ?= 1 2 3

setup:
	python3.11 -m venv .venv && .venv/bin/python -m pip install -r requirements-dev.txt

lint:
	ruff check src tests dags scripts

test:
	pytest -q

# Verifie les contraintes du sandbox BigQuery (a lancer une fois par projet GCP)
poc:
	python scripts/poc_sandbox.py --project $(GCP_PROJECT_ID)

download:
	python -m src.download --year $(YEAR) --months $(MONTH)

transform:
	python -m src.transform --year $(YEAR) --month $(MONTH)

load:
	python -m src.load_bq --year $(YEAR) --month $(MONTH)

dbt-build:
	cd dbt && dbt deps && dbt build --profiles-dir .

# Un mois de bout en bout
pipeline: download transform load dbt-build

# Reconstruit tout l'entrepot (le sandbox expire les tables apres 60 jours).
# Exemple : make rebuild YEAR=2024 MONTHS="1 2 3"
rebuild:
	@set -e; for m in $(MONTHS); do \
		echo "=== $(YEAR)-$$m"; \
		python -m src.download --year $(YEAR) --months $$m; \
		python -m src.transform --year $(YEAR) --month $$m; \
		python -m src.load_bq --year $(YEAR) --month $$m; \
	done
	$(MAKE) dbt-build

# --- Airflow (Docker) ---
airflow-up:
	docker compose up -d --build

airflow-down:
	docker compose down

airflow-logs:
	docker compose logs -f airflow

airflow-password:
	docker compose exec airflow cat /opt/airflow/standalone_admin_password.txt; echo

# Execute le DAG complet pour un mois, sans passer par le scheduler
airflow-test:
	docker compose exec airflow airflow dags test nyc_taxi_pipeline \
		--conf '{"year": $(YEAR), "month": $(MONTH)}'

clean:
	rm -rf data/raw/*.parquet data/raw/*.csv data/clean/* dbt/target dbt/logs
