.PHONY: setup lint test download transform load dbt-build pipeline clean

YEAR ?= 2024
MONTH ?= 1

setup:
	python -m venv .venv && . .venv/bin/activate && pip install -r requirements-dev.txt

lint:
	ruff check src tests dags

test:
	pytest -q

download:
	python -m src.download --year $(YEAR) --months $(MONTH)

transform:
	python -m src.transform --year $(YEAR) --month $(MONTH)

load:
	python -m src.load_bq --year $(YEAR) --month $(MONTH)

dbt-build:
	cd dbt && dbt deps && dbt build --profiles-dir .

pipeline: download transform load dbt-build

clean:
	rm -rf data/raw/*.parquet data/clean/* dbt/target dbt/logs
