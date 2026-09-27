"""DAG mensuel : download -> spark transform -> load BigQuery -> dbt build.

Le DAG n'embarque aucune logique metier : il orchestre les modules de `src/`
et le projet dbt. Backfill possible via `airflow dags backfill`.
"""
from __future__ import annotations

import os
import pendulum
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

PROJECT_DIR = os.getenv("NYC_PROJECT_DIR", "/opt/airflow/project")
DBT_DIR = f"{PROJECT_DIR}/dbt"

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "execution_timeout": pendulum.duration(hours=2),
}


@dag(
    dag_id="nyc_taxi_pipeline",
    description="Pipeline mensuel NYC Yellow Taxi (Spark -> BigQuery -> dbt)",
    schedule="0 6 5 * *",  # le 5 de chaque mois : la TLC publie avec du retard
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["nyc-taxi", "spark", "bigquery", "dbt"],
)
def nyc_taxi_pipeline():
    @task
    def resolve_period(data_interval_end=None) -> dict[str, int]:
        """Le run du mois M traite les donnees du mois M-2 (delai de publication TLC).

        Pour un planning cron, le run declenche le 5 du mois M a
        data_interval_start = 5 du mois M-1 et data_interval_end = 5 du mois M.
        On part donc de data_interval_end (partir de start donnerait M-3).
        """
        target = data_interval_end.subtract(months=2)
        return {"year": target.year, "month": target.month}

    @task
    def download(period: dict[str, int]) -> dict[str, int]:
        from src.download import download_month, download_zones

        download_zones()
        download_month(period["year"], period["month"])
        return period

    @task
    def spark_transform(period: dict[str, int]) -> dict:
        from src.transform import build_spark, transform

        spark = build_spark(f"nyc-taxi-{period['year']}-{period['month']:02d}")
        try:
            report = transform(spark, period["year"], period["month"])
        finally:
            spark.stop()

        if report["rows_out"] == 0:
            raise ValueError("aucune ligne valide apres nettoyage")
        return report

    @task
    def load_bigquery(report: dict) -> int:
        """Chargement idempotent (WRITE_TRUNCATE sur la partition) + verification du compte."""
        from src.load_bq import ensure_datasets, get_client, load_trips, load_zones

        client = get_client()
        ensure_datasets(client)
        load_zones(client)
        return load_trips(client, report["year"], report["month"])

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            f"cd {DBT_DIR} && "
            "dbt deps --quiet && "
            f"dbt build --profiles-dir {DBT_DIR}"
        ),
    )

    period = resolve_period()
    report = spark_transform(download(period))
    load_bigquery(report) >> dbt_build


nyc_taxi_pipeline()
