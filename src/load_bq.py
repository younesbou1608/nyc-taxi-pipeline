"""Chargement des Parquet nettoyes dans BigQuery (dataset raw).

Strategie : ecriture idempotente par partition mensuelle.
  - table trips_clean partitionnee par pickup_date, clusterisee (pickup_zone_id, payment_type)
  - le mois recharge est d'abord supprime (DELETE) puis reinsere -> rejouable sans doublon

Usage:
    python -m src.load_bq --year 2024 --month 1
    python -m src.load_bq --year 2024 --month 1 --zones-only
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from src.config import BQ, PATHS

log = logging.getLogger(__name__)

TRIPS_SCHEMA = [
    bigquery.SchemaField("vendor_id", "INTEGER"),
    bigquery.SchemaField("pickup_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("dropoff_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("pickup_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("pickup_hour", "INTEGER"),
    bigquery.SchemaField("pickup_dow", "INTEGER"),
    bigquery.SchemaField("is_weekend", "BOOLEAN"),
    bigquery.SchemaField("passenger_count", "INTEGER"),
    bigquery.SchemaField("trip_distance_miles", "FLOAT"),
    bigquery.SchemaField("trip_duration_min", "FLOAT"),
    bigquery.SchemaField("avg_speed_mph", "FLOAT"),
    bigquery.SchemaField("pickup_zone_id", "INTEGER"),
    bigquery.SchemaField("pickup_borough", "STRING"),
    bigquery.SchemaField("pickup_zone_name", "STRING"),
    bigquery.SchemaField("dropoff_zone_id", "INTEGER"),
    bigquery.SchemaField("dropoff_borough", "STRING"),
    bigquery.SchemaField("dropoff_zone_name", "STRING"),
    bigquery.SchemaField("payment_type_id", "INTEGER"),
    bigquery.SchemaField("payment_type", "STRING"),
    bigquery.SchemaField("fare_amount", "FLOAT"),
    bigquery.SchemaField("tip_amount", "FLOAT"),
    bigquery.SchemaField("tip_rate", "FLOAT"),
    bigquery.SchemaField("tolls_amount", "FLOAT"),
    bigquery.SchemaField("total_amount", "FLOAT"),
]

ZONES_SCHEMA = [
    bigquery.SchemaField("zone_id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("borough", "STRING"),
    bigquery.SchemaField("zone_name", "STRING"),
    bigquery.SchemaField("service_zone", "STRING"),
]


def get_client() -> bigquery.Client:
    if not BQ.project:
        raise RuntimeError("GCP_PROJECT_ID non defini (voir .env.example)")
    return bigquery.Client(project=BQ.project, location=BQ.location)


def ensure_datasets(client: bigquery.Client) -> None:
    for name in (BQ.raw_dataset, BQ.analytics_dataset):
        dataset_id = f"{BQ.project}.{name}"
        try:
            client.get_dataset(dataset_id)
        except NotFound:
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = BQ.location
            client.create_dataset(dataset)
            log.info("dataset cree: %s", dataset_id)


def ensure_trips_table(client: bigquery.Client) -> None:
    try:
        client.get_table(BQ.trips_table_id)
        return
    except NotFound:
        pass
    table = bigquery.Table(BQ.trips_table_id, schema=TRIPS_SCHEMA)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY, field="pickup_date"
    )
    table.clustering_fields = ["pickup_zone_id", "payment_type"]
    client.create_table(table)
    log.info("table creee: %s", BQ.trips_table_id)


def _local_parquet_files(root: Path) -> list[Path]:
    return sorted(p for p in root.rglob("*.parquet") if not p.name.startswith("_"))


def delete_month(client: bigquery.Client, year: int, month: int) -> None:
    """Rend le chargement idempotent : on purge le mois avant de le reinserer."""
    query = f"""
        DELETE FROM `{BQ.trips_table_id}`
        WHERE EXTRACT(YEAR FROM pickup_date) = @year
          AND EXTRACT(MONTH FROM pickup_date) = @month
    """
    job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("year", "INT64", year),
                bigquery.ScalarQueryParameter("month", "INT64", month),
            ]
        ),
    )
    job.result()
    log.info("purge %s-%02d: %s lignes supprimees", year, month, job.num_dml_affected_rows)


def load_trips(client: bigquery.Client, year: int, month: int) -> int:
    partition_dir = PATHS.clean / "trips" / f"year={year}" / f"month={month}"
    files = _local_parquet_files(partition_dir)
    if not files:
        raise FileNotFoundError(f"aucun parquet dans {partition_dir} (lancer src.transform)")

    ensure_trips_table(client)
    #delete_month(client, year, month)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=TRIPS_SCHEMA,
    )
    loaded = 0
    for path in files:
        with path.open("rb") as handle:
            job = client.load_table_from_file(
                handle, BQ.trips_table_id, job_config=job_config
            )
        job.result()
        loaded += job.output_rows
        log.info("charge %s (%s lignes)", path.name, job.output_rows)
    log.info("total charge pour %s-%02d: %s lignes", year, month, loaded)
    return loaded


def load_zones(client: bigquery.Client) -> int:
    files = _local_parquet_files(PATHS.clean / "zones")
    if not files:
        raise FileNotFoundError("zones nettoyees absentes (lancer src.transform)")

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=ZONES_SCHEMA,
    )
    loaded = 0
    for i, path in enumerate(files):
        if i == 1:
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        with path.open("rb") as handle:
            job = client.load_table_from_file(
                handle, BQ.zones_table_id, job_config=job_config
            )
        job.result()
        loaded += job.output_rows
    log.info("zones chargees: %s lignes", loaded)
    return loaded


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Load clean parquet into BigQuery")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    parser.add_argument("--zones-only", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    client = get_client()
    ensure_datasets(client)
    load_zones(client)
    if not args.zones_only:
        load_trips(client, args.year, args.month)
    return 0


if __name__ == "__main__":
    sys.exit(main())
