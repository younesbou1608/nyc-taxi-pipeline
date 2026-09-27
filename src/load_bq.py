"""Chargement des Parquet nettoyes dans BigQuery (dataset raw), compatible sandbox.

Contraintes du sandbox (verifiees par scripts/poc_sandbox.py) :
  - pas de DML (DELETE / UPDATE / MERGE)
  - partitions temporelles expirees au-dela de 60 jours -> donnees historiques perdues

Strategie :
  - trips_clean partitionnee par ENTIER sur year_month (202401...), clusterisee
    (pickup_zone_id, payment_type)
  - chargement du mois dans sa partition via le decorateur `trips_clean$202401` :
    1er fichier en WRITE_TRUNCATE (remplace le mois), suivants en WRITE_APPEND
    -> rejouable sans doublon, sans DML
  - verification post-chargement : lignes en base == rows_out du rapport qualite Spark

Usage:
    python -m src.load_bq --year 2024 --month 1
    python -m src.load_bq --year 2024 --month 1 --zones-only
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from src.config import BQ, PATHS, YEAR_MONTH_RANGE, year_month

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
    bigquery.SchemaField("year_month", "INTEGER", mode="REQUIRED"),
]

ZONES_SCHEMA = [
    bigquery.SchemaField("zone_id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("borough", "STRING"),
    bigquery.SchemaField("zone_name", "STRING"),
    bigquery.SchemaField("service_zone", "STRING"),
]


class LoadVerificationError(RuntimeError):
    """Le nombre de lignes en base ne correspond pas a la sortie du job Spark."""


# --------------------------------------------------------------------------- helpers purs
def partition_target(table_id: str, year: int, month: int) -> str:
    """Decorateur de partition BigQuery : `projet.dataset.table$202401`."""
    return f"{table_id}${year_month(year, month)}"


def write_dispositions(n_files: int) -> list[str]:
    """1er fichier remplace la partition, les suivants s'y ajoutent."""
    append = bigquery.WriteDisposition.WRITE_APPEND
    return [bigquery.WriteDisposition.WRITE_TRUNCATE] + [append] * (n_files - 1)


def expected_rows(year: int, month: int, quality_dir: Path | None = None) -> int:
    report = (quality_dir or PATHS.clean / "_quality") / f"{year}-{month:02d}.json"
    if not report.exists():
        raise FileNotFoundError(f"rapport qualite absent: {report} (lancer src.transform)")
    return int(json.loads(report.read_text())["rows_out"])


def _local_parquet_files(root: Path) -> list[Path]:
    return sorted(p for p in root.rglob("*.parquet") if not p.name.startswith("_"))


# --------------------------------------------------------------------------- BigQuery
def get_client() -> bigquery.Client:
    if not BQ.project:
        raise RuntimeError("GCP_PROJECT_ID non defini (voir .env.example)")
    return bigquery.Client(project=BQ.project, location=BQ.location)


def ensure_datasets(client: bigquery.Client) -> None:
    for name in (BQ.raw_dataset, BQ.analytics_dataset):
        dataset = bigquery.Dataset(f"{BQ.project}.{name}")
        dataset.location = BQ.location
        client.create_dataset(dataset, exists_ok=True)


def ensure_trips_table(client: bigquery.Client) -> None:
    """Cree la table partitionnee par year_month.

    Une ancienne table partitionnee par date (design initial) est supprimee et recreee :
    dans le sandbox elle ne contient de toute facon aucune donnee historique.
    """
    try:
        existing = client.get_table(BQ.trips_table_id)
        if existing.range_partitioning and existing.range_partitioning.field == "year_month":
            return
        log.warning("table %s au mauvais partitionnement -> recreee", BQ.trips_table_id)
        client.delete_table(BQ.trips_table_id)
    except NotFound:
        pass

    start, end, interval = YEAR_MONTH_RANGE
    table = bigquery.Table(BQ.trips_table_id, schema=TRIPS_SCHEMA)
    table.range_partitioning = bigquery.RangePartitioning(
        field="year_month",
        range_=bigquery.PartitionRange(start=start, end=end, interval=interval),
    )
    table.clustering_fields = ["pickup_zone_id", "payment_type"]
    client.create_table(table)
    log.info("table creee: %s", BQ.trips_table_id)


def _load_files(
    client: bigquery.Client, files: list[Path], destination: str, schema: list
) -> int:
    loaded = 0
    for path, disposition in zip(files, write_dispositions(len(files)), strict=True):
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=disposition,
            schema=schema,
        )
        with path.open("rb") as handle:
            job = client.load_table_from_file(handle, destination, job_config=job_config)
        job.result()
        loaded += job.output_rows
        log.info("charge %s (%s lignes, %s)", path.name, job.output_rows, disposition)
    return loaded


def count_month(client: bigquery.Client, year: int, month: int) -> int:
    job = client.query(
        f"SELECT COUNT(*) AS n FROM `{BQ.trips_table_id}` WHERE year_month = @ym",
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("ym", "INT64", year_month(year, month))
            ]
        ),
    )
    return next(iter(job.result())).n


def load_trips(client: bigquery.Client, year: int, month: int) -> int:
    partition_dir = PATHS.clean / "trips" / f"year={year}" / f"month={month}"
    files = _local_parquet_files(partition_dir)
    if not files:
        raise FileNotFoundError(f"aucun parquet dans {partition_dir} (lancer src.transform)")

    ensure_trips_table(client)
    loaded = _load_files(
        client, files, partition_target(BQ.trips_table_id, year, month), TRIPS_SCHEMA
    )

    # Garde-fou : le design initial "reussissait" en chargeant 0 ligne. Plus jamais.
    expected = expected_rows(year, month)
    in_table = count_month(client, year, month)
    if not (loaded == expected == in_table):
        raise LoadVerificationError(
            f"{year}-{month:02d}: spark={expected} charge={loaded} en_base={in_table}"
        )
    log.info("verifie %s-%02d: %s lignes", year, month, in_table)
    return in_table


def load_zones(client: bigquery.Client) -> int:
    files = _local_parquet_files(PATHS.clean / "zones")
    if not files:
        raise FileNotFoundError("zones nettoyees absentes (lancer src.transform)")
    loaded = _load_files(client, files, BQ.zones_table_id, ZONES_SCHEMA)
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
