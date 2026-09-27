"""Configuration centralisee du pipeline (surchargeable par variables d'environnement)."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Base URL du CDN NYC TLC. Si un telechargement echoue, verifier les liens
# sur la page officielle "TLC Trip Record Data" et surcharger TLC_BASE_URL.
TLC_BASE_URL = os.getenv("TLC_BASE_URL", "https://d37ci6vzurychx.cloudfront.net")
TRIP_DATA_PATH = "trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
ZONE_LOOKUP_PATH = "misc/taxi_zone_lookup.csv"


@dataclass(frozen=True)
class Paths:
    raw: Path = PROJECT_ROOT / "data" / "raw"
    clean: Path = PROJECT_ROOT / "data" / "clean"
    zones_csv: Path = PROJECT_ROOT / "data" / "raw" / "taxi_zone_lookup.csv"


@dataclass(frozen=True)
class QualityRules:
    """Seuils de validation metier appliques dans le job Spark."""

    min_trip_minutes: float = 1.0
    max_trip_minutes: float = 360.0
    min_distance_miles: float = 0.1
    max_distance_miles: float = 200.0
    min_total_amount: float = 0.01
    max_total_amount: float = 5_000.0
    max_passengers: int = 8


@dataclass(frozen=True)
class BigQueryConfig:
    project: str = os.getenv("GCP_PROJECT_ID", "")
    raw_dataset: str = os.getenv("BQ_RAW_DATASET", "nyc_taxi_raw")
    analytics_dataset: str = os.getenv("BQ_ANALYTICS_DATASET", "nyc_taxi_analytics")
    location: str = os.getenv("BQ_LOCATION", "US")
    trips_table: str = "trips_clean"
    zones_table: str = "taxi_zones"

    @property
    def trips_table_id(self) -> str:
        return f"{self.project}.{self.raw_dataset}.{self.trips_table}"

    @property
    def zones_table_id(self) -> str:
        return f"{self.project}.{self.raw_dataset}.{self.zones_table}"


# Partitionnement BigQuery par entier sur year_month (ex. 202401).
# Le sandbox expire les partitions *temporelles* de plus de 60 jours : des donnees 2024
# partitionnees par date seraient supprimees des le chargement (verifie par
# scripts/poc_sandbox.py). Le partitionnement par entier n'est pas concerne.
YEAR_MONTH_RANGE = (200901, 203001, 1)  # start, end (exclu), interval -> 2100 partitions


def year_month(year: int, month: int) -> int:
    return year * 100 + month


PATHS = Paths()
RULES = QualityRules()
BQ = BigQueryConfig()

SPARK_CONF: dict[str, str] = {
    "spark.sql.session.timeZone": "UTC",
    "spark.sql.shuffle.partitions": os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"),
    "spark.driver.memory": os.getenv("SPARK_DRIVER_MEMORY", "4g"),
    "spark.sql.parquet.compression.codec": "snappy",
    # INT96 (defaut Spark) est un format legacy : on ecrit des timestamps standard.
    "spark.sql.parquet.outputTimestampType": "TIMESTAMP_MICROS",
}
