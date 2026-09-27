"""Job PySpark : nettoyage, enrichissement et partitionnement des trajets.

Lit  : data/raw/yellow_tripdata_<year>-<month>.parquet + taxi_zone_lookup.csv
Ecrit: data/clean/trips/year=<y>/month=<m>/*.parquet
       data/clean/zones/*.parquet
       data/clean/_quality/<year>-<month>.json  (metriques de rejet)

Usage:
    python -m src.transform --year 2024 --month 1
    python -m src.transform --year 2024 --month 1 --sample 100000
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src.config import PATHS, RULES, SPARK_CONF

log = logging.getLogger(__name__)

ZONES_SCHEMA = StructType(
    [
        StructField("LocationID", IntegerType(), False),
        StructField("Borough", StringType(), True),
        StructField("Zone", StringType(), True),
        StructField("service_zone", StringType(), True),
    ]
)

PAYMENT_TYPES = {
    1: "Credit card",
    2: "Cash",
    3: "No charge",
    4: "Dispute",
    5: "Unknown",
    6: "Voided trip",
}

# Colonnes source utiles (le schema TLC varie legerement selon les annees).
SOURCE_COLUMNS = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "tip_amount",
    "tolls_amount",
    "total_amount",
]


def build_spark(app_name: str = "nyc-taxi-transform") -> SparkSession:
    builder = SparkSession.builder.appName(app_name)
    for key, value in SPARK_CONF.items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_zones(spark: SparkSession, csv_path: Path) -> DataFrame:
    """Referentiel des 265 zones TLC : petite table, diffusee en broadcast au join."""
    return (
        spark.read.option("header", True)
        .schema(ZONES_SCHEMA)
        .csv(str(csv_path))
        .select(
            F.col("LocationID").alias("zone_id"),
            F.initcap(F.trim("Borough")).alias("borough"),
            F.trim("Zone").alias("zone_name"),
            F.trim("service_zone").alias("service_zone"),
        )
        .dropDuplicates(["zone_id"])
    )


def read_trips(spark: SparkSession, parquet_path: Path) -> DataFrame:
    df = spark.read.parquet(str(parquet_path))
    missing = [c for c in SOURCE_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"colonnes absentes du fichier source: {missing}")
    return df.select(*SOURCE_COLUMNS)


def rename_and_cast(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("VendorID").cast("int").alias("vendor_id"),
        F.col("tpep_pickup_datetime").cast("timestamp").alias("pickup_at"),
        F.col("tpep_dropoff_datetime").cast("timestamp").alias("dropoff_at"),
        F.col("passenger_count").cast("int").alias("passenger_count"),
        F.col("trip_distance").cast("double").alias("trip_distance_miles"),
        F.col("PULocationID").cast("int").alias("pickup_zone_id"),
        F.col("DOLocationID").cast("int").alias("dropoff_zone_id"),
        F.col("payment_type").cast("int").alias("payment_type_id"),
        F.col("fare_amount").cast("double").alias("fare_amount"),
        F.col("tip_amount").cast("double").alias("tip_amount"),
        F.col("tolls_amount").cast("double").alias("tolls_amount"),
        F.col("total_amount").cast("double").alias("total_amount"),
    )


def add_derived_columns(df: DataFrame) -> DataFrame:
    duration_min = (
        F.col("dropoff_at").cast("long") - F.col("pickup_at").cast("long")
    ) / 60.0
    return (
        df.withColumn("trip_duration_min", F.round(duration_min, 2))
        .withColumn(
            "avg_speed_mph",
            F.when(
                F.col("trip_duration_min") > 0,
                F.round(F.col("trip_distance_miles") / (F.col("trip_duration_min") / 60.0), 2),
            ).otherwise(F.lit(None).cast(DoubleType())),
        )
        .withColumn(
            "tip_rate",
            F.when(
                F.col("fare_amount") > 0,
                F.round(F.col("tip_amount") / F.col("fare_amount"), 4),
            ).otherwise(F.lit(None).cast(DoubleType())),
        )
        .withColumn("pickup_date", F.to_date("pickup_at"))
        .withColumn("pickup_hour", F.hour("pickup_at"))
        .withColumn("pickup_dow", F.dayofweek("pickup_at"))
        .withColumn("is_weekend", F.col("pickup_dow").isin(1, 7))
        .withColumn("year", F.year("pickup_at"))
        .withColumn("month", F.month("pickup_at"))
        .withColumn("year_month", F.col("year") * 100 + F.col("month"))
    )


def valid_trip_condition(year: int, month: int):
    """Predicat unique : une ligne valide satisfait toutes ces regles metier."""
    return (
        F.col("pickup_at").isNotNull()
        & F.col("dropoff_at").isNotNull()
        & (F.col("dropoff_at") > F.col("pickup_at"))
        & (F.col("year") == year)
        & (F.col("month") == month)
        & F.col("trip_duration_min").between(RULES.min_trip_minutes, RULES.max_trip_minutes)
        & F.col("trip_distance_miles").between(
            RULES.min_distance_miles, RULES.max_distance_miles
        )
        & F.col("total_amount").between(RULES.min_total_amount, RULES.max_total_amount)
        & (F.col("fare_amount") >= 0)
        & (F.col("tip_amount") >= 0)
        & F.coalesce(F.col("passenger_count"), F.lit(1)).between(1, RULES.max_passengers)
        & F.col("pickup_zone_id").isNotNull()
        & F.col("dropoff_zone_id").isNotNull()
    )


def enrich_with_zones(trips: DataFrame, zones: DataFrame) -> DataFrame:
    pickup = zones.select(
        F.col("zone_id").alias("pickup_zone_id"),
        F.col("borough").alias("pickup_borough"),
        F.col("zone_name").alias("pickup_zone_name"),
    )
    dropoff = zones.select(
        F.col("zone_id").alias("dropoff_zone_id"),
        F.col("borough").alias("dropoff_borough"),
        F.col("zone_name").alias("dropoff_zone_name"),
    )
    payment_map = F.create_map(
        *[x for k, v in PAYMENT_TYPES.items() for x in (F.lit(k), F.lit(v))]
    )
    return (
        trips.join(F.broadcast(pickup), on="pickup_zone_id", how="left")
        .join(F.broadcast(dropoff), on="dropoff_zone_id", how="left")
        .withColumn(
            "payment_type",
            F.coalesce(payment_map[F.col("payment_type_id")], F.lit("Unknown")),
        )
    )


def transform(spark: SparkSession, year: int, month: int, sample: int | None = None) -> dict:
    source = PATHS.raw / f"yellow_tripdata_{year}-{month:02d}.parquet"
    if not source.exists():
        raise FileNotFoundError(f"fichier absent: {source} (lancer src.download d'abord)")

    raw = read_trips(spark, source)
    if sample:
        raw = raw.limit(sample)

    typed = add_derived_columns(rename_and_cast(raw)).cache()
    total = typed.count()

    clean = typed.filter(valid_trip_condition(year, month))
    kept = clean.count()

    enriched = enrich_with_zones(clean, read_zones(spark, PATHS.zones_csv)).select(
        "vendor_id",
        "pickup_at",
        "dropoff_at",
        "pickup_date",
        "pickup_hour",
        "pickup_dow",
        "is_weekend",
        "passenger_count",
        "trip_distance_miles",
        "trip_duration_min",
        "avg_speed_mph",
        "pickup_zone_id",
        "pickup_borough",
        "pickup_zone_name",
        "dropoff_zone_id",
        "dropoff_borough",
        "dropoff_zone_name",
        "payment_type_id",
        "payment_type",
        "fare_amount",
        "tip_amount",
        "tip_rate",
        "tolls_amount",
        "total_amount",
        "year_month",
        "year",
        "month",
    )

    target = PATHS.clean / "trips"
    (
        enriched.repartition(4)
        .write.mode("overwrite")
        .partitionBy("year", "month")
        .option("partitionOverwriteMode", "dynamic")
        .parquet(str(target))
    )

    zones_target = PATHS.clean / "zones"
    read_zones(spark, PATHS.zones_csv).write.mode("overwrite").parquet(str(zones_target))

    typed.unpersist()

    report = {
        "year": year,
        "month": month,
        "rows_in": total,
        "rows_out": kept,
        "rows_rejected": total - kept,
        "reject_rate": round((total - kept) / total, 4) if total else 0.0,
    }
    quality_dir = PATHS.clean / "_quality"
    quality_dir.mkdir(parents=True, exist_ok=True)
    (quality_dir / f"{year}-{month:02d}.json").write_text(json.dumps(report, indent=2))
    log.info("qualite: %s", report)

    if total and report["reject_rate"] > 0.25:
        log.warning(
            "taux de rejet eleve (%.1f%%) - verifier les regles", report["reject_rate"] * 100
        )
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Clean & enrich NYC taxi trips with Spark")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    parser.add_argument("--sample", type=int, default=None, help="limiter le nb de lignes (dev)")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    spark = build_spark()
    try:
        transform(spark, args.year, args.month, args.sample)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
