"""Tests unitaires du job Spark (SparkSession locale, jeux de donnees minuscules)."""
from __future__ import annotations

import datetime as dt

import pytest
from pyspark.sql import SparkSession

from src.transform import (
    add_derived_columns,
    enrich_with_zones,
    valid_trip_condition,
)


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.appName("tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _trip(**overrides):
    base = dict(
        vendor_id=1,
        pickup_at=dt.datetime(2024, 1, 15, 8, 0),
        dropoff_at=dt.datetime(2024, 1, 15, 8, 20),
        passenger_count=2,
        trip_distance_miles=5.0,
        pickup_zone_id=100,
        dropoff_zone_id=200,
        payment_type_id=1,
        fare_amount=20.0,
        tip_amount=4.0,
        tolls_amount=0.0,
        total_amount=24.0,
    )
    base.update(overrides)
    return base


def test_derived_columns(spark):
    df = add_derived_columns(spark.createDataFrame([_trip()]))
    row = df.collect()[0]
    assert row.trip_duration_min == pytest.approx(20.0)
    assert row.avg_speed_mph == pytest.approx(15.0)
    assert row.tip_rate == pytest.approx(0.2)
    assert (row.year, row.month, row.pickup_hour) == (2024, 1, 7)


@pytest.mark.parametrize(
    "override",
    [
        {"dropoff_at": dt.datetime(2024, 1, 15, 8, 0, 30)},   # trop court
        {"trip_distance_miles": 0.0},                          # distance nulle
        {"total_amount": -5.0},                                # montant negatif
        {"passenger_count": 15},                               # trop de passagers
        {"pickup_at": dt.datetime(2023, 12, 31, 23, 0),
         "dropoff_at": dt.datetime(2023, 12, 31, 23, 30)},     # hors periode
    ],
)
def test_invalid_rows_are_filtered(spark, override):
    df = add_derived_columns(spark.createDataFrame([_trip(**override)]))
    assert df.filter(valid_trip_condition(2024, 1)).count() == 0


def test_valid_row_is_kept(spark):
    df = add_derived_columns(spark.createDataFrame([_trip()]))
    assert df.filter(valid_trip_condition(2024, 1)).count() == 1


def test_enrich_with_zones(spark):
    trips = add_derived_columns(spark.createDataFrame([_trip()]))
    zones = spark.createDataFrame(
        [(100, "Manhattan", "Midtown", "Yellow Zone"),
         (200, "Queens", "Astoria", "Boro Zone")],
        "zone_id int, borough string, zone_name string, service_zone string",
    )
    row = enrich_with_zones(trips, zones).collect()[0]
    assert row.pickup_borough == "Manhattan"
    assert row.dropoff_zone_name == "Astoria"
    assert row.payment_type == "Credit card"


def test_unknown_payment_type_defaults(spark):
    trips = add_derived_columns(spark.createDataFrame([_trip(payment_type_id=99)]))
    zones = spark.createDataFrame(
        [(100, "Manhattan", "Midtown", "Yellow Zone")],
        "zone_id int, borough string, zone_name string, service_zone string",
    )
    assert enrich_with_zones(trips, zones).collect()[0].payment_type == "Unknown"
