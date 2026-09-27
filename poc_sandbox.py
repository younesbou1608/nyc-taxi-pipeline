"""POC : valide les contraintes du BigQuery sandbox avant de refactorer le pipeline.

Tests :
  0. Etat de la table trips_clean existante (info)
  1. DML (DELETE) autorise ou non (info)
  2. Chargement WRITE_TRUNCATE par partition entiere (year_month) -> idempotence  [DECISIF]
  3. Partition par DATE avec des dates 2024 -> perdues ou non (info)
  4. CREATE OR REPLACE TABLE ... PARTITION BY RANGE_BUCKET (= dbt `table`)       [DECISIF]

Usage (depuis la racine du repo, venv active) :
    python poc_sandbox.py --project nyc-taxi-pipeline-509022
    python poc_sandbox.py --project nyc-taxi-pipeline-509022 --keep   # garde le dataset de test
"""
from __future__ import annotations

import argparse
import datetime as dt
import io
import sys
from collections.abc import Callable

import pyarrow as pa
import pyarrow.parquet as pq
from google.api_core.exceptions import GoogleAPIError
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

POC_DATASET = "poc_sandbox"
RANGE = (200901, 203001, 1)  # year_month : 2100 partitions, sous la limite de 10 000

GREEN, RED, YELLOW, RESET = "\033[32m", "\033[31m", "\033[33m", "\033[0m"


def _parquet(table: pa.Table) -> io.BytesIO:
    buf = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)
    return buf


def _rows(year_month: int, n: int) -> pa.Table:
    return pa.table(
        {
            "trip_id": pa.array(range(n), pa.int64()),
            "year_month": pa.array([year_month] * n, pa.int64()),
            "amount": pa.array([float(i) for i in range(n)], pa.float64()),
        }
    )


RANGE_SCHEMA = [
    bigquery.SchemaField("trip_id", "INT64"),
    bigquery.SchemaField("year_month", "INT64"),
    bigquery.SchemaField("amount", "FLOAT64"),
]


class Poc:
    def __init__(self, client: bigquery.Client, project: str, raw_dataset: str):
        self.c = client
        self.project = project
        self.raw_dataset = raw_dataset
        self.ds = f"{project}.{POC_DATASET}"
        self.results: dict[str, bool | None] = {}

    # -- helpers ---------------------------------------------------------------
    def q(self, sql: str) -> bigquery.table.RowIterator:
        return self.c.query(sql).result()

    def count(self, table: str) -> int:
        return next(iter(self.q(f"SELECT COUNT(*) AS n FROM `{table}`"))).n

    def load(self, dest: str, data: pa.Table, schema, disposition) -> None:
        cfg = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=disposition,
            schema=schema,
        )
        self.c.load_table_from_file(_parquet(data), dest, job_config=cfg).result()

    def run(self, key: str, title: str, fn: Callable[[], bool | None]) -> None:
        print(f"\n== {title}")
        try:
            self.results[key] = fn()
        except GoogleAPIError as exc:
            print(f"   {RED}erreur API{RESET}: {getattr(exc, 'message', exc)}")
            self.results[key] = False

    # -- tests -----------------------------------------------------------------
    def t0_existing(self) -> None:
        fq = f"{self.project}.{self.raw_dataset}.trips_clean"
        try:
            t = self.c.get_table(fq)
        except NotFound:
            print("   table absente")
            return None
        print(f"   lignes          : {t.num_rows}")
        print(f"   partitionnement : {t.time_partitioning}")
        print(f"   expire le       : {t.expires}")
        if t.time_partitioning:
            print(f"   expiration part.: {t.time_partitioning.expiration_ms} ms")
        return None

    def t1_dml(self) -> None:
        tbl = f"{self.ds}.t_dml"
        self.q(f"CREATE OR REPLACE TABLE `{tbl}` AS SELECT 1 AS x")
        try:
            self.q(f"DELETE FROM `{tbl}` WHERE x = 1")
            print(f"   {GREEN}DML autorise{RESET} (meilleur que prevu)")
        except GoogleAPIError as exc:
            print(f"   {YELLOW}DML refuse{RESET} -> confirme : pas de DELETE/MERGE")
            print(f"   detail: {getattr(exc, 'message', exc)}")
        return None

    def t2_range_truncate(self) -> bool:
        fq = f"{self.ds}.t_range"
        self.c.delete_table(fq, not_found_ok=True)
        table = bigquery.Table(fq, schema=RANGE_SCHEMA)
        table.range_partitioning = bigquery.RangePartitioning(
            field="year_month",
            range_=bigquery.PartitionRange(start=RANGE[0], end=RANGE[1], interval=RANGE[2]),
        )
        self.c.create_table(table)
        trunc = bigquery.WriteDisposition.WRITE_TRUNCATE

        steps = [
            ("charge 202401 x1000", 202401, 1000, 1000),
            ("recharge 202401 x1000 (rejeu)", 202401, 1000, 1000),
            ("charge 202402 x1000", 202402, 1000, 2000),
            ("recharge 202401 x500 (correction)", 202401, 500, 1500),
        ]
        ok = True
        for label, ym, n, expected in steps:
            self.load(f"{fq}${ym}", _rows(ym, n), RANGE_SCHEMA, trunc)
            got = self.count(fq)
            status = f"{GREEN}OK{RESET}" if got == expected else f"{RED}KO{RESET}"
            print(f"   {label:<36} total={got:<5} attendu={expected:<5} {status}")
            ok &= got == expected
        print(f"   expiration table: {self.c.get_table(fq).expires}")
        return ok

    def t3_date_partition(self) -> None:
        fq = f"{self.ds}.t_date"
        self.c.delete_table(fq, not_found_ok=True)
        schema = [bigquery.SchemaField("d", "DATE"), bigquery.SchemaField("x", "INT64")]
        table = bigquery.Table(fq, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(field="d")
        self.c.create_table(table)
        data = pa.table(
            {"d": pa.array([dt.date(2024, 1, 15)] * 100), "x": pa.array(range(100), pa.int64())}
        )
        try:
            self.load(fq, data, schema, bigquery.WriteDisposition.WRITE_APPEND)
        except GoogleAPIError as exc:
            print(f"   {YELLOW}chargement refuse{RESET}: {getattr(exc, 'message', exc)}")
            return None
        got = self.count(fq)
        if got == 0:
            print(f"   {YELLOW}0/100 lignes{RESET} -> confirme : dates > 60 j perdues")
        else:
            print(f"   {GREEN}{got}/100 lignes{RESET} -> pas d'expiration immediate")
        return None

    def t4_ctas_partitioned(self) -> bool:
        src, dst = f"{self.ds}.t_range", f"{self.ds}.t_ctas"
        self.q(
            f"""
            CREATE OR REPLACE TABLE `{dst}`
            PARTITION BY RANGE_BUCKET(year_month, GENERATE_ARRAY({RANGE[0]}, {RANGE[1]}, {RANGE[2]}))
            AS SELECT year_month, COUNT(*) AS n FROM `{src}` GROUP BY 1
            """
        )
        got = self.count(dst)
        print(f"   lignes={got} attendu=2 {'OK' if got == 2 else 'KO'}")
        return got == 2


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("--project", required=True)
    p.add_argument("--location", default="US")
    p.add_argument("--raw-dataset", default="nyc_taxi_raw")
    p.add_argument("--keep", action="store_true", help="ne pas supprimer le dataset de test")
    args = p.parse_args(argv)

    try:
        client = bigquery.Client(project=args.project, location=args.location)
    except DefaultCredentialsError:
        print("Pas d'identifiants : lancer `gcloud auth application-default login`")
        return 2

    ds = bigquery.Dataset(f"{args.project}.{POC_DATASET}")
    ds.location = args.location
    client.create_dataset(ds, exists_ok=True)

    poc = Poc(client, args.project, args.raw_dataset)
    try:
        poc.run("t0", "0. Table trips_clean existante", poc.t0_existing)
        poc.run("t1", "1. DML (DELETE)", poc.t1_dml)
        poc.run("t2", "2. WRITE_TRUNCATE par partition year_month  [DECISIF]", poc.t2_range_truncate)
        poc.run("t3", "3. Partition DATE avec dates 2024", poc.t3_date_partition)
        poc.run("t4", "4. CTAS partitionne RANGE_BUCKET (dbt table)  [DECISIF]", poc.t4_ctas_partitioned)
    finally:
        if not args.keep:
            client.delete_dataset(ds, delete_contents=True, not_found_ok=True)

    go = bool(poc.results.get("t2")) and bool(poc.results.get("t4"))
    verdict = f"{GREEN}GO BigQuery sandbox{RESET}" if go else f"{RED}NO-GO -> DuckDB{RESET}"
    print(f"\n=== VERDICT : {verdict}")
    return 0 if go else 1


if __name__ == "__main__":
    sys.exit(main())
