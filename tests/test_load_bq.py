"""Tests des helpers de chargement BigQuery (sans appel reseau)."""
from __future__ import annotations

import json
from pathlib import Path

import pytest
from google.cloud import bigquery

from src.load_bq import expected_rows, partition_target, write_dispositions


def test_partition_target_uses_year_month_decorator():
    assert partition_target("p.d.trips_clean", 2024, 1) == "p.d.trips_clean$202401"
    assert partition_target("p.d.trips_clean", 2023, 12) == "p.d.trips_clean$202312"


@pytest.mark.parametrize("n_files", [1, 2, 4])
def test_first_file_truncates_then_appends(n_files):
    dispositions = write_dispositions(n_files)
    assert len(dispositions) == n_files
    assert dispositions[0] == bigquery.WriteDisposition.WRITE_TRUNCATE
    assert all(d == bigquery.WriteDisposition.WRITE_APPEND for d in dispositions[1:])


def test_expected_rows_reads_quality_report(tmp_path: Path):
    (tmp_path / "2024-01.json").write_text(json.dumps({"rows_out": 1234}))
    assert expected_rows(2024, 1, quality_dir=tmp_path) == 1234


def test_expected_rows_missing_report(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        expected_rows(2024, 1, quality_dir=tmp_path)
