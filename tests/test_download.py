"""Tests du module de telechargement (requetes HTTP simulees)."""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

from src import download


def test_skips_existing_file(tmp_path: Path):
    target = tmp_path / "file.parquet"
    target.write_bytes(b"cached")
    with patch.object(download, "requests") as fake:
        download._download("https://example.com/f.parquet", target)
        fake.get.assert_not_called()
    assert target.read_bytes() == b"cached"


def test_downloads_and_renames(tmp_path: Path):
    target = tmp_path / "file.parquet"
    response = MagicMock()
    response.iter_content.return_value = [b"abc", b"def"]
    response.__enter__.return_value = response
    with patch.object(download.requests, "get", return_value=response):
        download._download("https://example.com/f.parquet", target)
    assert target.read_bytes() == b"abcdef"
    assert not list(tmp_path.glob("tmp*"))
