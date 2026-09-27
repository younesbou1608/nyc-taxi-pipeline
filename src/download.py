"""Telechargement des fichiers mensuels Yellow Taxi et du referentiel de zones.

Usage:
    python -m src.download --year 2024 --months 1 2 3
    python -m src.download --year 2024 --months 1 --force
"""
from __future__ import annotations

import argparse
import logging
import sys
import tempfile
from pathlib import Path

import requests

from src.config import PATHS, TLC_BASE_URL, TRIP_DATA_PATH, ZONE_LOOKUP_PATH

log = logging.getLogger(__name__)
CHUNK = 1 << 20  # 1 MiB


def _download(url: str, target: Path, force: bool = False) -> Path:
    """Telecharge `url` vers `target` de facon atomique (fichier temporaire + rename)."""
    if target.exists() and not force:
        log.info("deja present, ignore: %s", target.name)
        return target

    target.parent.mkdir(parents=True, exist_ok=True)
    log.info("telechargement %s", url)
    with requests.get(url, stream=True, timeout=180) as resp:
        resp.raise_for_status()
        with tempfile.NamedTemporaryFile(dir=target.parent, delete=False) as tmp:
            tmp_path = Path(tmp.name)
            for chunk in resp.iter_content(CHUNK):
                tmp.write(chunk)
    tmp_path.replace(target)
    log.info("ecrit %s (%.1f MB)", target.name, target.stat().st_size / 1e6)
    return target


def download_month(year: int, month: int, force: bool = False) -> Path:
    url = f"{TLC_BASE_URL}/{TRIP_DATA_PATH.format(year=year, month=month)}"
    target = PATHS.raw / f"yellow_tripdata_{year}-{month:02d}.parquet"
    return _download(url, target, force)


def download_zones(force: bool = False) -> Path:
    return _download(f"{TLC_BASE_URL}/{ZONE_LOOKUP_PATH}", PATHS.zones_csv, force)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Download NYC TLC yellow taxi data")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--months", type=int, nargs="+", required=True)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    download_zones(args.force)
    for month in args.months:
        if not 1 <= month <= 12:
            raise ValueError(f"mois invalide: {month}")
        download_month(args.year, month, args.force)
    return 0


if __name__ == "__main__":
    sys.exit(main())
