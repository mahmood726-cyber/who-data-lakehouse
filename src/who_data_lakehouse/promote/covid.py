"""Promote raw COVID CSVs to silver parquet."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from who_data_lakehouse.normalize import normalize_columns

DATE_COLUMNS = ["date_reported", "date"]


def promote_covid(
    raw_dir: Path,
    silver_dir: Path,
    skip_existing: bool = False,
) -> list[dict]:
    """Convert each CSV in raw_dir to a normalized parquet in silver_dir."""
    silver_dir.mkdir(parents=True, exist_ok=True)
    results = []

    for csv_path in sorted(raw_dir.glob("*.csv")):
        stem = csv_path.stem
        parquet_path = silver_dir / f"{stem}.parquet"

        if skip_existing and parquet_path.exists():
            results.append({"file": csv_path.name, "status": "skipped", "rows": None, "path": str(parquet_path)})
            continue

        df = pd.read_csv(csv_path, low_memory=False)
        df = normalize_columns(df)

        for col in DATE_COLUMNS:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

        df.to_parquet(parquet_path, index=False)
        results.append({"file": csv_path.name, "status": "promoted", "rows": len(df), "path": str(parquet_path)})

    return results
