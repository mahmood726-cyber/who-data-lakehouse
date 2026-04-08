"""Promote raw HIDR XLSX datasets to silver parquet."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from who_data_lakehouse.normalize import normalize_columns


def promote_hidr(
    datasets_dir: Path,
    silver_dir: Path,
    skip_existing: bool = False,
) -> list[dict]:
    """Convert each HIDR XLSX in datasets_dir to a silver parquet file."""
    silver_dir.mkdir(parents=True, exist_ok=True)
    results = []

    for xlsx_path in sorted(datasets_dir.glob("*.xlsx")):
        stem = xlsx_path.stem
        parquet_path = silver_dir / f"{stem}.parquet"

        if skip_existing and parquet_path.exists():
            results.append({"file": xlsx_path.name, "status": "skipped", "rows": None, "path": str(parquet_path)})
            continue

        try:
            df = pd.read_excel(xlsx_path, engine="openpyxl")
        except Exception as exc:
            results.append({"file": xlsx_path.name, "status": "error", "rows": 0, "error": str(exc)})
            continue

        if df.empty:
            results.append({"file": xlsx_path.name, "status": "empty", "rows": 0, "path": None})
            continue

        df = normalize_columns(df)
        df.to_parquet(parquet_path, index=False)
        results.append({"file": xlsx_path.name, "status": "promoted", "rows": len(df), "path": str(parquet_path)})

    return results
