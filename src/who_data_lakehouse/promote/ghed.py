"""Promote raw GHED XLSX to silver parquet."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from who_data_lakehouse.normalize import normalize_columns

KNOWN_SHEETS = {
    "Data": "ghed_data.parquet",
    "Codebook": "ghed_codebook.parquet",
    "Metadata": "ghed_metadata.parquet",
}


def promote_ghed(
    xlsx_path: Path,
    silver_dir: Path,
    skip_existing: bool = False,
) -> dict:
    """Convert GHED XLSX sheets to silver parquet files."""
    silver_dir.mkdir(parents=True, exist_ok=True)
    main_out = silver_dir / "ghed_data.parquet"

    if skip_existing and main_out.exists():
        return {"file": xlsx_path.name, "status": "skipped", "sheets": []}

    xls = pd.ExcelFile(xlsx_path, engine="openpyxl")
    promoted_sheets = []

    for sheet_name, parquet_name in KNOWN_SHEETS.items():
        if sheet_name not in xls.sheet_names:
            continue
        df = pd.read_excel(xls, sheet_name=sheet_name)
        df = normalize_columns(df)
        out_path = silver_dir / parquet_name
        df.to_parquet(out_path, index=False)
        promoted_sheets.append({"sheet": sheet_name, "rows": len(df), "path": str(out_path)})

    for sheet_name in xls.sheet_names:
        if sheet_name in KNOWN_SHEETS or sheet_name.lower() in ("version",):
            continue
        df = pd.read_excel(xls, sheet_name=sheet_name)
        if df.empty:
            continue
        df = normalize_columns(df)
        safe_name = sheet_name.lower().replace(" ", "_").replace("-", "_")
        out_path = silver_dir / f"ghed_{safe_name}.parquet"
        df.to_parquet(out_path, index=False)
        promoted_sheets.append({"sheet": sheet_name, "rows": len(df), "path": str(out_path)})

    return {"file": xlsx_path.name, "status": "promoted", "sheets": promoted_sheets}
