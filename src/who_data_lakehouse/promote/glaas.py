"""Promote raw GLAAS XLSX to silver parquet (one file per sheet)."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from who_data_lakehouse.normalize import normalize_columns


def promote_glaas(
    xlsx_path: Path,
    silver_dir: Path,
    skip_existing: bool = False,
) -> dict:
    """Convert each sheet in the GLAAS workbook to a silver parquet file."""
    silver_dir.mkdir(parents=True, exist_ok=True)

    if skip_existing and any(silver_dir.glob("*.parquet")):
        return {"file": xlsx_path.name, "status": "skipped", "sheets": []}

    xls = pd.ExcelFile(xlsx_path, engine="openpyxl")
    promoted = []

    for sheet_name in xls.sheet_names:
        df = pd.read_excel(xls, sheet_name=sheet_name)
        if df.empty:
            continue
        df = normalize_columns(df)
        # Coerce mixed-type object columns to string for parquet compatibility
        for col in df.columns:
            if df[col].dtype == "object":
                df[col] = df[col].astype(str).replace("nan", pd.NA).replace("None", pd.NA)
        safe = sheet_name.lower().strip().replace(" ", "_").replace("-", "_").replace("/", "_")
        out_path = silver_dir / f"{safe}.parquet"
        df.to_parquet(out_path, index=False)
        promoted.append({"sheet": sheet_name, "rows": len(df), "path": str(out_path)})

    return {"file": xlsx_path.name, "status": "promoted", "sheets": promoted}
