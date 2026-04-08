"""Promote raw World Health Statistics XLSX to silver parquet."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from who_data_lakehouse.normalize import normalize_columns

SKIP_SHEETS = {"readme", "read me", "notes", "explanatory notes", "footnotes"}


def promote_whs(
    raw_dir: Path,
    silver_dir: Path,
    skip_existing: bool = False,
) -> list[dict]:
    """Convert each WHS XLSX in raw_dir to silver parquet."""
    silver_dir.mkdir(parents=True, exist_ok=True)
    results = []

    for xlsx_path in sorted(raw_dir.glob("*.xlsx")) + sorted(raw_dir.glob("*.xls")):
        stem = xlsx_path.stem
        main_out = silver_dir / f"{stem}.parquet"

        if skip_existing and main_out.exists():
            results.append({"file": xlsx_path.name, "status": "skipped", "rows": None})
            continue

        xls = pd.ExcelFile(xlsx_path, engine="openpyxl")
        data_sheets = [s for s in xls.sheet_names if s.lower().strip() not in SKIP_SHEETS]

        if not data_sheets:
            results.append({"file": xlsx_path.name, "status": "empty", "rows": 0})
            continue

        total_rows = 0
        for idx, sheet_name in enumerate(data_sheets):
            df = pd.read_excel(xls, sheet_name=sheet_name)
            if df.empty:
                continue
            df = normalize_columns(df)
            # Coerce mixed-type object columns to string for parquet compatibility
            for col in df.columns:
                if df[col].dtype == "object":
                    df[col] = df[col].astype(str).replace("nan", pd.NA).replace("None", pd.NA)
            if idx == 0:
                out_path = main_out
            else:
                safe = sheet_name.lower().replace(" ", "_").replace("-", "_")
                out_path = silver_dir / f"{stem}_{safe}.parquet"
            df.to_parquet(out_path, index=False)
            total_rows += len(df)

        results.append({"file": xlsx_path.name, "status": "promoted", "rows": total_rows})

    return results
