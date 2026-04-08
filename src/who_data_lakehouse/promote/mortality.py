"""Promote raw WHO Mortality Database ZIPs to silver parquet."""
from __future__ import annotations

import zipfile
from pathlib import Path

import pandas as pd

from who_data_lakehouse.normalize import normalize_columns

# Files that are documentation/PDF — skip these
SKIP_PATTERNS = ("mort_documentation",)


def promote_mortality(
    raw_dir: Path,
    silver_dir: Path,
    skip_existing: bool = False,
) -> list[dict]:
    """Extract and convert each mortality ZIP to silver parquet.

    Handles three file types inside ZIPs:
    - Extensionless CSV files (ICD data, population, country codes, notes)
    - .xlsx files (ICD10 addendum country files)
    - .xls files (availability list) — skipped if xlrd unavailable
    """
    silver_dir.mkdir(parents=True, exist_ok=True)
    results = []

    for zip_path in sorted(raw_dir.glob("*.zip")):
        stem = zip_path.stem

        # Skip documentation ZIPs
        if any(stem.startswith(pat) for pat in SKIP_PATTERNS):
            results.append({"file": zip_path.name, "status": "skipped_doc", "rows": 0})
            continue

        parquet_path = silver_dir / f"{stem}.parquet"
        if skip_existing and parquet_path.exists():
            results.append({"file": zip_path.name, "status": "skipped", "rows": None, "path": str(parquet_path)})
            continue

        try:
            frames = _extract_zip(zip_path)
        except Exception as exc:
            results.append({"file": zip_path.name, "status": "error", "rows": 0, "error": str(exc)})
            continue

        if not frames:
            results.append({"file": zip_path.name, "status": "empty", "rows": 0})
            continue

        # Concatenate if multiple files in one ZIP (e.g., morticd10_add has Germany.xlsx + Norway.xlsx)
        df = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
        df = normalize_columns(df)

        # Coerce mixed-type object columns to string for parquet compatibility
        for col in df.columns:
            if df[col].dtype == "object":
                df[col] = df[col].astype(str).replace("nan", pd.NA).replace("None", pd.NA)

        df.to_parquet(parquet_path, index=False)
        results.append({"file": zip_path.name, "status": "promoted", "rows": len(df), "path": str(parquet_path)})

    return results


def _extract_zip(zip_path: Path) -> list[pd.DataFrame]:
    """Extract all data files from a mortality ZIP, return as DataFrames."""
    frames = []
    with zipfile.ZipFile(zip_path) as z:
        for name in z.namelist():
            lower = name.lower()

            # Skip directories, PDFs, documentation
            if name.endswith("/") or lower.endswith(".pdf"):
                continue

            with z.open(name) as f:
                if lower.endswith(".xlsx"):
                    df = pd.read_excel(f, engine="openpyxl")
                elif lower.endswith(".xls"):
                    # xlrd may not be installed — try openpyxl first, fall back to CSV
                    try:
                        df = pd.read_excel(f, engine="xlrd")
                    except ImportError:
                        # Read as CSV fallback
                        f.seek(0)
                        try:
                            df = pd.read_csv(f, encoding="latin-1", low_memory=False)
                        except Exception:
                            continue
                else:
                    # Extensionless files — treat as CSV (comma-delimited)
                    df = pd.read_csv(f, encoding="latin-1", low_memory=False)

            if not df.empty:
                frames.append(df)

    return frames
