"""Master catalog builder: scans silver layer and builds a unified dataset index."""
from __future__ import annotations

from pathlib import Path

import pandas as pd


SOURCE_RULES = {
    "covid": {"pattern": "*.parquet", "depth": "flat"},
    "ghed": {"pattern": "*.parquet", "depth": "flat"},
    "glaas": {"pattern": "*.parquet", "depth": "flat"},
    "world_health_statistics": {"pattern": "*.parquet", "depth": "flat"},
    "hidr": {"pattern": "*.parquet", "depth": "flat"},
    "datadot": {"pattern": "*.parquet", "depth": "flat"},
}

DESCRIPTION_HINTS = {
    "covid": "WHO COVID-19 surveillance data",
    "ghed": "Global Health Expenditure Database",
    "glaas": "GLAAS water/sanitation/hygiene survey",
    "world_health_statistics": "World Health Statistics annex data",
    "hidr": "Health Inequality Data Repository",
    "datadot": "WHO data.who.int portal metadata",
    "gho": "Global Health Observatory indicator",
    "xmart": "WHO XMart OData service",
}


def _scan_flat_source(source: str, source_dir: Path) -> list[dict]:
    rows = []
    for pq in sorted(source_dir.glob("*.parquet")):
        df = pd.read_parquet(pq)
        rows.append({
            "source": source,
            "dataset": pq.stem,
            "description": f"{DESCRIPTION_HINTS.get(source, source)}: {pq.stem}",
            "rows": len(df),
            "columns": len(df.columns),
            "column_names": ", ".join(df.columns[:20]),
            "silver_path": str(pq),
        })
    return rows


def _scan_gho(source_dir: Path) -> list[dict]:
    rows = []
    for pq in sorted(source_dir.glob("*.parquet")):
        df = pd.read_parquet(pq)
        rows.append({
            "source": "gho",
            "dataset": pq.stem,
            "description": f"GHO reference table: {pq.stem}",
            "rows": len(df),
            "columns": len(df.columns),
            "column_names": ", ".join(df.columns[:20]),
            "silver_path": str(pq),
        })
    wide_dir = source_dir / "observations_wide"
    if wide_dir.exists():
        for pq in sorted(wide_dir.glob("*.parquet")):
            df = pd.read_parquet(pq)
            if df.empty:
                continue
            rows.append({
                "source": "gho",
                "dataset": f"indicator/{pq.stem}",
                "description": f"GHO indicator: {pq.stem}",
                "rows": len(df),
                "columns": len(df.columns),
                "column_names": ", ".join(df.columns[:20]),
                "silver_path": str(pq),
            })
    return rows


def _scan_xmart(source_dir: Path) -> list[dict]:
    rows = []
    for service_dir in sorted(source_dir.iterdir()):
        if not service_dir.is_dir():
            continue
        service = service_dir.name
        for entity_dir in sorted(service_dir.iterdir()):
            if not entity_dir.is_dir():
                continue
            parts = sorted(entity_dir.glob("part-*.parquet"))
            if not parts:
                continue
            total_rows = 0
            col_names = ""
            n_cols = 0
            for part in parts:
                df = pd.read_parquet(part)
                total_rows += len(df)
                if not col_names:
                    col_names = ", ".join(df.columns[:20])
                    n_cols = len(df.columns)
            if total_rows == 0:
                continue
            rows.append({
                "source": f"xmart/{service}",
                "dataset": entity_dir.name,
                "description": f"XMart {service}: {entity_dir.name}",
                "rows": total_rows,
                "columns": n_cols,
                "column_names": col_names,
                "silver_path": str(entity_dir),
            })
    return rows


def build_catalog(
    silver_dir: Path,
    output_path: Path | None = None,
) -> pd.DataFrame:
    """Build a master catalog of all datasets in the silver layer."""
    all_rows: list[dict] = []

    for child in sorted(silver_dir.iterdir()):
        if not child.is_dir():
            continue
        name = child.name

        if name == "gho":
            all_rows.extend(_scan_gho(child))
        elif name == "xmart":
            all_rows.extend(_scan_xmart(child))
        elif name in SOURCE_RULES:
            all_rows.extend(_scan_flat_source(name, child))
        else:
            all_rows.extend(_scan_flat_source(name, child))

    catalog = pd.DataFrame.from_records(all_rows)
    if catalog.empty:
        catalog = pd.DataFrame(columns=["source", "dataset", "description", "rows", "columns", "column_names", "silver_path"])

    catalog = catalog.sort_values(["source", "dataset"]).reset_index(drop=True)

    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        catalog.to_parquet(output_path, index=False)

    return catalog


def search_catalog(
    catalog: pd.DataFrame,
    keyword: str | None = None,
    source: str | None = None,
) -> pd.DataFrame:
    """Filter the catalog by keyword (searches dataset + description + column_names) and/or source."""
    mask = pd.Series(True, index=catalog.index)

    if source is not None:
        mask &= catalog["source"].str.lower() == source.lower()

    if keyword is not None:
        kw = keyword.lower()
        text_match = (
            catalog["dataset"].str.lower().str.contains(kw, na=False)
            | catalog["description"].str.lower().str.contains(kw, na=False)
            | catalog["column_names"].str.lower().str.contains(kw, na=False)
        )
        mask &= text_match

    return catalog.loc[mask].reset_index(drop=True)
