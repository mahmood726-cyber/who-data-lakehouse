"""Promote raw GHO facts JSONL.gz to silver parquet (observations_wide + observation_dimensions)."""
from __future__ import annotations

import gzip
import json
from pathlib import Path

import pandas as pd

from who_data_lakehouse.normalize import normalize_gho_observations


def promote_gho_facts(
    facts_dir: Path,
    silver_dir: Path,
    skip_existing: bool = False,
) -> list[dict]:
    """Convert each indicator JSONL.gz file to observations_wide + observation_dimensions parquet."""
    wide_dir = silver_dir / "observations_wide"
    dims_dir = silver_dir / "observation_dimensions"
    wide_dir.mkdir(parents=True, exist_ok=True)
    dims_dir.mkdir(parents=True, exist_ok=True)

    results = []

    for gz_path in sorted(facts_dir.glob("*.jsonl.gz")):
        indicator_code = gz_path.stem.replace(".jsonl", "")
        wide_path = wide_dir / f"{indicator_code}.parquet"
        dims_path = dims_dir / f"{indicator_code}.parquet"

        if skip_existing and wide_path.exists() and dims_path.exists():
            results.append({
                "file": gz_path.name, "status": "skipped",
                "rows": None, "indicator_code": indicator_code,
            })
            continue

        records = []
        with gzip.open(gz_path, "rt", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    records.append(json.loads(line))

        if not records:
            results.append({
                "file": gz_path.name, "status": "empty",
                "rows": 0, "indicator_code": indicator_code,
            })
            continue

        wide_frame, dims_frame = normalize_gho_observations(records)
        wide_frame.to_parquet(wide_path, index=False)
        dims_frame.to_parquet(dims_path, index=False)

        results.append({
            "file": gz_path.name, "status": "promoted",
            "rows": len(wide_frame), "indicator_code": indicator_code,
        })

    return results
