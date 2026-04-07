import gzip
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def write_json(payload: object, destination: Path) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return destination


def write_jsonl(records: list[dict], destination: Path) -> Path:
    return append_jsonl(records, destination, append=False)


def append_jsonl(records: Iterable[dict], destination: Path, append: bool = True) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    lines = "\n".join(json.dumps(record, ensure_ascii=False) for record in records)
    if destination.suffix == ".gz":
        mode = "at" if append else "wt"
        with gzip.open(destination, mode, encoding="utf-8") as handle:
            handle.write(lines)
            if lines:
                handle.write("\n")
    else:
        mode = "a" if append else "w"
        with destination.open(mode, encoding="utf-8") as handle:
            handle.write(lines)
            if lines:
                handle.write("\n")
    return destination


def write_dataframe(frame: pd.DataFrame, destination: Path, index: bool = False) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.suffix == ".parquet":
        frame.to_parquet(destination, index=index)
    elif destination.suffix == ".csv":
        frame.to_csv(destination, index=index)
    else:
        raise ValueError(f"Unsupported tabular output: {destination}")
    return destination


def read_table(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix == ".tsv":
        return pd.read_csv(path, sep="\t")
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path)
    raise ValueError(f"Unsupported table format: {path}")
