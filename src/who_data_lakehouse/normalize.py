import hashlib
import re
from typing import Iterable

import pandas as pd


GHO_DIMENSION_SLOTS = (
    ("spatial", "spatial_dim_type", "spatial_dim"),
    ("time", "time_dim_type", "time_dimension_value"),
    ("filter", "dim1_type", "dim1"),
    ("filter", "dim2_type", "dim2"),
    ("filter", "dim3_type", "dim3"),
    ("data_source", "data_source_dim_type", "data_source_dim"),
)


def snake_case(value: str) -> str:
    value = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", value)
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", value)
    value = re.sub(r"[^a-zA-Z0-9]+", "_", value)
    return value.strip("_").lower()


def normalize_columns(frame: pd.DataFrame) -> pd.DataFrame:
    renamed = frame.copy()
    renamed.columns = [snake_case(str(column)) for column in renamed.columns]
    return renamed


def parse_datetimes(frame: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    parsed = frame.copy()
    for column in columns:
        if column in parsed.columns:
            parsed[column] = pd.to_datetime(parsed[column], utc=True, errors="coerce")
    return parsed


def build_observation_id(row: pd.Series) -> str:
    keys = (
        "indicator_code",
        "spatial_dim_type",
        "spatial_dim",
        "time_dim_type",
        "time_dimension_value",
        "time_dim",
        "dim1_type",
        "dim1",
        "dim2_type",
        "dim2",
        "dim3_type",
        "dim3",
        "data_source_dim_type",
        "data_source_dim",
    )
    raw = "|".join("" if pd.isna(row.get(key)) else str(row.get(key)) for key in keys)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def normalize_gho_observations(records: list[dict]) -> tuple[pd.DataFrame, pd.DataFrame]:
    wide = normalize_columns(pd.DataFrame.from_records(records))
    if wide.empty:
        empty = pd.DataFrame(
            columns=["observation_id", "dimension_role", "dimension_type", "dimension_code"]
        )
        return wide, empty
    wide = parse_datetimes(wide, ("date", "time_dimension_begin", "time_dimension_end"))
    if "value" in wide.columns:
        wide = wide.rename(columns={"value": "value_text"})
    for column in ("numeric_value", "low", "high"):
        if column in wide.columns:
            wide[column] = pd.to_numeric(wide[column], errors="coerce")
    wide["observation_id"] = wide.apply(build_observation_id, axis=1)
    dimensions = explode_gho_dimensions(wide)
    return wide, dimensions


def explode_gho_dimensions(frame: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict] = []
    for row in frame.itertuples(index=False):
        record = row._asdict()
        observation_id = record["observation_id"]
        for dimension_role, type_column, code_column in GHO_DIMENSION_SLOTS:
            dimension_type = record.get(type_column)
            dimension_code = record.get(code_column)
            if pd.isna(dimension_type) or pd.isna(dimension_code):
                continue
            rows.append(
                {
                    "observation_id": observation_id,
                    "dimension_role": dimension_role,
                    "dimension_type": str(dimension_type),
                    "dimension_code": str(dimension_code),
                }
            )
    if not rows:
        return pd.DataFrame(
            columns=["observation_id", "dimension_role", "dimension_type", "dimension_code"]
        )
    return pd.DataFrame.from_records(rows)
