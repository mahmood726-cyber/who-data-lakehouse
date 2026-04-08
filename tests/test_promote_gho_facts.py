"""Tests for GHO facts JSONL.gz → silver parquet promotion."""
from pathlib import Path
import gzip
import json
import pandas as pd
import pytest

from who_data_lakehouse.promote.gho_facts import promote_gho_facts


@pytest.fixture
def tmp_raw(tmp_path):
    raw_dir = tmp_path / "raw" / "gho" / "facts"
    raw_dir.mkdir(parents=True)
    records = [
        {
            "Id": 1,
            "IndicatorCode": "TEST_001",
            "SpatialDimType": "COUNTRY",
            "SpatialDim": "KEN",
            "TimeDimType": "YEAR",
            "TimeDim": 2020,
            "Dim1Type": "SEX",
            "Dim1": "BTSX",
            "Dim2Type": None,
            "Dim2": None,
            "Dim3Type": None,
            "Dim3": None,
            "Value": "64.8",
            "NumericValue": 64.8,
            "Low": 62.0,
            "High": 67.0,
            "Date": "2024-01-15T00:00:00+00:00",
            "TimeDimensionValue": 2020,
            "TimeDimensionBegin": "2020-01-01",
            "TimeDimensionEnd": "2020-12-31",
            "DataSourceDimType": None,
            "DataSourceDim": None,
            "Comments": None,
            "ParentLocationCode": "AFR",
            "ParentLocation": "Africa",
        },
        {
            "Id": 2,
            "IndicatorCode": "TEST_001",
            "SpatialDimType": "COUNTRY",
            "SpatialDim": "GHA",
            "TimeDimType": "YEAR",
            "TimeDim": 2020,
            "Dim1Type": "SEX",
            "Dim1": "BTSX",
            "Dim2Type": None,
            "Dim2": None,
            "Dim3Type": None,
            "Dim3": None,
            "Value": "63.5",
            "NumericValue": 63.5,
            "Low": 61.0,
            "High": 66.0,
            "Date": "2024-01-15T00:00:00+00:00",
            "TimeDimensionValue": 2020,
            "TimeDimensionBegin": "2020-01-01",
            "TimeDimensionEnd": "2020-12-31",
            "DataSourceDimType": None,
            "DataSourceDim": None,
            "Comments": None,
            "ParentLocationCode": "AFR",
            "ParentLocation": "Africa",
        },
    ]
    path = raw_dir / "TEST_001.jsonl.gz"
    with gzip.open(path, "wt", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec) + "\n")
    return tmp_path


@pytest.fixture
def tmp_silver(tmp_path):
    return tmp_path / "silver" / "gho"


def test_promote_gho_facts_creates_wide_and_dims(tmp_raw, tmp_silver):
    results = promote_gho_facts(tmp_raw / "raw" / "gho" / "facts", tmp_silver)
    assert len(results) == 1
    assert results[0]["status"] == "promoted"
    wide_path = tmp_silver / "observations_wide" / "TEST_001.parquet"
    dims_path = tmp_silver / "observation_dimensions" / "TEST_001.parquet"
    assert wide_path.exists()
    assert dims_path.exists()
    df_wide = pd.read_parquet(wide_path)
    assert len(df_wide) == 2
    assert "observation_id" in df_wide.columns
    assert "numeric_value" in df_wide.columns


def test_promote_gho_facts_skips_existing(tmp_raw, tmp_silver):
    promote_gho_facts(tmp_raw / "raw" / "gho" / "facts", tmp_silver)
    results = promote_gho_facts(tmp_raw / "raw" / "gho" / "facts", tmp_silver, skip_existing=True)
    assert results[0]["status"] == "skipped"
