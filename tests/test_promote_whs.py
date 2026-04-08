"""Tests for WHS XLSX → silver parquet promotion."""
from pathlib import Path
import pandas as pd
import pytest

from who_data_lakehouse.promote.whs import promote_whs


@pytest.fixture
def tmp_raw(tmp_path):
    raw_dir = tmp_path / "raw" / "world_health_statistics"
    raw_dir.mkdir(parents=True)
    df = pd.DataFrame({
        "IND_NAME": ["Life expectancy", "Life expectancy"],
        "DIM_GEO_NAME": ["Afghanistan", "Albania"],
        "IND_CODE": ["WHOSIS_000001", "WHOSIS_000001"],
        "DIM_GEO_CODE": ["AFG", "ALB"],
        "DIM_TIME_YEAR": [2020, 2020],
        "DIM_1_CODE": [None, None],
        "VALUE_NUMERIC": [64.8, 78.6],
        "VALUE_STRING": [None, None],
        "VALUE_COMMENTS": [None, None],
    })
    with pd.ExcelWriter(raw_dir / "web_download.xlsx", engine="openpyxl") as w:
        pd.DataFrame({"info": ["readme"]}).to_excel(w, sheet_name="readme", index=False)
        df.to_excel(w, sheet_name="data", index=False)
    return tmp_path


@pytest.fixture
def tmp_silver(tmp_path):
    return tmp_path / "silver" / "world_health_statistics"


def test_promote_whs_creates_parquet(tmp_raw, tmp_silver):
    results = promote_whs(tmp_raw / "raw" / "world_health_statistics", tmp_silver)
    assert len(results) >= 1
    assert results[0]["status"] == "promoted"
    parquet = tmp_silver / "web_download.parquet"
    assert parquet.exists()
    df = pd.read_parquet(parquet)
    assert len(df) == 2
    assert "ind_name" in df.columns
    assert "dim_geo_code" in df.columns


def test_promote_whs_skips_readme_sheets(tmp_raw, tmp_silver):
    promote_whs(tmp_raw / "raw" / "world_health_statistics", tmp_silver)
    files = list(tmp_silver.glob("*.parquet"))
    names = [f.stem for f in files]
    assert "web_download" in names
    assert "web_download_readme" not in names
