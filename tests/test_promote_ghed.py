"""Tests for GHED XLSX → silver parquet promotion."""
from pathlib import Path
import pandas as pd
import pytest

from who_data_lakehouse.promote.ghed import promote_ghed


@pytest.fixture
def tmp_raw(tmp_path):
    raw_dir = tmp_path / "raw" / "ghed"
    raw_dir.mkdir(parents=True)
    df = pd.DataFrame({
        "location": ["Afghanistan", "Albania"],
        "code": ["AFG", "ALB"],
        "region": ["EMRO", "EURO"],
        "income": ["Low", "Upper middle"],
        "year": [2020, 2020],
        "che_gdp": [10.2, 5.3],
        "gghed_che": [20.1, 55.0],
    })
    with pd.ExcelWriter(raw_dir / "GHED_data.XLSX", engine="openpyxl") as w:
        df.to_excel(w, sheet_name="Data", index=False)
        pd.DataFrame({"code": ["che_gdp"], "description": ["CHE as % GDP"]}).to_excel(
            w, sheet_name="Codebook", index=False
        )
    return tmp_path


@pytest.fixture
def tmp_silver(tmp_path):
    return tmp_path / "silver" / "ghed"


def test_promote_ghed_creates_parquet(tmp_raw, tmp_silver):
    result = promote_ghed(tmp_raw / "raw" / "ghed" / "GHED_data.XLSX", tmp_silver)
    assert result["status"] == "promoted"
    assert (tmp_silver / "ghed_data.parquet").exists()
    df = pd.read_parquet(tmp_silver / "ghed_data.parquet")
    assert len(df) == 2
    assert "location" in df.columns
    assert "che_gdp" in df.columns


def test_promote_ghed_extracts_codebook(tmp_raw, tmp_silver):
    result = promote_ghed(tmp_raw / "raw" / "ghed" / "GHED_data.XLSX", tmp_silver)
    codebook_path = tmp_silver / "ghed_codebook.parquet"
    assert codebook_path.exists()
    df = pd.read_parquet(codebook_path)
    assert len(df) == 1


def test_promote_ghed_skips_existing(tmp_raw, tmp_silver):
    promote_ghed(tmp_raw / "raw" / "ghed" / "GHED_data.XLSX", tmp_silver)
    result = promote_ghed(tmp_raw / "raw" / "ghed" / "GHED_data.XLSX", tmp_silver, skip_existing=True)
    assert result["status"] == "skipped"
