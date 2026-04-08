"""Tests for HIDR XLSX → silver parquet promotion."""
from pathlib import Path
import pandas as pd
import pytest

from who_data_lakehouse.promote.hidr import promote_hidr


@pytest.fixture
def tmp_raw(tmp_path):
    raw_dir = tmp_path / "raw" / "hidr" / "datasets"
    raw_dir.mkdir(parents=True)
    df = pd.DataFrame({
        "setting": ["Kenya", "Kenya"],
        "date": ["2020", "2021"],
        "source": ["DHS", "DHS"],
        "indicator_abbr": ["IMR", "IMR"],
        "indicator_name": ["Infant mortality rate", "Infant mortality rate"],
        "dimension": ["Total", "Total"],
        "subgroup": ["Total", "Total"],
        "estimate": [30.5, 28.1],
        "se": [1.2, 1.1],
        "ci_lb": [28.1, 25.9],
        "ci_ub": [32.9, 30.3],
        "population": [5000000, 5100000],
        "iso3": ["KEN", "KEN"],
        "dataset_id": ["rep_dhs_ahn", "rep_dhs_ahn"],
    })
    df.to_excel(raw_dir / "rep_dhs_ahn.xlsx", index=False, engine="openpyxl")
    df2 = df.copy()
    df2["dataset_id"] = "rep_covid_cfr"
    df2.to_excel(raw_dir / "rep_covid_cfr.xlsx", index=False, engine="openpyxl")
    return tmp_path


@pytest.fixture
def tmp_silver(tmp_path):
    return tmp_path / "silver" / "hidr"


def test_promote_hidr_creates_parquets(tmp_raw, tmp_silver):
    results = promote_hidr(tmp_raw / "raw" / "hidr" / "datasets", tmp_silver)
    assert len(results) == 2
    assert all(r["status"] == "promoted" for r in results)
    assert (tmp_silver / "rep_dhs_ahn.parquet").exists()
    assert (tmp_silver / "rep_covid_cfr.parquet").exists()


def test_promote_hidr_normalizes_columns(tmp_raw, tmp_silver):
    promote_hidr(tmp_raw / "raw" / "hidr" / "datasets", tmp_silver)
    df = pd.read_parquet(tmp_silver / "rep_dhs_ahn.parquet")
    assert "indicator_name" in df.columns
    assert "iso3" in df.columns
    assert len(df) == 2


def test_promote_hidr_skips_existing(tmp_raw, tmp_silver):
    promote_hidr(tmp_raw / "raw" / "hidr" / "datasets", tmp_silver)
    results = promote_hidr(tmp_raw / "raw" / "hidr" / "datasets", tmp_silver, skip_existing=True)
    assert all(r["status"] == "skipped" for r in results)
