"""Tests for COVID CSV → silver parquet promotion."""
from pathlib import Path
import pandas as pd
import pytest

from who_data_lakehouse.promote.covid import promote_covid


@pytest.fixture
def tmp_raw(tmp_path):
    """Create a minimal raw COVID directory with one CSV."""
    raw_dir = tmp_path / "raw" / "covid"
    raw_dir.mkdir(parents=True)
    df = pd.DataFrame({
        "Date_reported": ["2024-01-01", "2024-01-02"],
        "Country_code": ["US", "US"],
        "Country": ["United States of America", "United States of America"],
        "WHO_region": ["AMRO", "AMRO"],
        "New_cases": [100.0, 200.0],
        "Cumulative_cases": [1000, 1200],
        "New_deaths": [1.0, 2.0],
        "Cumulative_deaths": [50, 52],
    })
    df.to_csv(raw_dir / "WHO-COVID-19-global-data.csv", index=False)
    return tmp_path


@pytest.fixture
def tmp_silver(tmp_path):
    return tmp_path / "silver" / "covid"


def test_promote_covid_creates_parquet(tmp_raw, tmp_silver):
    results = promote_covid(tmp_raw / "raw" / "covid", tmp_silver)
    assert len(results) == 1
    assert results[0]["status"] == "promoted"
    parquet_path = tmp_silver / "WHO-COVID-19-global-data.parquet"
    assert parquet_path.exists()
    df = pd.read_parquet(parquet_path)
    assert len(df) == 2
    assert "date_reported" in df.columns
    assert "country_code" in df.columns


def test_promote_covid_normalizes_columns(tmp_raw, tmp_silver):
    promote_covid(tmp_raw / "raw" / "covid", tmp_silver)
    df = pd.read_parquet(tmp_silver / "WHO-COVID-19-global-data.parquet")
    for col in df.columns:
        assert col == col.lower(), f"Column {col} not lowercase"
        assert " " not in col, f"Column {col} has spaces"


def test_promote_covid_parses_dates(tmp_raw, tmp_silver):
    promote_covid(tmp_raw / "raw" / "covid", tmp_silver)
    df = pd.read_parquet(tmp_silver / "WHO-COVID-19-global-data.parquet")
    assert pd.api.types.is_datetime64_any_dtype(df["date_reported"])


def test_promote_covid_skips_existing(tmp_raw, tmp_silver):
    promote_covid(tmp_raw / "raw" / "covid", tmp_silver)
    results = promote_covid(tmp_raw / "raw" / "covid", tmp_silver, skip_existing=True)
    assert results[0]["status"] == "skipped"
