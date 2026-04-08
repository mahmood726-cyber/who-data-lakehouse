"""Tests for the search CLI functionality."""
from pathlib import Path
import pandas as pd
import pytest

from who_data_lakehouse.catalog import build_catalog, search_catalog


@pytest.fixture
def sample_catalog(tmp_path):
    silver = tmp_path / "silver"
    covid_dir = silver / "covid"
    covid_dir.mkdir(parents=True)
    pd.DataFrame({
        "date_reported": pd.to_datetime(["2020-03-01"]),
        "country": ["Kenya"],
        "new_cases": [10],
    }).to_parquet(covid_dir / "WHO-COVID-19-global-data.parquet", index=False)

    hidr_dir = silver / "hidr"
    hidr_dir.mkdir(parents=True)
    pd.DataFrame({
        "setting": ["Kenya"],
        "indicator_name": ["Tuberculosis incidence"],
        "estimate": [250.0],
        "iso3": ["KEN"],
    }).to_parquet(hidr_dir / "rep_tb_incidence.parquet", index=False)

    ghed_dir = silver / "ghed"
    ghed_dir.mkdir(parents=True)
    pd.DataFrame({
        "location": ["Kenya"],
        "code": ["KEN"],
        "year": [2020],
        "che_gdp": [5.2],
    }).to_parquet(ghed_dir / "ghed_data.parquet", index=False)

    catalog = build_catalog(silver)
    return catalog


def test_search_by_keyword(sample_catalog):
    results = search_catalog(sample_catalog, keyword="covid")
    assert len(results) >= 1
    assert all("covid" in r.lower() for r in results["dataset"].tolist() + results["description"].tolist())


def test_search_by_source(sample_catalog):
    results = search_catalog(sample_catalog, source="hidr")
    assert len(results) >= 1
    assert all(r == "hidr" for r in results["source"])


def test_search_no_match(sample_catalog):
    results = search_catalog(sample_catalog, keyword="nonexistent_xyz")
    assert len(results) == 0


def test_search_case_insensitive(sample_catalog):
    results = search_catalog(sample_catalog, keyword="COVID")
    assert len(results) >= 1
