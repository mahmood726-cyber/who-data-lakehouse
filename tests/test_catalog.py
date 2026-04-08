"""Tests for master catalog builder."""
from pathlib import Path
import pandas as pd
import pytest

from who_data_lakehouse.catalog import build_catalog


@pytest.fixture
def silver_tree(tmp_path):
    """Create a minimal silver directory tree with parquet files from multiple sources."""
    silver = tmp_path / "silver"

    # COVID
    covid_dir = silver / "covid"
    covid_dir.mkdir(parents=True)
    pd.DataFrame({
        "date_reported": pd.to_datetime(["2020-03-01", "2024-12-31"]),
        "country_code": ["KEN", "GHA"],
        "country": ["Kenya", "Ghana"],
        "new_cases": [10, 20],
    }).to_parquet(covid_dir / "WHO-COVID-19-global-data.parquet", index=False)

    # GHO observations_wide
    gho_wide = silver / "gho" / "observations_wide"
    gho_wide.mkdir(parents=True)
    pd.DataFrame({
        "indicator_code": ["WHOSIS_000001"] * 2,
        "spatial_dim": ["KEN", "GHA"],
        "numeric_value": [64.8, 63.5],
    }).to_parquet(gho_wide / "WHOSIS_000001.parquet", index=False)

    # HIDR
    hidr_dir = silver / "hidr"
    hidr_dir.mkdir(parents=True)
    pd.DataFrame({
        "setting": ["Kenya"],
        "indicator_name": ["Infant mortality rate"],
        "estimate": [30.5],
        "iso3": ["KEN"],
        "dataset_id": ["rep_dhs_ahn"],
    }).to_parquet(hidr_dir / "rep_dhs_ahn.parquet", index=False)

    # GHED
    ghed_dir = silver / "ghed"
    ghed_dir.mkdir(parents=True)
    pd.DataFrame({
        "location": ["Kenya"],
        "code": ["KEN"],
        "year": [2020],
        "che_gdp": [5.2],
    }).to_parquet(ghed_dir / "ghed_data.parquet", index=False)

    # XMart
    xmart_dir = silver / "xmart" / "mncah" / "BD_EUROCAT"
    xmart_dir.mkdir(parents=True)
    pd.DataFrame({
        "birth_year": [2020],
        "total_cases": [100],
        "status": ["active"],
    }).to_parquet(xmart_dir / "part-00000.parquet", index=False)

    return silver


def test_build_catalog_returns_dataframe(silver_tree):
    catalog = build_catalog(silver_tree)
    assert isinstance(catalog, pd.DataFrame)
    assert len(catalog) >= 5


def test_catalog_has_required_columns(silver_tree):
    catalog = build_catalog(silver_tree)
    required = ["source", "dataset", "description", "rows", "columns", "silver_path"]
    for col in required:
        assert col in catalog.columns, f"Missing column: {col}"


def test_catalog_row_counts_correct(silver_tree):
    catalog = build_catalog(silver_tree)
    covid_row = catalog[catalog["dataset"] == "WHO-COVID-19-global-data"].iloc[0]
    assert covid_row["rows"] == 2
    assert covid_row["source"] == "covid"


def test_catalog_saves_to_parquet(silver_tree, tmp_path):
    catalog = build_catalog(silver_tree, output_path=tmp_path / "catalog.parquet")
    assert (tmp_path / "catalog.parquet").exists()
    loaded = pd.read_parquet(tmp_path / "catalog.parquet")
    assert len(loaded) == len(catalog)
