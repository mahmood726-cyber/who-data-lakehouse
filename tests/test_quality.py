"""Tests for data quality module (no network, synthetic data)."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from who_data_lakehouse.quality import (
    check_completeness,
    check_temporal_coverage,
    detect_outliers,
)


def _make_full_grid() -> pd.DataFrame:
    """Create a complete country-year grid with no missing values."""
    countries = ["GBR", "USA", "CHN"]
    years = [2018, 2019, 2020, 2021, 2022]
    rows = []
    for c in countries:
        for y in years:
            rows.append({
                "country_iso3": c,
                "year": y,
                "indicator": "test",
                "value": 70.0 + np.random.default_rng(42).random() * 10,
                "sex": "BTSX",
                "data_source": "TEST",
            })
    return pd.DataFrame(rows)


def _make_sparse_grid() -> pd.DataFrame:
    """Create a grid with some missing values."""
    df = _make_full_grid()
    # Set some values to NaN
    df.loc[0, "value"] = np.nan
    df.loc[3, "value"] = np.nan
    df.loc[7, "value"] = np.nan
    return df


class TestCheckCompleteness:
    def test_full_grid_100_percent(self):
        df = _make_full_grid()
        pct = check_completeness(df)
        assert pct == 100.0

    def test_sparse_grid_less_than_100(self):
        df = _make_sparse_grid()
        pct = check_completeness(df)
        assert 0.0 < pct < 100.0

    def test_empty_dataframe_returns_zero(self):
        df = pd.DataFrame(columns=["country_iso3", "year", "value"])
        pct = check_completeness(df)
        assert pct == 0.0

    def test_all_null_returns_zero(self):
        df = pd.DataFrame({
            "country_iso3": ["GBR", "USA"],
            "year": [2020, 2020],
            "value": [np.nan, np.nan],
        })
        pct = check_completeness(df)
        assert pct == 0.0


class TestCheckTemporalCoverage:
    def test_correct_min_max_years(self):
        df = _make_full_grid()
        coverage = check_temporal_coverage(df)
        gbr = coverage[coverage["country_iso3"] == "GBR"].iloc[0]
        assert gbr["min_year"] == 2018
        assert gbr["max_year"] == 2022

    def test_detects_gaps(self):
        df = pd.DataFrame({
            "country_iso3": ["GBR", "GBR", "GBR"],
            "year": [2018, 2020, 2022],  # missing 2019, 2021
            "value": [70.0, 71.0, 72.0],
        })
        coverage = check_temporal_coverage(df)
        gbr = coverage[coverage["country_iso3"] == "GBR"].iloc[0]
        assert gbr["gaps"] == 2  # expected 5 years, got 3

    def test_no_gaps_for_complete_series(self):
        df = _make_full_grid()
        coverage = check_temporal_coverage(df)
        assert all(coverage["gaps"] == 0)

    def test_empty_dataframe(self):
        df = pd.DataFrame(columns=["country_iso3", "year", "value"])
        coverage = check_temporal_coverage(df)
        assert len(coverage) == 0
        assert "min_year" in coverage.columns


class TestDetectOutliers:
    def test_flags_extreme_value(self):
        rng = np.random.default_rng(42)
        values = list(rng.normal(70, 2, 20))
        values.append(200.0)  # extreme outlier
        df = pd.DataFrame({
            "country_iso3": ["GBR"] * 21,
            "year": list(range(2000, 2021)),
            "value": values,
        })
        outliers = detect_outliers(df, sigma=3.0)
        assert len(outliers) >= 1
        assert 200.0 in outliers["value"].values

    def test_no_outliers_in_clean_data(self):
        rng = np.random.default_rng(42)
        df = pd.DataFrame({
            "country_iso3": ["GBR"] * 20,
            "year": list(range(2000, 2020)),
            "value": list(rng.normal(70, 1, 20)),
        })
        outliers = detect_outliers(df, sigma=5.0)
        assert len(outliers) == 0

    def test_empty_dataframe(self):
        df = pd.DataFrame(columns=["country_iso3", "year", "value"])
        outliers = detect_outliers(df)
        assert len(outliers) == 0

    def test_single_country_single_obs_no_crash(self):
        df = pd.DataFrame({
            "country_iso3": ["GBR"],
            "year": [2020],
            "value": [70.0],
        })
        outliers = detect_outliers(df)
        assert len(outliers) == 0

    def test_z_score_column_present(self):
        rng = np.random.default_rng(42)
        values = list(rng.normal(70, 2, 20))
        values.append(200.0)
        df = pd.DataFrame({
            "country_iso3": ["GBR"] * 21,
            "year": list(range(2000, 2021)),
            "value": values,
        })
        outliers = detect_outliers(df, sigma=3.0)
        assert "z_score" in outliers.columns
