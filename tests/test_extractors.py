"""Tests for domain extractors (mock-based, no network)."""
from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd
import pytest

from who_data_lakehouse.extractors.mortality import INDICATORS as MORT_INDICATORS
from who_data_lakehouse.extractors.mortality import extract_mortality
from who_data_lakehouse.extractors.morbidity import INDICATORS as MORB_INDICATORS
from who_data_lakehouse.extractors.morbidity import extract_morbidity
from who_data_lakehouse.extractors.risk_factors import INDICATORS as RISK_INDICATORS
from who_data_lakehouse.extractors.risk_factors import extract_risk_factors
from who_data_lakehouse.extractors.health_systems import INDICATORS as HS_INDICATORS
from who_data_lakehouse.extractors.health_systems import extract_health_systems
from who_data_lakehouse.extractors.expenditure import INDICATORS as EXP_INDICATORS
from who_data_lakehouse.extractors.expenditure import extract_expenditure
from who_data_lakehouse.extractors.immunization import INDICATORS as IMM_INDICATORS
from who_data_lakehouse.extractors.immunization import extract_immunization


EXPECTED_COLUMNS = {"country_iso3", "year", "indicator", "value", "sex", "data_source"}


def _make_mock_client(indicator_data: list[dict] | None = None):
    """Create a mock WHOClient that returns test data for any indicator."""
    if indicator_data is None:
        indicator_data = [
            {"SpatialDim": "GBR", "TimeDim": 2020, "NumericValue": 81.3, "Dim1": "BTSX"},
            {"SpatialDim": "USA", "TimeDim": 2020, "NumericValue": 78.9, "Dim1": "BTSX"},
            {"SpatialDim": "PAK", "TimeDim": 2019, "NumericValue": 67.1, "Dim1": "MLE"},
        ]
    client = MagicMock()
    client.get_indicator_data.return_value = indicator_data
    return client


class TestMortalityExtractor:
    def test_returns_dataframe_with_expected_columns(self):
        client = _make_mock_client()
        df = extract_mortality(client)
        assert isinstance(df, pd.DataFrame)
        assert EXPECTED_COLUMNS.issubset(df.columns)

    def test_life_expectancy_indicator_present(self):
        client = _make_mock_client()
        df = extract_mortality(client)
        assert "life_expectancy" in df["indicator"].values

    def test_has_known_indicator_codes(self):
        assert "life_expectancy" in MORT_INDICATORS
        assert "under5_mortality" in MORT_INDICATORS


class TestMorbidityExtractor:
    def test_tb_indicator_present(self):
        client = _make_mock_client()
        df = extract_morbidity(client)
        assert "tb_incidence" in df["indicator"].values

    def test_has_known_indicator_codes(self):
        assert "tb_incidence" in MORB_INDICATORS
        assert "hiv_prevalence" in MORB_INDICATORS


class TestRiskFactorsExtractor:
    def test_tobacco_indicator_present(self):
        client = _make_mock_client()
        df = extract_risk_factors(client)
        assert "tobacco_smoking" in df["indicator"].values

    def test_has_known_indicator_codes(self):
        assert "tobacco_smoking" in RISK_INDICATORS
        assert "raised_blood_pressure" in RISK_INDICATORS


class TestHealthSystemsExtractor:
    def test_uhc_indicator_present(self):
        client = _make_mock_client()
        df = extract_health_systems(client)
        assert "uhc_index" in df["indicator"].values

    def test_has_known_indicator_codes(self):
        assert "uhc_index" in HS_INDICATORS
        assert "hospital_beds" in HS_INDICATORS


class TestExpenditureExtractor:
    def test_ghed_indicator_present(self):
        client = _make_mock_client()
        df = extract_expenditure(client)
        assert "che_gdp_pct" in df["indicator"].values

    def test_data_source_is_ghed(self):
        client = _make_mock_client()
        df = extract_expenditure(client)
        assert all(ds == "WHO_GHED" for ds in df["data_source"])


class TestImmunizationExtractor:
    def test_dtp3_indicator_present(self):
        client = _make_mock_client()
        df = extract_immunization(client)
        assert "dtp3_coverage" in df["indicator"].values

    def test_has_known_indicator_codes(self):
        assert "dtp3_coverage" in IMM_INDICATORS
        assert "bcg_coverage" in IMM_INDICATORS


class TestFiltering:
    def test_country_filter(self):
        client = _make_mock_client()
        df = extract_mortality(client, countries=["GBR"])
        assert set(df["country_iso3"].unique()) == {"GBR"}

    def test_year_filter(self):
        client = _make_mock_client()
        df = extract_mortality(client, years=[2019])
        assert set(df["year"].unique()) == {2019}

    def test_combined_filters(self):
        client = _make_mock_client()
        df = extract_mortality(client, countries=["PAK"], years=[2019])
        assert len(df) > 0
        assert all(df["country_iso3"] == "PAK")
        assert all(df["year"] == 2019)

    def test_no_match_returns_empty(self):
        client = _make_mock_client()
        df = extract_mortality(client, countries=["ZZZ"])
        assert len(df) == 0

    def test_null_numeric_skipped(self):
        data = [
            {"SpatialDim": "GBR", "TimeDim": 2020, "NumericValue": None, "Dim1": "BTSX"},
            {"SpatialDim": "USA", "TimeDim": 2020, "NumericValue": 78.9, "Dim1": "BTSX"},
        ]
        client = _make_mock_client(data)
        df = extract_mortality(client)
        # Only USA should appear (GBR has None value)
        gbr_rows = df[df["country_iso3"] == "GBR"]
        usa_rows = df[df["country_iso3"] == "USA"]
        assert len(gbr_rows) == 0
        assert len(usa_rows) > 0
