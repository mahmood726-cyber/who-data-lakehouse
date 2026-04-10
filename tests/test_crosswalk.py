"""Tests for country code cross-walk (no network required)."""
from __future__ import annotations

import pandas as pd
import pytest

from who_data_lakehouse.crosswalk import (
    CROSSWALK,
    enrich_dataframe,
    get_all_iso3_codes,
    iso3_to_ihme,
    iso3_to_name,
    iso3_to_wb,
    who_to_iso3,
)


class TestWhoToIso3:
    def test_gbr_maps_to_gbr(self):
        assert who_to_iso3("GBR") == "GBR"

    def test_usa_maps_to_usa(self):
        assert who_to_iso3("USA") == "USA"

    def test_unknown_returns_none(self):
        assert who_to_iso3("XYZ_FAKE") is None


class TestIso3ToIhme:
    def test_usa_maps_to_102(self):
        assert iso3_to_ihme("USA") == 102

    def test_gbr_maps_to_95(self):
        assert iso3_to_ihme("GBR") == 95

    def test_pak_maps_to_165(self):
        assert iso3_to_ihme("PAK") == 165

    def test_unknown_returns_none(self):
        assert iso3_to_ihme("XYZ_FAKE") is None


class TestIso3ToWb:
    def test_chn_maps_to_chn(self):
        assert iso3_to_wb("CHN") == "CHN"

    def test_ind_maps_to_ind(self):
        assert iso3_to_wb("IND") == "IND"

    def test_unknown_returns_none(self):
        assert iso3_to_wb("XYZ_FAKE") is None


class TestCrosswalkSize:
    def test_at_least_50_countries(self):
        assert len(CROSSWALK) >= 50

    def test_all_entries_have_required_keys(self):
        for code, entry in CROSSWALK.items():
            assert "iso3" in entry, f"{code} missing iso3"
            assert "name" in entry, f"{code} missing name"
            assert "ihme_id" in entry, f"{code} missing ihme_id"
            assert "wb_code" in entry, f"{code} missing wb_code"

    def test_get_all_iso3_codes_sorted(self):
        codes = get_all_iso3_codes()
        assert len(codes) >= 50
        assert codes == sorted(codes)


class TestIso3ToName:
    def test_known_country(self):
        assert iso3_to_name("JPN") == "Japan"

    def test_unknown(self):
        assert iso3_to_name("ZZZ") is None


class TestEnrichDataframe:
    def test_adds_columns(self):
        df = pd.DataFrame({
            "country_iso3": ["GBR", "USA", "CHN"],
            "value": [1.0, 2.0, 3.0],
        })
        enriched = enrich_dataframe(df)
        assert "ihme_id" in enriched.columns
        assert "wb_code" in enriched.columns
        assert "country_name" in enriched.columns
        assert enriched.loc[0, "ihme_id"] == 95
        assert enriched.loc[1, "wb_code"] == "USA"
        assert enriched.loc[2, "country_name"] == "China"

    def test_unknown_codes_get_none(self):
        df = pd.DataFrame({
            "country_iso3": ["XYZ"],
            "value": [1.0],
        })
        enriched = enrich_dataframe(df)
        assert pd.isna(enriched.loc[0, "ihme_id"])
