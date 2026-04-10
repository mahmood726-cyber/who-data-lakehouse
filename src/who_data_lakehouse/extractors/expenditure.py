"""
WHO GHO Health Expenditure indicators (via GHED codes in GHO API).

Indicator codes:
- GHED_CHEGDP_SHA2011: Current health expenditure as % of GDP
- GHED_OOPSCHECUR_SHA2011: Out-of-pocket spending as % of current health expenditure
- GHED_GGHE-DCHE_SHA2011: Domestic general government health expenditure as % of CHE
"""
from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from who_data_lakehouse.api_client import WHOClient


INDICATORS: dict[str, str] = {
    "che_gdp_pct": "GHED_CHEGDP_SHA2011",
    "oop_che_pct": "GHED_OOPSCHECUR_SHA2011",
    "gghe_che_pct": "GHED_GGHE-DCHE_SHA2011",
}


def extract_expenditure(
    client: "WHOClient",
    countries: list[str] | None = None,
    years: list[int] | None = None,
) -> pd.DataFrame:
    """
    Extract health expenditure indicators from WHO GHO (GHED series).

    Returns DataFrame with columns:
        country_iso3, year, indicator, value, sex, data_source
    """
    records: list[dict] = []
    for name, code in INDICATORS.items():
        data = client.get_indicator_data(code)
        for rec in data:
            spatial = rec.get("SpatialDim")
            time_dim = rec.get("TimeDim")
            if countries and spatial not in countries:
                continue
            if years and time_dim not in years:
                continue
            numeric = rec.get("NumericValue")
            if numeric is None:
                continue
            records.append({
                "country_iso3": spatial,
                "year": time_dim,
                "indicator": name,
                "value": numeric,
                "sex": rec.get("Dim1", "BTSX"),
                "data_source": "WHO_GHED",
            })
    return pd.DataFrame(records)
