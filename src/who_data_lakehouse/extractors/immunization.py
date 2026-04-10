"""
WHO GHO Immunization indicators.

Indicator codes:
- WHS4_100: DTP3 immunization coverage (% of one-year-olds)
- WHS4_117: MCV1 immunization coverage (% of one-year-olds)
- WHS8_110: BCG immunization coverage (% of one-year-olds)
"""
from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from who_data_lakehouse.api_client import WHOClient


INDICATORS: dict[str, str] = {
    "dtp3_coverage": "WHS4_100",
    "mcv1_coverage": "WHS4_117",
    "bcg_coverage": "WHS8_110",
}


def extract_immunization(
    client: "WHOClient",
    countries: list[str] | None = None,
    years: list[int] | None = None,
) -> pd.DataFrame:
    """
    Extract immunization coverage indicators from WHO GHO.

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
                "data_source": "WHO_GHO",
            })
    return pd.DataFrame(records)
