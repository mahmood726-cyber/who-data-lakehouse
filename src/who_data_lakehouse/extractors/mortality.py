"""
WHO GHO Mortality indicators.

Indicator codes:
- WHOSIS_000001: Life expectancy at birth (both sexes)
- MDG_0000000001: Under-5 mortality rate (per 1000 live births)
- MORT_MATERNALNUM: Maternal mortality ratio (per 100,000 live births)
- NEONATMR: Neonatal mortality rate (per 1000 live births)
"""
from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from who_data_lakehouse.api_client import WHOClient


INDICATORS: dict[str, str] = {
    "life_expectancy": "WHOSIS_000001",
    "under5_mortality": "MDG_0000000001",
    "maternal_mortality": "MORT_MATERNALNUM",
    "neonatal_mortality": "NEONATMR",
}


def extract_mortality(
    client: "WHOClient",
    countries: list[str] | None = None,
    years: list[int] | None = None,
) -> pd.DataFrame:
    """
    Extract mortality indicators from WHO GHO.

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
