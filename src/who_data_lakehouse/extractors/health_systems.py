"""
WHO GHO Health Systems indicators.

Indicator codes:
- UHC_SCI_COMPONENTS: UHC service coverage index
- HWF_0001: Medical doctors (per 10,000 population)
- HWF_0006: Nursing and midwifery personnel (per 10,000 population)
- BEDS: Hospital beds (per 10,000 population)
"""
from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from who_data_lakehouse.api_client import WHOClient


INDICATORS: dict[str, str] = {
    "uhc_index": "UHC_SCI_COMPONENTS",
    "medical_doctors": "HWF_0001",
    "nursing_midwifery": "HWF_0006",
    "hospital_beds": "BEDS",
}


def extract_health_systems(
    client: "WHOClient",
    countries: list[str] | None = None,
    years: list[int] | None = None,
) -> pd.DataFrame:
    """
    Extract health systems indicators from WHO GHO.

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
