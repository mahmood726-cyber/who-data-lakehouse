"""
WHO GHO Risk Factor indicators.

Indicator codes:
- M_Est_smk_curr_std: Age-standardized current tobacco smoking prevalence
- SA_0000001462: Total alcohol per capita (15+) consumption (litres of pure alcohol)
- NCD_BMI_30A: Obesity prevalence (BMI >= 30), age-standardized
- BP_04: Raised blood pressure (SBP >= 140 or DBP >= 90), age-standardized
"""
from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from who_data_lakehouse.api_client import WHOClient


INDICATORS: dict[str, str] = {
    "tobacco_smoking": "M_Est_smk_curr_std",
    "alcohol_consumption": "SA_0000001462",
    "obesity": "NCD_BMI_30A",
    "raised_blood_pressure": "BP_04",
}


def extract_risk_factors(
    client: "WHOClient",
    countries: list[str] | None = None,
    years: list[int] | None = None,
) -> pd.DataFrame:
    """
    Extract risk factor indicators from WHO GHO.

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
