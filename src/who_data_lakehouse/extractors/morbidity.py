"""
WHO GHO Morbidity indicators.

Indicator codes:
- TB_e_inc_100k: Tuberculosis incidence (per 100,000)
- MALARIA_EST_INCIDENCE: Malaria estimated incidence (per 1000 at risk)
- HIV_0000000001: HIV prevalence among adults aged 15-49
- NCD_BMI_30A: Age-standardized NCD obesity prevalence (BMI >= 30)
"""
from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from who_data_lakehouse.api_client import WHOClient


INDICATORS: dict[str, str] = {
    "tb_incidence": "TB_e_inc_100k",
    "malaria_incidence": "MALARIA_EST_INCIDENCE",
    "hiv_prevalence": "HIV_0000000001",
    "ncd_obesity": "NCD_BMI_30A",
}


def extract_morbidity(
    client: "WHOClient",
    countries: list[str] | None = None,
    years: list[int] | None = None,
) -> pd.DataFrame:
    """
    Extract morbidity indicators from WHO GHO.

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
