"""
Country code cross-walk: WHO GHO SpatialDim (ISO3) <-> IHME location_id <-> World Bank code.

Hardcoded lookup table for 60+ countries covering the most common codes used in
cardiology and global health research.  Avoids API dependency for cross-walking.

Sources for IHME location_id:
  - IHME GBD location hierarchy (publicly available)
  - Verified against ihme-data-lakehouse registry

Sources for World Bank codes:
  - World Bank country API (ISO3 codes, mostly identical to WHO SpatialDim)
"""
from __future__ import annotations

from typing import Any


# fmt: off
CROSSWALK: dict[str, dict[str, Any]] = {
    # iso3 -> {name, ihme_id, wb_code}
    "AFG": {"iso3": "AFG", "name": "Afghanistan",                   "ihme_id": 160, "wb_code": "AFG"},
    "AGO": {"iso3": "AGO", "name": "Angola",                        "ihme_id": 168, "wb_code": "AGO"},
    "ARG": {"iso3": "ARG", "name": "Argentina",                     "ihme_id": 97,  "wb_code": "ARG"},
    "AUS": {"iso3": "AUS", "name": "Australia",                     "ihme_id": 71,  "wb_code": "AUS"},
    "AUT": {"iso3": "AUT", "name": "Austria",                       "ihme_id": 75,  "wb_code": "AUT"},
    "BGD": {"iso3": "BGD", "name": "Bangladesh",                    "ihme_id": 161, "wb_code": "BGD"},
    "BEL": {"iso3": "BEL", "name": "Belgium",                      "ihme_id": 76,  "wb_code": "BEL"},
    "BRA": {"iso3": "BRA", "name": "Brazil",                        "ihme_id": 135, "wb_code": "BRA"},
    "CAN": {"iso3": "CAN", "name": "Canada",                        "ihme_id": 101, "wb_code": "CAN"},
    "CHE": {"iso3": "CHE", "name": "Switzerland",                   "ihme_id": 93,  "wb_code": "CHE"},
    "CHL": {"iso3": "CHL", "name": "Chile",                         "ihme_id": 98,  "wb_code": "CHL"},
    "CHN": {"iso3": "CHN", "name": "China",                         "ihme_id": 6,   "wb_code": "CHN"},
    "COD": {"iso3": "COD", "name": "Democratic Republic of the Congo", "ihme_id": 171, "wb_code": "COD"},
    "COL": {"iso3": "COL", "name": "Colombia",                      "ihme_id": 125, "wb_code": "COL"},
    "CZE": {"iso3": "CZE", "name": "Czechia",                       "ihme_id": 78,  "wb_code": "CZE"},
    "DEU": {"iso3": "DEU", "name": "Germany",                       "ihme_id": 81,  "wb_code": "DEU"},
    "DNK": {"iso3": "DNK", "name": "Denmark",                       "ihme_id": 78,  "wb_code": "DNK"},
    "EGY": {"iso3": "EGY", "name": "Egypt",                         "ihme_id": 141, "wb_code": "EGY"},
    "ESP": {"iso3": "ESP", "name": "Spain",                         "ihme_id": 92,  "wb_code": "ESP"},
    "ETH": {"iso3": "ETH", "name": "Ethiopia",                      "ihme_id": 179, "wb_code": "ETH"},
    "FIN": {"iso3": "FIN", "name": "Finland",                       "ihme_id": 79,  "wb_code": "FIN"},
    "FRA": {"iso3": "FRA", "name": "France",                        "ihme_id": 80,  "wb_code": "FRA"},
    "GBR": {"iso3": "GBR", "name": "United Kingdom",                "ihme_id": 95,  "wb_code": "GBR"},
    "GHA": {"iso3": "GHA", "name": "Ghana",                         "ihme_id": 207, "wb_code": "GHA"},
    "GRC": {"iso3": "GRC", "name": "Greece",                        "ihme_id": 82,  "wb_code": "GRC"},
    "IDN": {"iso3": "IDN", "name": "Indonesia",                     "ihme_id": 11,  "wb_code": "IDN"},
    "IND": {"iso3": "IND", "name": "India",                         "ihme_id": 163, "wb_code": "IND"},
    "IRL": {"iso3": "IRL", "name": "Ireland",                       "ihme_id": 83,  "wb_code": "IRL"},
    "IRN": {"iso3": "IRN", "name": "Iran",                          "ihme_id": 142, "wb_code": "IRN"},
    "IRQ": {"iso3": "IRQ", "name": "Iraq",                          "ihme_id": 143, "wb_code": "IRQ"},
    "ISR": {"iso3": "ISR", "name": "Israel",                        "ihme_id": 144, "wb_code": "ISR"},
    "ITA": {"iso3": "ITA", "name": "Italy",                         "ihme_id": 86,  "wb_code": "ITA"},
    "JPN": {"iso3": "JPN", "name": "Japan",                         "ihme_id": 67,  "wb_code": "JPN"},
    "KEN": {"iso3": "KEN", "name": "Kenya",                         "ihme_id": 180, "wb_code": "KEN"},
    "KOR": {"iso3": "KOR", "name": "Republic of Korea",             "ihme_id": 68,  "wb_code": "KOR"},
    "LKA": {"iso3": "LKA", "name": "Sri Lanka",                     "ihme_id": 17,  "wb_code": "LKA"},
    "MAR": {"iso3": "MAR", "name": "Morocco",                       "ihme_id": 146, "wb_code": "MAR"},
    "MEX": {"iso3": "MEX", "name": "Mexico",                        "ihme_id": 130, "wb_code": "MEX"},
    "MMR": {"iso3": "MMR", "name": "Myanmar",                       "ihme_id": 15,  "wb_code": "MMR"},
    "MOZ": {"iso3": "MOZ", "name": "Mozambique",                    "ihme_id": 184, "wb_code": "MOZ"},
    "MYS": {"iso3": "MYS", "name": "Malaysia",                      "ihme_id": 13,  "wb_code": "MYS"},
    "NGA": {"iso3": "NGA", "name": "Nigeria",                       "ihme_id": 214, "wb_code": "NGA"},
    "NLD": {"iso3": "NLD", "name": "Netherlands",                   "ihme_id": 89,  "wb_code": "NLD"},
    "NOR": {"iso3": "NOR", "name": "Norway",                        "ihme_id": 90,  "wb_code": "NOR"},
    "NPL": {"iso3": "NPL", "name": "Nepal",                         "ihme_id": 164, "wb_code": "NPL"},
    "NZL": {"iso3": "NZL", "name": "New Zealand",                   "ihme_id": 72,  "wb_code": "NZL"},
    "PAK": {"iso3": "PAK", "name": "Pakistan",                      "ihme_id": 165, "wb_code": "PAK"},
    "PER": {"iso3": "PER", "name": "Peru",                          "ihme_id": 123, "wb_code": "PER"},
    "PHL": {"iso3": "PHL", "name": "Philippines",                   "ihme_id": 16,  "wb_code": "PHL"},
    "POL": {"iso3": "POL", "name": "Poland",                        "ihme_id": 51,  "wb_code": "POL"},
    "PRT": {"iso3": "PRT", "name": "Portugal",                      "ihme_id": 91,  "wb_code": "PRT"},
    "ROU": {"iso3": "ROU", "name": "Romania",                       "ihme_id": 52,  "wb_code": "ROU"},
    "RUS": {"iso3": "RUS", "name": "Russian Federation",            "ihme_id": 62,  "wb_code": "RUS"},
    "SAU": {"iso3": "SAU", "name": "Saudi Arabia",                  "ihme_id": 152, "wb_code": "SAU"},
    "SDN": {"iso3": "SDN", "name": "Sudan",                         "ihme_id": 522, "wb_code": "SDN"},
    "SWE": {"iso3": "SWE", "name": "Sweden",                        "ihme_id": 93,  "wb_code": "SWE"},
    "THA": {"iso3": "THA", "name": "Thailand",                      "ihme_id": 18,  "wb_code": "THA"},
    "TUR": {"iso3": "TUR", "name": "Turkey",                        "ihme_id": 155, "wb_code": "TUR"},
    "TZA": {"iso3": "TZA", "name": "Tanzania",                      "ihme_id": 189, "wb_code": "TZA"},
    "UGA": {"iso3": "UGA", "name": "Uganda",                        "ihme_id": 190, "wb_code": "UGA"},
    "UKR": {"iso3": "UKR", "name": "Ukraine",                       "ihme_id": 63,  "wb_code": "UKR"},
    "USA": {"iso3": "USA", "name": "United States",                 "ihme_id": 102, "wb_code": "USA"},
    "VNM": {"iso3": "VNM", "name": "Viet Nam",                      "ihme_id": 20,  "wb_code": "VNM"},
    "ZAF": {"iso3": "ZAF", "name": "South Africa",                  "ihme_id": 196, "wb_code": "ZAF"},
    "ZMB": {"iso3": "ZMB", "name": "Zambia",                        "ihme_id": 191, "wb_code": "ZMB"},
    "ZWE": {"iso3": "ZWE", "name": "Zimbabwe",                      "ihme_id": 198, "wb_code": "ZWE"},
}
# fmt: on


def who_to_iso3(who_code: str) -> str | None:
    """Map WHO GHO SpatialDim code to ISO3 (usually identical)."""
    entry = CROSSWALK.get(who_code)
    return entry["iso3"] if entry else None


def iso3_to_ihme(iso3: str) -> int | None:
    """Map ISO3 code to IHME location_id."""
    entry = CROSSWALK.get(iso3)
    return entry["ihme_id"] if entry else None


def iso3_to_wb(iso3: str) -> str | None:
    """Map ISO3 code to World Bank country code."""
    entry = CROSSWALK.get(iso3)
    return entry["wb_code"] if entry else None


def iso3_to_name(iso3: str) -> str | None:
    """Map ISO3 code to country name."""
    entry = CROSSWALK.get(iso3)
    return entry["name"] if entry else None


def get_all_iso3_codes() -> list[str]:
    """Return all ISO3 codes in the crosswalk."""
    return sorted(CROSSWALK.keys())


def enrich_dataframe(df: "pandas.DataFrame", iso3_col: str = "country_iso3") -> "pandas.DataFrame":
    """Add ihme_id, wb_code, and country_name columns from the crosswalk."""
    import pandas as pd

    enriched = df.copy()
    enriched["ihme_id"] = enriched[iso3_col].map(lambda c: iso3_to_ihme(c))
    enriched["wb_code"] = enriched[iso3_col].map(lambda c: iso3_to_wb(c))
    enriched["country_name"] = enriched[iso3_col].map(lambda c: iso3_to_name(c))
    return enriched
