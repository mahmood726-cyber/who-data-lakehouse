"""
Dedicated WHO GHO + GHED API client with rate limiting and pagination.

WHO GHO API: https://ghoapi.azureedge.net/api/
Endpoints:
  GET /api/Indicator                    -> list all indicators
  GET /api/{IndicatorCode}              -> data for specific indicator
  GET /api/DIMENSION/{DimensionCode}    -> dimension values

GHED indicators are available via the GHO API with codes starting with "GHED_".
"""
from __future__ import annotations

import time
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from who_data_lakehouse.config import DEFAULT_TIMEOUT_SECONDS, USER_AGENT


class WHOClient:
    """Rate-limited client for the WHO Global Health Observatory OData API."""

    BASE_URL = "https://ghoapi.azureedge.net/api"

    def __init__(self, rate_limit_seconds: float = 0.5) -> None:
        self.rate_limit = rate_limit_seconds
        self._last_request: float = 0.0
        self.session = self._build_session()

    @staticmethod
    def _build_session() -> requests.Session:
        retry = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET", "HEAD"),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session = requests.Session()
        session.headers.update({
            "Accept": "application/json",
            "User-Agent": USER_AGENT,
        })
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def _throttled_get(
        self,
        url: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Rate-limited GET request that respects inter-request spacing."""
        elapsed = time.time() - self._last_request
        if elapsed < self.rate_limit:
            time.sleep(self.rate_limit - elapsed)
        resp = self.session.get(url, params=params, timeout=DEFAULT_TIMEOUT_SECONDS)
        self._last_request = time.time()
        resp.raise_for_status()
        return resp.json()

    def list_indicators(self) -> list[dict[str, Any]]:
        """Return list of all indicator codes and names."""
        data = self._throttled_get(f"{self.BASE_URL}/Indicator")
        return data.get("value", [])

    def get_indicator_data(
        self,
        indicator_code: str,
        filters: str | None = None,
    ) -> list[dict[str, Any]]:
        """
        Get data for a specific indicator with automatic pagination.

        Parameters
        ----------
        indicator_code : str
            GHO indicator code, e.g. "WHOSIS_000001".
        filters : str, optional
            OData filter string, e.g. "$filter=SpatialDim eq 'GBR'".

        Returns
        -------
        list[dict]
            Records with keys like SpatialDim, TimeDim, NumericValue, etc.
        """
        url: str | None = f"{self.BASE_URL}/{indicator_code}"
        params: dict[str, str] = {}
        if filters:
            params["$filter"] = filters

        all_records: list[dict[str, Any]] = []
        while url:
            data = self._throttled_get(url, params if params else None)
            all_records.extend(data.get("value", []))
            url = data.get("@odata.nextLink")
            params = {}  # nextLink already includes query params

        return all_records

    def get_dimension_values(self, dimension_code: str) -> list[dict[str, Any]]:
        """Get values for a dimension (e.g., COUNTRY, YEAR, SEX)."""
        data = self._throttled_get(
            f"{self.BASE_URL}/DIMENSION/{dimension_code}/DimensionValues"
        )
        return data.get("value", [])
