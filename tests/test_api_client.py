"""Tests for WHO GHO API client (mock-based, no network)."""
from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

import pytest
import requests

from who_data_lakehouse.api_client import WHOClient


@pytest.fixture
def client():
    """Create a WHOClient with short rate limit for test speed."""
    c = WHOClient(rate_limit_seconds=0.05)
    return c


class TestListIndicators:
    def test_returns_list(self, client):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "value": [
                {"IndicatorCode": "WHOSIS_000001", "IndicatorName": "Life expectancy"},
                {"IndicatorCode": "MDG_0000000001", "IndicatorName": "Under-5 mortality"},
            ]
        }
        mock_response.raise_for_status = MagicMock()
        with patch.object(client.session, "get", return_value=mock_response):
            result = client.list_indicators()
        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0]["IndicatorCode"] == "WHOSIS_000001"


class TestGetIndicatorData:
    def test_returns_records_with_expected_fields(self, client):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "value": [
                {
                    "SpatialDim": "GBR",
                    "TimeDim": 2020,
                    "NumericValue": 81.3,
                    "Dim1": "BTSX",
                },
            ]
        }
        mock_response.raise_for_status = MagicMock()
        with patch.object(client.session, "get", return_value=mock_response):
            records = client.get_indicator_data("WHOSIS_000001")
        assert len(records) == 1
        assert records[0]["SpatialDim"] == "GBR"
        assert records[0]["NumericValue"] == 81.3

    def test_pagination_follows_nextlink(self, client):
        page1 = MagicMock()
        page1.json.return_value = {
            "value": [{"SpatialDim": "GBR", "NumericValue": 81.3}],
            "@odata.nextLink": "https://ghoapi.azureedge.net/api/TEST?$skip=1",
        }
        page1.raise_for_status = MagicMock()

        page2 = MagicMock()
        page2.json.return_value = {
            "value": [{"SpatialDim": "USA", "NumericValue": 78.9}],
        }
        page2.raise_for_status = MagicMock()

        with patch.object(client.session, "get", side_effect=[page1, page2]):
            records = client.get_indicator_data("TEST")
        assert len(records) == 2
        assert records[0]["SpatialDim"] == "GBR"
        assert records[1]["SpatialDim"] == "USA"

    def test_filters_passed_as_params(self, client):
        mock_response = MagicMock()
        mock_response.json.return_value = {"value": []}
        mock_response.raise_for_status = MagicMock()
        with patch.object(client.session, "get", return_value=mock_response) as mock_get:
            client.get_indicator_data("TEST", filters="SpatialDim eq 'GBR'")
        call_args = mock_get.call_args
        assert call_args[1].get("params", {}).get("$filter") == "SpatialDim eq 'GBR'"


class TestRateLimiting:
    def test_requests_spaced_by_rate_limit(self, client):
        mock_response = MagicMock()
        mock_response.json.return_value = {"value": []}
        mock_response.raise_for_status = MagicMock()

        client.rate_limit = 0.1
        with patch.object(client.session, "get", return_value=mock_response):
            t0 = time.time()
            client._throttled_get("http://test1")
            client._throttled_get("http://test2")
            elapsed = time.time() - t0
        assert elapsed >= 0.09, f"Elapsed {elapsed} < rate_limit"


class TestHTTPError:
    def test_http_error_raised(self, client):
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("404 Not Found")
        with patch.object(client.session, "get", return_value=mock_response):
            with pytest.raises(requests.HTTPError):
                client._throttled_get("http://nonexistent")
