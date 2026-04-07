from __future__ import annotations

import pandas as pd

from who_data_lakehouse.config import DEFAULT_PAGE_SIZE
from who_data_lakehouse.http import build_session, get_json


BASE_URL = "https://ghoapi.azureedge.net/api"


class GhoClient:
    def __init__(self) -> None:
        self.session = build_session()

    def list_indicators(self, page_size: int = DEFAULT_PAGE_SIZE) -> list[dict]:
        return self._paged_entity("Indicator", page_size=page_size)

    def list_dimensions(self, page_size: int = DEFAULT_PAGE_SIZE) -> list[dict]:
        return self._paged_entity("Dimension", page_size=page_size)

    def list_dimension_values(self, dimension_code: str) -> list[dict]:
        payload = get_json(
            self.session,
            f"{BASE_URL}/Dimension/{dimension_code}/DimensionValues",
            params={"$format": "json"},
        )
        return payload.get("value", [])

    def fetch_indicator_records(
        self,
        indicator_code: str,
        page_size: int = 1000,
        row_limit: int | None = None,
    ) -> list[dict]:
        return self._paged_entity(indicator_code, page_size=page_size, row_limit=row_limit)

    def build_metadata_tables(self, page_size: int = DEFAULT_PAGE_SIZE) -> dict[str, pd.DataFrame]:
        indicators = pd.DataFrame.from_records(self.list_indicators(page_size=page_size))
        dimensions = pd.DataFrame.from_records(self.list_dimensions(page_size=page_size))
        value_frames = []
        for dimension_code in dimensions["Code"].dropna().astype(str).tolist():
            values = self.list_dimension_values(dimension_code)
            if not values:
                continue
            value_frame = pd.DataFrame.from_records(values)
            value_frame["RequestedDimensionCode"] = dimension_code
            value_frames.append(value_frame)
        dimension_values = (
            pd.concat(value_frames, ignore_index=True) if value_frames else pd.DataFrame()
        )
        return {
            "indicators": indicators,
            "dimensions": dimensions,
            "dimension_values": dimension_values,
        }

    def _paged_entity(
        self,
        entity_name: str,
        page_size: int = DEFAULT_PAGE_SIZE,
        row_limit: int | None = None,
    ) -> list[dict]:
        rows: list[dict] = []
        skip = 0
        while True:
            payload = get_json(
                self.session,
                f"{BASE_URL}/{entity_name}",
                params={"$format": "json", "$top": page_size, "$skip": skip},
            )
            batch = payload.get("value", [])
            if not batch:
                break
            rows.extend(batch)
            if row_limit is not None and len(rows) >= row_limit:
                return rows[:row_limit]
            if len(batch) < page_size:
                break
            skip += page_size
        return rows
