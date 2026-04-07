from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import urljoin

import pandas as pd

from who_data_lakehouse.config import DEFAULT_PAGE_SIZE
from who_data_lakehouse.http import build_session, download_to_path, get_json, get_text


CATALOG_PAGE_URL = "https://data.who.int/indicators"
API_URL = "https://data.who.int/api/datadot/ddiindicatorperspectives"
DEFAULT_SELECT_FIELDS = (
    "Id",
    "Identifier",
    "FullName",
    "ShortName",
    "ShortDescription",
    "ItemDefaultUrl",
    "DataExportUri",
    "DataUpdateLast",
    "LastModified",
    "PublicationDate",
    "TemporalCoverageTo",
    "AlternativeCodes",
    "AlsoKnownAs",
    "DataComplete",
    "IsDefaultPerspective",
)


class DatadotClient:
    def __init__(self) -> None:
        self.session = build_session()
        self.provider = self.discover_provider()

    def discover_provider(self) -> str:
        html = get_text(self.session, CATALOG_PAGE_URL)
        match = re.search(r"sf_provider=([A-Za-z0-9_]+)", html)
        if match:
            return match.group(1)
        match = re.search(r"(dynamicProvider\d+)", html)
        if match:
            return match.group(1)
        raise RuntimeError("Could not discover data.who.int provider id")

    def fetch_indicator_catalog(
        self,
        page_size: int = DEFAULT_PAGE_SIZE,
        row_limit: int | None = None,
    ) -> list[dict]:
        rows: list[dict] = []
        skip = 0
        safe_page_size = min(page_size, 100)
        select_clause = ",".join(DEFAULT_SELECT_FIELDS)
        while True:
            payload = get_json(
                self.session,
                API_URL,
                params={
                    "sf_provider": self.provider,
                    "sf_culture": "en",
                    "$select": select_clause,
                    "$top": safe_page_size,
                    "$skip": skip,
                },
            )
            batch = payload.get("value", [])
            if not batch:
                break
            rows.extend(batch)
            if row_limit is not None and len(rows) >= row_limit:
                return rows[:row_limit]
            if len(batch) < safe_page_size:
                break
            skip += safe_page_size
        return rows

    def build_catalog_table(
        self,
        page_size: int = DEFAULT_PAGE_SIZE,
        row_limit: int | None = None,
    ) -> pd.DataFrame:
        frame = pd.json_normalize(
            self.fetch_indicator_catalog(page_size=page_size, row_limit=row_limit)
        )
        if frame.empty:
            return frame
        if "ItemDefaultUrl" in frame.columns:
            frame["AbsoluteIndicatorUrl"] = frame["ItemDefaultUrl"].map(
                lambda value: urljoin(
                    "https://data.who.int/indicators/i/",
                    str(value).lstrip("/"),
                )
                if pd.notna(value)
                else None
            )
        if "DataExportUri" in frame.columns:
            frame["DataExportUri"] = frame["DataExportUri"].replace("", pd.NA)
        return frame

    def download_exports(
        self,
        destination_dir: Path,
        identifiers: list[str] | None = None,
        row_limit: int | None = None,
        skip_existing: bool = False,
    ) -> dict[str, Path]:
        frame = self.build_catalog_table(page_size=DEFAULT_PAGE_SIZE, row_limit=row_limit)
        if identifiers:
            frame = frame[frame["Identifier"].isin(identifiers)]
        if frame.empty:
            return {}
        downloads: dict[str, Path] = {}
        exportable = frame[frame["DataExportUri"].notna()].copy()
        for row in exportable.itertuples(index=False):
            identifier = getattr(row, "Identifier", None) or getattr(row, "Id")
            filename = f"{self._slugify(str(identifier))}.csv"
            destination = destination_dir / filename
            if skip_existing and destination.exists():
                downloads[str(identifier)] = destination
                continue
            downloads[str(identifier)] = download_to_path(
                self.session,
                getattr(row, "DataExportUri"),
                destination,
            )
        return downloads

    @staticmethod
    def _slugify(value: str) -> str:
        return re.sub(r"[^A-Za-z0-9._-]+", "_", value).strip("_")
