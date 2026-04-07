from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
from bs4 import BeautifulSoup

from who_data_lakehouse.http import build_session, get_json, get_text


API_URL = "https://data.who.int/api/datadot/pages"


class DatadotPageClient:
    def __init__(self) -> None:
        self.session = build_session()

    def fetch_pages(self, page_size: int = 100) -> list[dict]:
        rows: list[dict] = []
        skip = 0
        safe_page_size = min(page_size, 100)
        while True:
            payload = get_json(
                self.session,
                API_URL,
                params={"$top": safe_page_size, "$skip": skip},
            )
            batch = payload.get("value", [])
            if not batch:
                break
            rows.extend(batch)
            if len(batch) < safe_page_size:
                break
            skip += safe_page_size
        return rows

    def build_pages_table(self, page_size: int = 100) -> pd.DataFrame:
        return pd.DataFrame.from_records(self.fetch_pages(page_size=page_size))

    def file_like_pages(self, pages: pd.DataFrame) -> pd.DataFrame:
        if pages.empty or "RelativeUrlPath" not in pages.columns:
            return pd.DataFrame()
        mask = pages["RelativeUrlPath"].fillna("").str.contains(r"\.(?:csv|xlsx|zip)$", regex=True)
        return pages.loc[mask].copy()

    def scan_download_links(
        self,
        pages: pd.DataFrame,
        concurrency: int = 8,
    ) -> pd.DataFrame:
        if pages.empty:
            return pd.DataFrame(columns=["title", "view_url", "text", "href"])

        subset = pages.loc[
            pages["RelativeUrlPath"].fillna("").str.contains("/dashboards/|/platforms/", regex=True),
            ["Title", "ViewUrl", "RelativeUrlPath"],
        ].drop_duplicates()

        def probe(view_url: str) -> list[dict]:
            session = build_session()
            html = get_text(session, view_url)
            soup = BeautifulSoup(html, "html.parser")
            rows: list[dict] = []
            for link in soup.find_all("a", href=True):
                href = link["href"].strip()
                text = " ".join(link.get_text(" ", strip=True).split())
                if (
                    "blob.core.windows.net" in href
                    or href.lower().endswith((".csv", ".xlsx", ".zip"))
                    or text.lower() == "download"
                ):
                    rows.append({"view_url": view_url, "text": text, "href": href})
            return rows

        found: list[dict] = []
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = {executor.submit(probe, row.ViewUrl): row for row in subset.itertuples(index=False)}
            for future in as_completed(futures):
                row = futures[future]
                try:
                    rows = future.result()
                except Exception:
                    continue
                for item in rows:
                    item["title"] = row.Title
                    item["relative_url_path"] = row.RelativeUrlPath
                    found.append(item)

        if not found:
            return pd.DataFrame(columns=["title", "view_url", "relative_url_path", "text", "href"])
        frame = pd.DataFrame.from_records(found).drop_duplicates()
        return frame.sort_values(["title", "href"]).reset_index(drop=True)

    def useful_download_links(self, links: pd.DataFrame) -> pd.DataFrame:
        if links.empty:
            return pd.DataFrame(columns=["title", "view_url", "relative_url_path", "text", "href"])
        mask = (
            links["href"].str.contains(r"\.(?:csv|xlsx|zip)(?:\?|$)", case=False, regex=True)
            | links["href"].str.contains("/api/export_", case=False, regex=False)
        )
        useful = links.loc[mask].copy()
        return useful.drop_duplicates(subset=["href"]).reset_index(drop=True)

    def download_useful_links(
        self,
        links: pd.DataFrame,
        destination_dir: Path,
        skip_existing: bool = False,
    ) -> dict[str, Path]:
        written: dict[str, Path] = {}
        for row in links.itertuples(index=False):
            response = self.session.get(row.href, timeout=120)
            response.raise_for_status()
            filename = self._infer_filename(row.href, response.headers.get("content-disposition"))
            destination = destination_dir / filename
            if skip_existing and destination.exists():
                written[row.href] = destination
                continue
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(response.content)
            written[row.href] = destination
        return written

    @staticmethod
    def _infer_filename(url: str, content_disposition: str | None) -> str:
        if content_disposition and "filename=" in content_disposition:
            filename = content_disposition.split("filename=", 1)[1].strip().strip('"')
            if filename:
                return filename
        name = Path(urlparse(url).path).name
        if name:
            return name
        return "download.bin"
