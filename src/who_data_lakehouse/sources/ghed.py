from __future__ import annotations

from pathlib import Path
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from who_data_lakehouse.http import build_session, download_to_path, get_text


PAGE_URL = "https://apps.who.int/nha/database/Select/Indicators/en"


class GhedClient:
    def __init__(self) -> None:
        self.session = build_session()

    def discover_download_url(self) -> str:
        html = get_text(self.session, PAGE_URL)
        soup = BeautifulSoup(html, "html.parser")
        for link in soup.find_all("a", href=True):
            text = " ".join(link.get_text(" ", strip=True).split())
            if "Download all data in XLSX format" in text:
                return urljoin(PAGE_URL, link["href"])
        raise RuntimeError("Could not discover GHED download URL")

    def download_export(self, destination_dir: Path, skip_existing: bool = False) -> Path:
        destination = destination_dir / "GHED_data.XLSX"
        if skip_existing and destination.exists():
            return destination
        return download_to_path(self.session, self.discover_download_url(), destination)
