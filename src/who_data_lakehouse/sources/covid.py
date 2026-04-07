from __future__ import annotations

from pathlib import Path
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from who_data_lakehouse.http import build_session, download_to_path, get_text


PAGE_URL = "https://data.who.int/dashboards/covid19/data"


class CovidDashboardClient:
    def __init__(self) -> None:
        self.session = build_session()

    def discover_downloads(self) -> dict[str, str]:
        html = get_text(self.session, PAGE_URL)
        soup = BeautifulSoup(html, "html.parser")
        downloads: dict[str, str] = {}
        for link in soup.find_all("a", href=True):
            href = link["href"].strip()
            text = " ".join(link.get_text(" ", strip=True).split())
            if text != "Download":
                continue
            filename = Path(urlparse(href).path).name
            if not filename:
                continue
            downloads[filename] = href
        if not downloads:
            raise RuntimeError("No COVID dashboard downloads discovered")
        return downloads

    def download_all(self, destination_dir: Path, skip_existing: bool = False) -> dict[str, Path]:
        downloads = self.discover_downloads()
        written: dict[str, Path] = {}
        for filename, url in downloads.items():
            destination = destination_dir / filename
            if skip_existing and destination.exists():
                written[filename] = destination
                continue
            written[filename] = download_to_path(self.session, url, destination)
        return written
