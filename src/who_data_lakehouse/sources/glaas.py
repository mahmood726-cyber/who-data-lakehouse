from __future__ import annotations

import re
from pathlib import Path

from who_data_lakehouse.http import build_session, download_to_path, get_text


PAGE_URL = "https://glaas.who.int/"


class GlaasClient:
    def __init__(self) -> None:
        self.session = build_session()

    def discover_full_dataset_url(self) -> str:
        html = get_text(self.session, PAGE_URL)
        match = re.search(r"https://cdn\.who\.int[^\"']+\.xlsx[^\"']*", html)
        if not match:
            raise RuntimeError("Could not discover the GLAAS full dataset workbook URL")
        return match.group(0).replace("&amp;", "&")

    def download_full_dataset(
        self,
        destination_dir: Path,
        skip_existing: bool = False,
    ) -> Path:
        url = self.discover_full_dataset_url()
        filename = Path(url.split("?", 1)[0]).name
        destination = destination_dir / filename
        if skip_existing and destination.exists():
            return destination
        return download_to_path(self.session, url, destination)
