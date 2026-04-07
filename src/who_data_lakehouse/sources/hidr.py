from __future__ import annotations

from pathlib import Path
from urllib.parse import urljoin

import pandas as pd
from bs4 import BeautifulSoup

from who_data_lakehouse.http import build_session, download_to_path, get_text
from who_data_lakehouse.storage import read_table


DOC_PAGE_URL = "https://www.who.int/data/inequality-monitor/data/hidr-api"
DOWNLOAD_BASE_URL = "https://datasafe-h5afbhf4gwctabaa.z01.azurefd.net/api/download/TOP"
REFERENCE_LABELS = {
    "Dataset IDs": "dataset_ids",
    "Dataset variables": "dataset_variables",
    "OData feed URLs": "odata_feed_urls",
}


class HidrClient:
    def __init__(self) -> None:
        self.session = build_session()

    def discover_reference_docs(self) -> dict[str, str]:
        html = get_text(self.session, DOC_PAGE_URL)
        soup = BeautifulSoup(html, "html.parser")
        docs: dict[str, str] = {}
        for link in soup.find_all("a", href=True):
            label = " ".join(link.get_text(" ", strip=True).split())
            key = REFERENCE_LABELS.get(label)
            if key:
                docs[key] = urljoin(DOC_PAGE_URL, link["href"])
        if set(docs) != set(REFERENCE_LABELS.values()):
            missing = sorted(set(REFERENCE_LABELS.values()) - set(docs))
            raise RuntimeError(f"Missing HIDR reference docs: {', '.join(missing)}")
        return docs

    def download_reference_docs(self, destination_dir: Path) -> dict[str, Path]:
        docs = self.discover_reference_docs()
        downloaded: dict[str, Path] = {}
        for key, url in docs.items():
            suffix = Path(url.split("?")[0]).suffix or ".bin"
            downloaded[key] = download_to_path(
                self.session,
                url,
                destination_dir / f"{key}{suffix}",
            )
        return downloaded

    def download_dataset(self, dataset_id: str, destination_dir: Path) -> Path:
        url = f"{DOWNLOAD_BASE_URL}/{dataset_id}/data"
        return download_to_path(self.session, url, destination_dir / f"{dataset_id}.xlsx")

    def dataset_ids_from_reference(self, reference_path: Path) -> list[str]:
        frame = read_table(reference_path)
        lowered = {column.lower(): column for column in frame.columns}
        preferred = None
        for candidate in lowered:
            if "dataset" in candidate and "id" in candidate:
                preferred = lowered[candidate]
                break
        if preferred is None:
            preferred = frame.columns[0]
        dataset_ids = (
            frame[preferred]
            .dropna()
            .astype(str)
            .str.strip()
            .replace("", pd.NA)
            .dropna()
            .tolist()
        )
        return list(dict.fromkeys(dataset_ids))
