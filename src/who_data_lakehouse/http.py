from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from who_data_lakehouse.config import DEFAULT_TIMEOUT_SECONDS, USER_AGENT


def build_session(user_agent: str = USER_AGENT) -> requests.Session:
    retry = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "HEAD"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.headers.update(
        {
            "Accept": "application/json, text/plain, */*",
            "User-Agent": user_agent,
        }
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def get_json(session: requests.Session, url: str, params: dict | None = None) -> dict:
    response = session.get(url, params=params, timeout=DEFAULT_TIMEOUT_SECONDS)
    response.raise_for_status()
    return response.json()


def get_text(session: requests.Session, url: str, params: dict | None = None) -> str:
    response = session.get(url, params=params, timeout=DEFAULT_TIMEOUT_SECONDS)
    response.raise_for_status()
    return response.text


def download_to_path(
    session: requests.Session,
    url: str,
    destination: Path,
    params: dict | None = None,
) -> Path:
    response = session.get(url, params=params, timeout=DEFAULT_TIMEOUT_SECONDS)
    response.raise_for_status()
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(response.content)
    return destination
