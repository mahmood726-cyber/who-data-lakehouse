from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
REFERENCE_DIR = DATA_DIR / "reference"
MANIFEST_DIR = ROOT / "manifests"
OUTPUT_DIR = ROOT / "output"
PLAYWRIGHT_OUTPUT_DIR = OUTPUT_DIR / "playwright"

DEFAULT_TIMEOUT_SECONDS = 180
DEFAULT_PAGE_SIZE = 200
DEFAULT_CONCURRENCY = 4
USER_AGENT = "who-data-lakehouse/0.1"


def ensure_project_directories() -> None:
    for path in (
        RAW_DIR,
        BRONZE_DIR,
        SILVER_DIR,
        REFERENCE_DIR,
        MANIFEST_DIR,
        PLAYWRIGHT_OUTPUT_DIR,
    ):
        path.mkdir(parents=True, exist_ok=True)
