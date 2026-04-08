"""Tests for mortality ZIP → silver parquet promotion."""
from pathlib import Path
import zipfile
import pandas as pd
import pytest

from who_data_lakehouse.promote.mortality import promote_mortality


@pytest.fixture
def tmp_raw(tmp_path):
    raw_dir = tmp_path / "raw" / "mortality"
    raw_dir.mkdir(parents=True)

    # Create a ZIP with an extensionless CSV (like morticd10_part1)
    csv_content = (
        "Country,Admin1,SubDiv,Year,List,Cause,Sex,Frmat,Deaths1,Deaths2\n"
        "1400,,,2001,101,1000,1,07,332,8\n"
        "1400,,,2001,101,1000,2,07,222,11\n"
    )
    zip_path = raw_dir / "morticd10_part1.zip"
    with zipfile.ZipFile(zip_path, "w") as z:
        z.writestr("Morticd10_part1", csv_content)

    # Create a ZIP with country codes
    cc_content = "country,name\n1010,Algeria\n1020,Angola\n"
    with zipfile.ZipFile(raw_dir / "mort_country_codes.zip", "w") as z:
        z.writestr("country_codes", cc_content)

    # Create a documentation ZIP (should be skipped)
    with zipfile.ZipFile(raw_dir / "mort_documentation_abc.zip", "w") as z:
        z.writestr("doc.pdf", b"fake pdf")

    return tmp_path


@pytest.fixture
def tmp_silver(tmp_path):
    return tmp_path / "silver" / "mortality"


def test_promote_mortality_creates_parquets(tmp_raw, tmp_silver):
    results = promote_mortality(tmp_raw / "raw" / "mortality", tmp_silver)
    promoted = [r for r in results if r["status"] == "promoted"]
    assert len(promoted) == 2
    assert (tmp_silver / "morticd10_part1.parquet").exists()
    assert (tmp_silver / "mort_country_codes.parquet").exists()
    df = pd.read_parquet(tmp_silver / "morticd10_part1.parquet")
    assert len(df) == 2
    assert "country" in df.columns
    assert "deaths1" in df.columns


def test_promote_mortality_skips_documentation(tmp_raw, tmp_silver):
    results = promote_mortality(tmp_raw / "raw" / "mortality", tmp_silver)
    doc = [r for r in results if "documentation" in r["file"]]
    assert len(doc) == 1
    assert doc[0]["status"] == "skipped_doc"


def test_promote_mortality_skips_existing(tmp_raw, tmp_silver):
    promote_mortality(tmp_raw / "raw" / "mortality", tmp_silver)
    results = promote_mortality(tmp_raw / "raw" / "mortality", tmp_silver, skip_existing=True)
    promoted = [r for r in results if r["status"] == "promoted"]
    assert len(promoted) == 0
