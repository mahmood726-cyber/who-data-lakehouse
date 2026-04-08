"""Tests for GLAAS XLSX → silver parquet promotion."""
from pathlib import Path
import pandas as pd
import pytest

from who_data_lakehouse.promote.glaas import promote_glaas


@pytest.fixture
def tmp_raw(tmp_path):
    raw_dir = tmp_path / "raw" / "glaas"
    raw_dir.mkdir(parents=True)
    with pd.ExcelWriter(raw_dir / "glaas_test.xlsx", engine="openpyxl") as w:
        pd.DataFrame({"country": ["Kenya", "Ghana"], "q1": [1, 2], "q2": [3, 4]}).to_excel(
            w, sheet_name="Section A Data", index=False
        )
        pd.DataFrame({"code": [1, 2], "meaning": ["Yes", "No"]}).to_excel(
            w, sheet_name="Section A Response Code", index=False
        )
        pd.DataFrame({"country": ["Kenya"], "status": ["Complete"]}).to_excel(
            w, sheet_name="GLAAS Participants 2025", index=False
        )
    return tmp_path


@pytest.fixture
def tmp_silver(tmp_path):
    return tmp_path / "silver" / "glaas"


def test_promote_glaas_creates_parquets(tmp_raw, tmp_silver):
    result = promote_glaas(
        tmp_raw / "raw" / "glaas" / "glaas_test.xlsx", tmp_silver
    )
    assert result["status"] == "promoted"
    assert len(result["sheets"]) == 3
    assert (tmp_silver / "section_a_data.parquet").exists()
    assert (tmp_silver / "section_a_response_code.parquet").exists()
    assert (tmp_silver / "glaas_participants_2025.parquet").exists()


def test_promote_glaas_normalizes_columns(tmp_raw, tmp_silver):
    promote_glaas(tmp_raw / "raw" / "glaas" / "glaas_test.xlsx", tmp_silver)
    df = pd.read_parquet(tmp_silver / "section_a_data.parquet")
    assert "country" in df.columns
    assert len(df) == 2
