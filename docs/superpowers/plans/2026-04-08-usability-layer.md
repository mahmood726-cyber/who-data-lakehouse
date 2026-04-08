# WHO Data Lakehouse Usability Layer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the lakehouse data easily discoverable and analysis-ready by promoting all raw sources to silver (parquet), building a master catalog, and adding a search CLI.

**Architecture:** Five promotion modules (one per unpromoted source) convert raw CSV/XLSX/ZIP to normalized parquet in `data/silver/`. A catalog builder scans all silver data and produces `data/catalog.parquet` with one row per dataset. A new `who-data search` CLI command queries the catalog by keyword, topic, or country. GHO facts get loaded via the existing pipeline. HIDR XLSX datasets get unpacked to parquet.

**Tech Stack:** Python 3.11+, pandas, pyarrow, openpyxl (already in dependencies)

---

## File Structure

| File | Responsibility |
|------|---------------|
| `src/who_data_lakehouse/promote/__init__.py` | Package init |
| `src/who_data_lakehouse/promote/covid.py` | COVID CSV → silver parquet |
| `src/who_data_lakehouse/promote/ghed.py` | GHED XLSX → silver parquet |
| `src/who_data_lakehouse/promote/whs.py` | WHS XLSX → silver parquet |
| `src/who_data_lakehouse/promote/glaas.py` | GLAAS XLSX → silver parquet |
| `src/who_data_lakehouse/promote/hidr.py` | HIDR XLSX datasets → silver parquet |
| `src/who_data_lakehouse/promote/gho_facts.py` | GHO facts JSONL.gz → silver parquet |
| `src/who_data_lakehouse/catalog.py` | Master catalog builder |
| `src/who_data_lakehouse/cli.py` | (modify) Add `promote`, `build-catalog`, `search` commands |
| `tests/test_promote_covid.py` | Tests for COVID promotion |
| `tests/test_promote_ghed.py` | Tests for GHED promotion |
| `tests/test_promote_whs.py` | Tests for WHS promotion |
| `tests/test_promote_glaas.py` | Tests for GLAAS promotion |
| `tests/test_promote_hidr.py` | Tests for HIDR promotion |
| `tests/test_promote_gho_facts.py` | Tests for GHO facts promotion |
| `tests/test_catalog.py` | Tests for catalog builder |
| `tests/test_search_cli.py` | Tests for search CLI |

---

### Task 1: COVID CSV Promotion

**Files:**
- Create: `src/who_data_lakehouse/promote/__init__.py`
- Create: `src/who_data_lakehouse/promote/covid.py`
- Create: `tests/test_promote_covid.py`

- [ ] **Step 1: Create the promote package and write the failing test**

Create `src/who_data_lakehouse/promote/__init__.py`:
```python
"""Promotion modules: raw → silver for each WHO data source."""
```

Create `tests/test_promote_covid.py`:
```python
"""Tests for COVID CSV → silver parquet promotion."""
from pathlib import Path
import pandas as pd
import pytest

from who_data_lakehouse.promote.covid import promote_covid


@pytest.fixture
def tmp_raw(tmp_path):
    """Create a minimal raw COVID directory with one CSV."""
    raw_dir = tmp_path / "raw" / "covid"
    raw_dir.mkdir(parents=True)
    df = pd.DataFrame({
        "Date_reported": ["2024-01-01", "2024-01-02"],
        "Country_code": ["US", "US"],
        "Country": ["United States of America", "United States of America"],
        "WHO_region": ["AMRO", "AMRO"],
        "New_cases": [100.0, 200.0],
        "Cumulative_cases": [1000, 1200],
        "New_deaths": [1.0, 2.0],
        "Cumulative_deaths": [50, 52],
    })
    df.to_csv(raw_dir / "WHO-COVID-19-global-data.csv", index=False)
    return tmp_path


@pytest.fixture
def tmp_silver(tmp_path):
    return tmp_path / "silver" / "covid"


def test_promote_covid_creates_parquet(tmp_raw, tmp_silver):
    results = promote_covid(tmp_raw / "raw" / "covid", tmp_silver)
    assert len(results) == 1
    assert results[0]["status"] == "promoted"
    parquet_path = tmp_silver / "WHO-COVID-19-global-data.parquet"
    assert parquet_path.exists()
    df = pd.read_parquet(parquet_path)
    assert len(df) == 2
    assert "date_reported" in df.columns  # snake_case
    assert "country_code" in df.columns


def test_promote_covid_normalizes_columns(tmp_raw, tmp_silver):
    promote_covid(tmp_raw / "raw" / "covid", tmp_silver)
    df = pd.read_parquet(tmp_silver / "WHO-COVID-19-global-data.parquet")
    # All columns should be snake_case
    for col in df.columns:
        assert col == col.lower(), f"Column {col} not lowercase"
        assert " " not in col, f"Column {col} has spaces"


def test_promote_covid_parses_dates(tmp_raw, tmp_silver):
    promote_covid(tmp_raw / "raw" / "covid", tmp_silver)
    df = pd.read_parquet(tmp_silver / "WHO-COVID-19-global-data.parquet")
    assert pd.api.types.is_datetime64_any_dtype(df["date_reported"])


def test_promote_covid_skips_existing(tmp_raw, tmp_silver):
    promote_covid(tmp_raw / "raw" / "covid", tmp_silver)
    results = promote_covid(tmp_raw / "raw" / "covid", tmp_silver, skip_existing=True)
    assert results[0]["status"] == "skipped"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd C:\Projects\who-data-lakehouse && python -m pytest tests/test_promote_covid.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'who_data_lakehouse.promote.covid'`

- [ ] **Step 3: Implement COVID promotion**

Create `src/who_data_lakehouse/promote/covid.py`:
```python
"""Promote raw COVID CSVs to silver parquet."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from who_data_lakehouse.normalize import normalize_columns


# Columns that should be parsed as dates
DATE_COLUMNS = ["date_reported", "date"]


def promote_covid(
    raw_dir: Path,
    silver_dir: Path,
    skip_existing: bool = False,
) -> list[dict]:
    """Convert each CSV in raw_dir to a normalized parquet in silver_dir.

    Returns a list of dicts with keys: file, status, rows, path.
    """
    silver_dir.mkdir(parents=True, exist_ok=True)
    results = []

    for csv_path in sorted(raw_dir.glob("*.csv")):
        stem = csv_path.stem
        parquet_path = silver_dir / f"{stem}.parquet"

        if skip_existing and parquet_path.exists():
            results.append({"file": csv_path.name, "status": "skipped", "rows": None, "path": str(parquet_path)})
            continue

        df = pd.read_csv(csv_path, low_memory=False)
        df = normalize_columns(df)

        # Parse date columns if present
        for col in DATE_COLUMNS:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

        df.to_parquet(parquet_path, index=False)
        results.append({"file": csv_path.name, "status": "promoted", "rows": len(df), "path": str(parquet_path)})

    return results
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd C:\Projects\who-data-lakehouse && python -m pytest tests/test_promote_covid.py -v`
Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
git add src/who_data_lakehouse/promote/__init__.py src/who_data_lakehouse/promote/covid.py tests/test_promote_covid.py
git commit -m "feat: add COVID CSV → silver parquet promotion"
```

---

### Task 2: GHED XLSX Promotion

**Files:**
- Create: `src/who_data_lakehouse/promote/ghed.py`
- Create: `tests/test_promote_ghed.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_promote_ghed.py`:
```python
"""Tests for GHED XLSX → silver parquet promotion."""
from pathlib import Path
import pandas as pd
import pytest

from who_data_lakehouse.promote.ghed import promote_ghed


@pytest.fixture
def tmp_raw(tmp_path):
    raw_dir = tmp_path / "raw" / "ghed"
    raw_dir.mkdir(parents=True)
    # GHED has a 'Data' sheet with location/code/region/income/year + indicators
    df = pd.DataFrame({
        "location": ["Afghanistan", "Albania"],
        "code": ["AFG", "ALB"],
        "region": ["EMRO", "EURO"],
        "income": ["Low", "Upper middle"],
        "year": [2020, 2020],
        "che_gdp": [10.2, 5.3],
        "gghed_che": [20.1, 55.0],
    })
    with pd.ExcelWriter(raw_dir / "GHED_data.XLSX", engine="openpyxl") as w:
        df.to_excel(w, sheet_name="Data", index=False)
        pd.DataFrame({"code": ["che_gdp"], "description": ["CHE as % GDP"]}).to_excel(
            w, sheet_name="Codebook", index=False
        )
    return tmp_path


@pytest.fixture
def tmp_silver(tmp_path):
    return tmp_path / "silver" / "ghed"


def test_promote_ghed_creates_parquet(tmp_raw, tmp_silver):
    result = promote_ghed(tmp_raw / "raw" / "ghed" / "GHED_data.XLSX", tmp_silver)
    assert result["status"] == "promoted"
    assert (tmp_silver / "ghed_data.parquet").exists()
    df = pd.read_parquet(tmp_silver / "ghed_data.parquet")
    assert len(df) == 2
    assert "location" in df.columns
    assert "che_gdp" in df.columns


def test_promote_ghed_extracts_codebook(tmp_raw, tmp_silver):
    result = promote_ghed(tmp_raw / "raw" / "ghed" / "GHED_data.XLSX", tmp_silver)
    codebook_path = tmp_silver / "ghed_codebook.parquet"
    assert codebook_path.exists()
    df = pd.read_parquet(codebook_path)
    assert len(df) == 1


def test_promote_ghed_skips_existing(tmp_raw, tmp_silver):
    promote_ghed(tmp_raw / "raw" / "ghed" / "GHED_data.XLSX", tmp_silver)
    result = promote_ghed(tmp_raw / "raw" / "ghed" / "GHED_data.XLSX", tmp_silver, skip_existing=True)
    assert result["status"] == "skipped"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd C:\Projects\who-data-lakehouse && python -m pytest tests/test_promote_ghed.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Implement GHED promotion**

Create `src/who_data_lakehouse/promote/ghed.py`:
```python
"""Promote raw GHED XLSX to silver parquet."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from who_data_lakehouse.normalize import normalize_columns

# Sheets to extract from GHED workbook
KNOWN_SHEETS = {
    "Data": "ghed_data.parquet",
    "Codebook": "ghed_codebook.parquet",
    "Metadata": "ghed_metadata.parquet",
}


def promote_ghed(
    xlsx_path: Path,
    silver_dir: Path,
    skip_existing: bool = False,
) -> dict:
    """Convert GHED XLSX sheets to silver parquet files.

    Returns dict with keys: file, status, sheets.
    """
    silver_dir.mkdir(parents=True, exist_ok=True)
    main_out = silver_dir / "ghed_data.parquet"

    if skip_existing and main_out.exists():
        return {"file": xlsx_path.name, "status": "skipped", "sheets": []}

    xls = pd.ExcelFile(xlsx_path, engine="openpyxl")
    promoted_sheets = []

    for sheet_name, parquet_name in KNOWN_SHEETS.items():
        if sheet_name not in xls.sheet_names:
            continue
        df = pd.read_excel(xls, sheet_name=sheet_name)
        df = normalize_columns(df)
        out_path = silver_dir / parquet_name
        df.to_parquet(out_path, index=False)
        promoted_sheets.append({"sheet": sheet_name, "rows": len(df), "path": str(out_path)})

    # Also promote any extra sheets not in KNOWN_SHEETS
    for sheet_name in xls.sheet_names:
        if sheet_name in KNOWN_SHEETS or sheet_name.lower() in ("version",):
            continue
        df = pd.read_excel(xls, sheet_name=sheet_name)
        if df.empty:
            continue
        df = normalize_columns(df)
        safe_name = sheet_name.lower().replace(" ", "_").replace("-", "_")
        out_path = silver_dir / f"ghed_{safe_name}.parquet"
        df.to_parquet(out_path, index=False)
        promoted_sheets.append({"sheet": sheet_name, "rows": len(df), "path": str(out_path)})

    return {"file": xlsx_path.name, "status": "promoted", "sheets": promoted_sheets}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd C:\Projects\who-data-lakehouse && python -m pytest tests/test_promote_ghed.py -v`
Expected: 3 passed

- [ ] **Step 5: Commit**

```bash
git add src/who_data_lakehouse/promote/ghed.py tests/test_promote_ghed.py
git commit -m "feat: add GHED XLSX → silver parquet promotion"
```

---

### Task 3: WHS XLSX Promotion

**Files:**
- Create: `src/who_data_lakehouse/promote/whs.py`
- Create: `tests/test_promote_whs.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_promote_whs.py`:
```python
"""Tests for WHS XLSX → silver parquet promotion."""
from pathlib import Path
import pandas as pd
import pytest

from who_data_lakehouse.promote.whs import promote_whs


@pytest.fixture
def tmp_raw(tmp_path):
    raw_dir = tmp_path / "raw" / "world_health_statistics"
    raw_dir.mkdir(parents=True)
    # web_download.xlsx has a 'data' sheet with structured indicator data
    df = pd.DataFrame({
        "IND_NAME": ["Life expectancy", "Life expectancy"],
        "DIM_GEO_NAME": ["Afghanistan", "Albania"],
        "IND_CODE": ["WHOSIS_000001", "WHOSIS_000001"],
        "DIM_GEO_CODE": ["AFG", "ALB"],
        "DIM_TIME_YEAR": [2020, 2020],
        "DIM_1_CODE": [None, None],
        "VALUE_NUMERIC": [64.8, 78.6],
        "VALUE_STRING": [None, None],
        "VALUE_COMMENTS": [None, None],
    })
    with pd.ExcelWriter(raw_dir / "web_download.xlsx", engine="openpyxl") as w:
        pd.DataFrame({"info": ["readme"]}).to_excel(w, sheet_name="readme", index=False)
        df.to_excel(w, sheet_name="data", index=False)
    return tmp_path


@pytest.fixture
def tmp_silver(tmp_path):
    return tmp_path / "silver" / "world_health_statistics"


def test_promote_whs_creates_parquet(tmp_raw, tmp_silver):
    results = promote_whs(tmp_raw / "raw" / "world_health_statistics", tmp_silver)
    assert len(results) >= 1
    assert results[0]["status"] == "promoted"
    parquet = tmp_silver / "web_download.parquet"
    assert parquet.exists()
    df = pd.read_parquet(parquet)
    assert len(df) == 2
    assert "ind_name" in df.columns
    assert "dim_geo_code" in df.columns


def test_promote_whs_skips_readme_sheets(tmp_raw, tmp_silver):
    promote_whs(tmp_raw / "raw" / "world_health_statistics", tmp_silver)
    # Should only have the data sheet, not readme
    files = list(tmp_silver.glob("*.parquet"))
    names = [f.stem for f in files]
    assert "web_download" in names
    # readme sheet should not become a separate file
    assert "web_download_readme" not in names
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd C:\Projects\who-data-lakehouse && python -m pytest tests/test_promote_whs.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Implement WHS promotion**

Create `src/who_data_lakehouse/promote/whs.py`:
```python
"""Promote raw World Health Statistics XLSX to silver parquet."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from who_data_lakehouse.normalize import normalize_columns

# Sheets to skip (metadata/documentation, not data)
SKIP_SHEETS = {"readme", "read me", "notes", "explanatory notes", "footnotes"}


def promote_whs(
    raw_dir: Path,
    silver_dir: Path,
    skip_existing: bool = False,
) -> list[dict]:
    """Convert each WHS XLSX in raw_dir to silver parquet.

    For multi-sheet workbooks, the first data sheet (not readme/notes) is
    saved as <stem>.parquet. Additional data sheets are saved as
    <stem>_<sheet>.parquet.
    """
    silver_dir.mkdir(parents=True, exist_ok=True)
    results = []

    for xlsx_path in sorted(raw_dir.glob("*.xlsx")) + sorted(raw_dir.glob("*.xls")):
        stem = xlsx_path.stem
        main_out = silver_dir / f"{stem}.parquet"

        if skip_existing and main_out.exists():
            results.append({"file": xlsx_path.name, "status": "skipped", "rows": None})
            continue

        xls = pd.ExcelFile(xlsx_path, engine="openpyxl")
        data_sheets = [s for s in xls.sheet_names if s.lower().strip() not in SKIP_SHEETS]

        if not data_sheets:
            results.append({"file": xlsx_path.name, "status": "empty", "rows": 0})
            continue

        total_rows = 0
        for idx, sheet_name in enumerate(data_sheets):
            df = pd.read_excel(xls, sheet_name=sheet_name)
            if df.empty:
                continue
            df = normalize_columns(df)
            if idx == 0:
                out_path = main_out
            else:
                safe = sheet_name.lower().replace(" ", "_").replace("-", "_")
                out_path = silver_dir / f"{stem}_{safe}.parquet"
            df.to_parquet(out_path, index=False)
            total_rows += len(df)

        results.append({"file": xlsx_path.name, "status": "promoted", "rows": total_rows})

    return results
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd C:\Projects\who-data-lakehouse && python -m pytest tests/test_promote_whs.py -v`
Expected: 2 passed

- [ ] **Step 5: Commit**

```bash
git add src/who_data_lakehouse/promote/whs.py tests/test_promote_whs.py
git commit -m "feat: add WHS XLSX → silver parquet promotion"
```

---

### Task 4: GLAAS XLSX Promotion

**Files:**
- Create: `src/who_data_lakehouse/promote/glaas.py`
- Create: `tests/test_promote_glaas.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_promote_glaas.py`:
```python
"""Tests for GLAAS XLSX → silver parquet promotion."""
from pathlib import Path
import pandas as pd
import pytest

from who_data_lakehouse.promote.glaas import promote_glaas


@pytest.fixture
def tmp_raw(tmp_path):
    raw_dir = tmp_path / "raw" / "glaas"
    raw_dir.mkdir(parents=True)
    # GLAAS has Section A/B/C/D Data + Response Code sheets + Participants
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
    # Each sheet becomes its own parquet
    assert (tmp_silver / "section_a_data.parquet").exists()
    assert (tmp_silver / "section_a_response_code.parquet").exists()
    assert (tmp_silver / "glaas_participants_2025.parquet").exists()


def test_promote_glaas_normalizes_columns(tmp_raw, tmp_silver):
    promote_glaas(tmp_raw / "raw" / "glaas" / "glaas_test.xlsx", tmp_silver)
    df = pd.read_parquet(tmp_silver / "section_a_data.parquet")
    assert "country" in df.columns
    assert len(df) == 2
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd C:\Projects\who-data-lakehouse && python -m pytest tests/test_promote_glaas.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Implement GLAAS promotion**

Create `src/who_data_lakehouse/promote/glaas.py`:
```python
"""Promote raw GLAAS XLSX to silver parquet (one file per sheet)."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from who_data_lakehouse.normalize import normalize_columns


def promote_glaas(
    xlsx_path: Path,
    silver_dir: Path,
    skip_existing: bool = False,
) -> dict:
    """Convert each sheet in the GLAAS workbook to a silver parquet file.

    Returns dict with keys: file, status, sheets.
    """
    silver_dir.mkdir(parents=True, exist_ok=True)

    # Use first sheet name as existence check
    if skip_existing and any(silver_dir.glob("*.parquet")):
        return {"file": xlsx_path.name, "status": "skipped", "sheets": []}

    xls = pd.ExcelFile(xlsx_path, engine="openpyxl")
    promoted = []

    for sheet_name in xls.sheet_names:
        df = pd.read_excel(xls, sheet_name=sheet_name)
        if df.empty:
            continue
        df = normalize_columns(df)
        safe = sheet_name.lower().strip().replace(" ", "_").replace("-", "_").replace("/", "_")
        out_path = silver_dir / f"{safe}.parquet"
        df.to_parquet(out_path, index=False)
        promoted.append({"sheet": sheet_name, "rows": len(df), "path": str(out_path)})

    return {"file": xlsx_path.name, "status": "promoted", "sheets": promoted}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd C:\Projects\who-data-lakehouse && python -m pytest tests/test_promote_glaas.py -v`
Expected: 2 passed

- [ ] **Step 5: Commit**

```bash
git add src/who_data_lakehouse/promote/glaas.py tests/test_promote_glaas.py
git commit -m "feat: add GLAAS XLSX → silver parquet promotion"
```

---

### Task 5: HIDR XLSX Promotion

**Files:**
- Create: `src/who_data_lakehouse/promote/hidr.py`
- Create: `tests/test_promote_hidr.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_promote_hidr.py`:
```python
"""Tests for HIDR XLSX → silver parquet promotion."""
from pathlib import Path
import pandas as pd
import pytest

from who_data_lakehouse.promote.hidr import promote_hidr


@pytest.fixture
def tmp_raw(tmp_path):
    raw_dir = tmp_path / "raw" / "hidr" / "datasets"
    raw_dir.mkdir(parents=True)
    # HIDR datasets have the standard epidemiological schema
    df = pd.DataFrame({
        "setting": ["Kenya", "Kenya"],
        "date": ["2020", "2021"],
        "source": ["DHS", "DHS"],
        "indicator_abbr": ["IMR", "IMR"],
        "indicator_name": ["Infant mortality rate", "Infant mortality rate"],
        "dimension": ["Total", "Total"],
        "subgroup": ["Total", "Total"],
        "estimate": [30.5, 28.1],
        "se": [1.2, 1.1],
        "ci_lb": [28.1, 25.9],
        "ci_ub": [32.9, 30.3],
        "population": [5000000, 5100000],
        "iso3": ["KEN", "KEN"],
        "dataset_id": ["rep_dhs_ahn", "rep_dhs_ahn"],
    })
    df.to_excel(raw_dir / "rep_dhs_ahn.xlsx", index=False, engine="openpyxl")
    df2 = df.copy()
    df2["dataset_id"] = "rep_covid_cfr"
    df2.to_excel(raw_dir / "rep_covid_cfr.xlsx", index=False, engine="openpyxl")
    return tmp_path


@pytest.fixture
def tmp_silver(tmp_path):
    return tmp_path / "silver" / "hidr"


def test_promote_hidr_creates_parquets(tmp_raw, tmp_silver):
    results = promote_hidr(tmp_raw / "raw" / "hidr" / "datasets", tmp_silver)
    assert len(results) == 2
    assert all(r["status"] == "promoted" for r in results)
    assert (tmp_silver / "rep_dhs_ahn.parquet").exists()
    assert (tmp_silver / "rep_covid_cfr.parquet").exists()


def test_promote_hidr_normalizes_columns(tmp_raw, tmp_silver):
    promote_hidr(tmp_raw / "raw" / "hidr" / "datasets", tmp_silver)
    df = pd.read_parquet(tmp_silver / "rep_dhs_ahn.parquet")
    assert "indicator_name" in df.columns
    assert "iso3" in df.columns
    assert len(df) == 2


def test_promote_hidr_skips_existing(tmp_raw, tmp_silver):
    promote_hidr(tmp_raw / "raw" / "hidr" / "datasets", tmp_silver)
    results = promote_hidr(tmp_raw / "raw" / "hidr" / "datasets", tmp_silver, skip_existing=True)
    assert all(r["status"] == "skipped" for r in results)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd C:\Projects\who-data-lakehouse && python -m pytest tests/test_promote_hidr.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Implement HIDR promotion**

Create `src/who_data_lakehouse/promote/hidr.py`:
```python
"""Promote raw HIDR XLSX datasets to silver parquet."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from who_data_lakehouse.normalize import normalize_columns


def promote_hidr(
    datasets_dir: Path,
    silver_dir: Path,
    skip_existing: bool = False,
) -> list[dict]:
    """Convert each HIDR XLSX in datasets_dir to a silver parquet file.

    Returns a list of dicts with keys: file, status, rows, path.
    """
    silver_dir.mkdir(parents=True, exist_ok=True)
    results = []

    for xlsx_path in sorted(datasets_dir.glob("*.xlsx")):
        stem = xlsx_path.stem
        parquet_path = silver_dir / f"{stem}.parquet"

        if skip_existing and parquet_path.exists():
            results.append({"file": xlsx_path.name, "status": "skipped", "rows": None, "path": str(parquet_path)})
            continue

        try:
            df = pd.read_excel(xlsx_path, engine="openpyxl")
        except Exception as exc:
            results.append({"file": xlsx_path.name, "status": "error", "rows": 0, "error": str(exc)})
            continue

        if df.empty:
            results.append({"file": xlsx_path.name, "status": "empty", "rows": 0, "path": None})
            continue

        df = normalize_columns(df)
        df.to_parquet(parquet_path, index=False)
        results.append({"file": xlsx_path.name, "status": "promoted", "rows": len(df), "path": str(parquet_path)})

    return results
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd C:\Projects\who-data-lakehouse && python -m pytest tests/test_promote_hidr.py -v`
Expected: 3 passed

- [ ] **Step 5: Commit**

```bash
git add src/who_data_lakehouse/promote/hidr.py tests/test_promote_hidr.py
git commit -m "feat: add HIDR XLSX → silver parquet promotion"
```

---

### Task 6: GHO Facts Promotion

**Files:**
- Create: `src/who_data_lakehouse/promote/gho_facts.py`
- Create: `tests/test_promote_gho_facts.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_promote_gho_facts.py`:
```python
"""Tests for GHO facts JSONL.gz → silver parquet promotion."""
from pathlib import Path
import gzip
import json
import pandas as pd
import pytest

from who_data_lakehouse.promote.gho_facts import promote_gho_facts


@pytest.fixture
def tmp_raw(tmp_path):
    raw_dir = tmp_path / "raw" / "gho" / "facts"
    raw_dir.mkdir(parents=True)
    records = [
        {
            "Id": 1,
            "IndicatorCode": "TEST_001",
            "SpatialDimType": "COUNTRY",
            "SpatialDim": "KEN",
            "TimeDimType": "YEAR",
            "TimeDim": 2020,
            "Dim1Type": "SEX",
            "Dim1": "BTSX",
            "Dim2Type": None,
            "Dim2": None,
            "Dim3Type": None,
            "Dim3": None,
            "Value": "64.8",
            "NumericValue": 64.8,
            "Low": 62.0,
            "High": 67.0,
            "Date": "2024-01-15T00:00:00+00:00",
            "TimeDimensionValue": 2020,
            "TimeDimensionBegin": "2020-01-01",
            "TimeDimensionEnd": "2020-12-31",
            "DataSourceDimType": None,
            "DataSourceDim": None,
            "Comments": None,
            "ParentLocationCode": "AFR",
            "ParentLocation": "Africa",
        },
        {
            "Id": 2,
            "IndicatorCode": "TEST_001",
            "SpatialDimType": "COUNTRY",
            "SpatialDim": "GHA",
            "TimeDimType": "YEAR",
            "TimeDim": 2020,
            "Dim1Type": "SEX",
            "Dim1": "BTSX",
            "Dim2Type": None,
            "Dim2": None,
            "Dim3Type": None,
            "Dim3": None,
            "Value": "63.5",
            "NumericValue": 63.5,
            "Low": 61.0,
            "High": 66.0,
            "Date": "2024-01-15T00:00:00+00:00",
            "TimeDimensionValue": 2020,
            "TimeDimensionBegin": "2020-01-01",
            "TimeDimensionEnd": "2020-12-31",
            "DataSourceDimType": None,
            "DataSourceDim": None,
            "Comments": None,
            "ParentLocationCode": "AFR",
            "ParentLocation": "Africa",
        },
    ]
    path = raw_dir / "TEST_001.jsonl.gz"
    with gzip.open(path, "wt", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec) + "\n")
    return tmp_path


@pytest.fixture
def tmp_silver(tmp_path):
    return tmp_path / "silver" / "gho"


def test_promote_gho_facts_creates_wide_and_dims(tmp_raw, tmp_silver):
    results = promote_gho_facts(tmp_raw / "raw" / "gho" / "facts", tmp_silver)
    assert len(results) == 1
    assert results[0]["status"] == "promoted"
    wide_path = tmp_silver / "observations_wide" / "TEST_001.parquet"
    dims_path = tmp_silver / "observation_dimensions" / "TEST_001.parquet"
    assert wide_path.exists()
    assert dims_path.exists()
    df_wide = pd.read_parquet(wide_path)
    assert len(df_wide) == 2
    assert "observation_id" in df_wide.columns
    assert "numeric_value" in df_wide.columns


def test_promote_gho_facts_skips_existing(tmp_raw, tmp_silver):
    promote_gho_facts(tmp_raw / "raw" / "gho" / "facts", tmp_silver)
    results = promote_gho_facts(tmp_raw / "raw" / "gho" / "facts", tmp_silver, skip_existing=True)
    assert results[0]["status"] == "skipped"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd C:\Projects\who-data-lakehouse && python -m pytest tests/test_promote_gho_facts.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Implement GHO facts promotion**

Create `src/who_data_lakehouse/promote/gho_facts.py`:
```python
"""Promote raw GHO facts JSONL.gz to silver parquet (observations_wide + observation_dimensions)."""
from __future__ import annotations

import gzip
import json
from pathlib import Path

import pandas as pd

from who_data_lakehouse.normalize import normalize_gho_observations


def promote_gho_facts(
    facts_dir: Path,
    silver_dir: Path,
    skip_existing: bool = False,
) -> list[dict]:
    """Convert each indicator JSONL.gz file to observations_wide + observation_dimensions parquet.

    Returns a list of dicts with keys: file, status, rows, indicator_code.
    """
    wide_dir = silver_dir / "observations_wide"
    dims_dir = silver_dir / "observation_dimensions"
    wide_dir.mkdir(parents=True, exist_ok=True)
    dims_dir.mkdir(parents=True, exist_ok=True)

    results = []

    for gz_path in sorted(facts_dir.glob("*.jsonl.gz")):
        indicator_code = gz_path.stem.replace(".jsonl", "")
        wide_path = wide_dir / f"{indicator_code}.parquet"
        dims_path = dims_dir / f"{indicator_code}.parquet"

        if skip_existing and wide_path.exists() and dims_path.exists():
            results.append({
                "file": gz_path.name, "status": "skipped",
                "rows": None, "indicator_code": indicator_code,
            })
            continue

        # Read JSONL.gz
        records = []
        with gzip.open(gz_path, "rt", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    records.append(json.loads(line))

        if not records:
            results.append({
                "file": gz_path.name, "status": "empty",
                "rows": 0, "indicator_code": indicator_code,
            })
            continue

        wide_frame, dims_frame = normalize_gho_observations(records)
        wide_frame.to_parquet(wide_path, index=False)
        dims_frame.to_parquet(dims_path, index=False)

        results.append({
            "file": gz_path.name, "status": "promoted",
            "rows": len(wide_frame), "indicator_code": indicator_code,
        })

    return results
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd C:\Projects\who-data-lakehouse && python -m pytest tests/test_promote_gho_facts.py -v`
Expected: 2 passed

- [ ] **Step 5: Commit**

```bash
git add src/who_data_lakehouse/promote/gho_facts.py tests/test_promote_gho_facts.py
git commit -m "feat: add GHO facts JSONL.gz → silver parquet promotion"
```

---

### Task 7: Master Catalog Builder

**Files:**
- Create: `src/who_data_lakehouse/catalog.py`
- Create: `tests/test_catalog.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_catalog.py`:
```python
"""Tests for master catalog builder."""
from pathlib import Path
import pandas as pd
import pytest

from who_data_lakehouse.catalog import build_catalog


@pytest.fixture
def silver_tree(tmp_path):
    """Create a minimal silver directory tree with parquet files from multiple sources."""
    silver = tmp_path / "silver"

    # COVID
    covid_dir = silver / "covid"
    covid_dir.mkdir(parents=True)
    pd.DataFrame({
        "date_reported": pd.to_datetime(["2020-03-01", "2024-12-31"]),
        "country_code": ["KEN", "GHA"],
        "country": ["Kenya", "Ghana"],
        "new_cases": [10, 20],
    }).to_parquet(covid_dir / "WHO-COVID-19-global-data.parquet", index=False)

    # GHO observations_wide
    gho_wide = silver / "gho" / "observations_wide"
    gho_wide.mkdir(parents=True)
    pd.DataFrame({
        "indicator_code": ["WHOSIS_000001"] * 2,
        "spatial_dim": ["KEN", "GHA"],
        "numeric_value": [64.8, 63.5],
    }).to_parquet(gho_wide / "WHOSIS_000001.parquet", index=False)

    # HIDR
    hidr_dir = silver / "hidr"
    hidr_dir.mkdir(parents=True)
    pd.DataFrame({
        "setting": ["Kenya"],
        "indicator_name": ["Infant mortality rate"],
        "estimate": [30.5],
        "iso3": ["KEN"],
        "dataset_id": ["rep_dhs_ahn"],
    }).to_parquet(hidr_dir / "rep_dhs_ahn.parquet", index=False)

    # GHED
    ghed_dir = silver / "ghed"
    ghed_dir.mkdir(parents=True)
    pd.DataFrame({
        "location": ["Kenya"],
        "code": ["KEN"],
        "year": [2020],
        "che_gdp": [5.2],
    }).to_parquet(ghed_dir / "ghed_data.parquet", index=False)

    # XMart
    xmart_dir = silver / "xmart" / "mncah" / "BD_EUROCAT"
    xmart_dir.mkdir(parents=True)
    pd.DataFrame({
        "birth_year": [2020],
        "total_cases": [100],
        "status": ["active"],
    }).to_parquet(xmart_dir / "part-00000.parquet", index=False)

    return silver


def test_build_catalog_returns_dataframe(silver_tree):
    catalog = build_catalog(silver_tree)
    assert isinstance(catalog, pd.DataFrame)
    assert len(catalog) >= 5  # covid, gho, hidr, ghed, xmart


def test_catalog_has_required_columns(silver_tree):
    catalog = build_catalog(silver_tree)
    required = ["source", "dataset", "description", "rows", "columns", "silver_path"]
    for col in required:
        assert col in catalog.columns, f"Missing column: {col}"


def test_catalog_row_counts_correct(silver_tree):
    catalog = build_catalog(silver_tree)
    covid_row = catalog[catalog["dataset"] == "WHO-COVID-19-global-data"].iloc[0]
    assert covid_row["rows"] == 2
    assert covid_row["source"] == "covid"


def test_catalog_saves_to_parquet(silver_tree, tmp_path):
    catalog = build_catalog(silver_tree, output_path=tmp_path / "catalog.parquet")
    assert (tmp_path / "catalog.parquet").exists()
    loaded = pd.read_parquet(tmp_path / "catalog.parquet")
    assert len(loaded) == len(catalog)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd C:\Projects\who-data-lakehouse && python -m pytest tests/test_catalog.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Implement the catalog builder**

Create `src/who_data_lakehouse/catalog.py`:
```python
"""Master catalog builder: scans silver layer and builds a unified dataset index."""
from __future__ import annotations

from pathlib import Path

import pandas as pd


# Source-specific scan strategies
SOURCE_RULES = {
    "covid": {"pattern": "*.parquet", "depth": "flat"},
    "ghed": {"pattern": "*.parquet", "depth": "flat"},
    "glaas": {"pattern": "*.parquet", "depth": "flat"},
    "world_health_statistics": {"pattern": "*.parquet", "depth": "flat"},
    "hidr": {"pattern": "*.parquet", "depth": "flat"},
    "datadot": {"pattern": "*.parquet", "depth": "flat"},
}

# Sources with nested structure
NESTED_SOURCES = {
    "gho": {
        "observations_wide": {"pattern": "*.parquet", "prefix": "gho_indicator"},
    },
    "xmart": None,  # special: service/entity_set/part-*.parquet
}

# Human-readable descriptions based on dataset names
DESCRIPTION_HINTS = {
    "covid": "WHO COVID-19 surveillance data",
    "ghed": "Global Health Expenditure Database",
    "glaas": "GLAAS water/sanitation/hygiene survey",
    "world_health_statistics": "World Health Statistics annex data",
    "hidr": "Health Inequality Data Repository",
    "datadot": "WHO data.who.int portal metadata",
    "gho": "Global Health Observatory indicator",
    "xmart": "WHO XMart OData service",
}


def _scan_flat_source(source: str, source_dir: Path) -> list[dict]:
    """Scan a flat directory of parquet files."""
    rows = []
    for pq in sorted(source_dir.glob("*.parquet")):
        df = pd.read_parquet(pq)
        rows.append({
            "source": source,
            "dataset": pq.stem,
            "description": f"{DESCRIPTION_HINTS.get(source, source)}: {pq.stem}",
            "rows": len(df),
            "columns": len(df.columns),
            "column_names": ", ".join(df.columns[:20]),
            "silver_path": str(pq),
        })
    return rows


def _scan_gho(source_dir: Path) -> list[dict]:
    """Scan GHO silver: reference files + observations_wide indicators."""
    rows = []

    # Reference files at top level
    for pq in sorted(source_dir.glob("*.parquet")):
        df = pd.read_parquet(pq)
        rows.append({
            "source": "gho",
            "dataset": pq.stem,
            "description": f"GHO reference table: {pq.stem}",
            "rows": len(df),
            "columns": len(df.columns),
            "column_names": ", ".join(df.columns[:20]),
            "silver_path": str(pq),
        })

    # Observation wide files
    wide_dir = source_dir / "observations_wide"
    if wide_dir.exists():
        for pq in sorted(wide_dir.glob("*.parquet")):
            df = pd.read_parquet(pq)
            if df.empty:
                continue
            rows.append({
                "source": "gho",
                "dataset": f"indicator/{pq.stem}",
                "description": f"GHO indicator: {pq.stem}",
                "rows": len(df),
                "columns": len(df.columns),
                "column_names": ", ".join(df.columns[:20]),
                "silver_path": str(pq),
            })
    return rows


def _scan_xmart(source_dir: Path) -> list[dict]:
    """Scan XMart silver: service/entity_set/part-*.parquet."""
    rows = []
    for service_dir in sorted(source_dir.iterdir()):
        if not service_dir.is_dir():
            continue
        service = service_dir.name
        for entity_dir in sorted(service_dir.iterdir()):
            if not entity_dir.is_dir():
                continue
            parts = sorted(entity_dir.glob("part-*.parquet"))
            if not parts:
                continue
            total_rows = 0
            col_names = ""
            n_cols = 0
            for part in parts:
                df = pd.read_parquet(part)
                total_rows += len(df)
                if not col_names:
                    col_names = ", ".join(df.columns[:20])
                    n_cols = len(df.columns)
            if total_rows == 0:
                continue
            rows.append({
                "source": f"xmart/{service}",
                "dataset": entity_dir.name,
                "description": f"XMart {service}: {entity_dir.name}",
                "rows": total_rows,
                "columns": n_cols,
                "column_names": col_names,
                "silver_path": str(entity_dir),
            })
    return rows


def build_catalog(
    silver_dir: Path,
    output_path: Path | None = None,
) -> pd.DataFrame:
    """Build a master catalog of all datasets in the silver layer.

    Scans every source subdirectory and produces one row per dataset with
    source, name, description, row count, column count, and path.
    """
    all_rows: list[dict] = []

    for child in sorted(silver_dir.iterdir()):
        if not child.is_dir():
            continue
        name = child.name

        if name == "gho":
            all_rows.extend(_scan_gho(child))
        elif name == "xmart":
            all_rows.extend(_scan_xmart(child))
        elif name in SOURCE_RULES:
            all_rows.extend(_scan_flat_source(name, child))
        else:
            # Unknown source — scan flat
            all_rows.extend(_scan_flat_source(name, child))

    catalog = pd.DataFrame.from_records(all_rows)
    if catalog.empty:
        catalog = pd.DataFrame(columns=["source", "dataset", "description", "rows", "columns", "column_names", "silver_path"])

    catalog = catalog.sort_values(["source", "dataset"]).reset_index(drop=True)

    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        catalog.to_parquet(output_path, index=False)

    return catalog
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd C:\Projects\who-data-lakehouse && python -m pytest tests/test_catalog.py -v`
Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
git add src/who_data_lakehouse/catalog.py tests/test_catalog.py
git commit -m "feat: add master catalog builder for silver layer"
```

---

### Task 8: CLI Commands (promote, build-catalog, search)

**Files:**
- Modify: `src/who_data_lakehouse/cli.py`
- Create: `tests/test_search_cli.py`

- [ ] **Step 1: Write the failing test for search**

Create `tests/test_search_cli.py`:
```python
"""Tests for the search CLI functionality."""
from pathlib import Path
import pandas as pd
import pytest

from who_data_lakehouse.catalog import build_catalog, search_catalog


@pytest.fixture
def sample_catalog(tmp_path):
    silver = tmp_path / "silver"
    covid_dir = silver / "covid"
    covid_dir.mkdir(parents=True)
    pd.DataFrame({
        "date_reported": pd.to_datetime(["2020-03-01"]),
        "country": ["Kenya"],
        "new_cases": [10],
    }).to_parquet(covid_dir / "WHO-COVID-19-global-data.parquet", index=False)

    hidr_dir = silver / "hidr"
    hidr_dir.mkdir(parents=True)
    pd.DataFrame({
        "setting": ["Kenya"],
        "indicator_name": ["Tuberculosis incidence"],
        "estimate": [250.0],
        "iso3": ["KEN"],
    }).to_parquet(hidr_dir / "rep_tb_incidence.parquet", index=False)

    ghed_dir = silver / "ghed"
    ghed_dir.mkdir(parents=True)
    pd.DataFrame({
        "location": ["Kenya"],
        "code": ["KEN"],
        "year": [2020],
        "che_gdp": [5.2],
    }).to_parquet(ghed_dir / "ghed_data.parquet", index=False)

    catalog = build_catalog(silver)
    return catalog


def test_search_by_keyword(sample_catalog):
    results = search_catalog(sample_catalog, keyword="covid")
    assert len(results) >= 1
    assert all("covid" in r.lower() for r in results["dataset"].tolist() + results["description"].tolist())


def test_search_by_source(sample_catalog):
    results = search_catalog(sample_catalog, source="hidr")
    assert len(results) >= 1
    assert all(r == "hidr" for r in results["source"])


def test_search_no_match(sample_catalog):
    results = search_catalog(sample_catalog, keyword="nonexistent_xyz")
    assert len(results) == 0


def test_search_case_insensitive(sample_catalog):
    results = search_catalog(sample_catalog, keyword="COVID")
    assert len(results) >= 1
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd C:\Projects\who-data-lakehouse && python -m pytest tests/test_search_cli.py -v`
Expected: FAIL with `ImportError: cannot import name 'search_catalog'`

- [ ] **Step 3: Add search_catalog to catalog.py**

Add to the bottom of `src/who_data_lakehouse/catalog.py`:
```python
def search_catalog(
    catalog: pd.DataFrame,
    keyword: str | None = None,
    source: str | None = None,
) -> pd.DataFrame:
    """Filter the catalog by keyword (searches dataset + description) and/or source."""
    mask = pd.Series(True, index=catalog.index)

    if source is not None:
        mask &= catalog["source"].str.lower() == source.lower()

    if keyword is not None:
        kw = keyword.lower()
        text_match = (
            catalog["dataset"].str.lower().str.contains(kw, na=False)
            | catalog["description"].str.lower().str.contains(kw, na=False)
            | catalog["column_names"].str.lower().str.contains(kw, na=False)
        )
        mask &= text_match

    return catalog.loc[mask].reset_index(drop=True)
```

- [ ] **Step 4: Run search tests to verify they pass**

Run: `cd C:\Projects\who-data-lakehouse && python -m pytest tests/test_search_cli.py -v`
Expected: 4 passed

- [ ] **Step 5: Add CLI commands to cli.py**

Add three new subcommands to `build_parser()` in `src/who_data_lakehouse/cli.py`. Add these imports at the top:
```python
from who_data_lakehouse.promote.covid import promote_covid
from who_data_lakehouse.promote.ghed import promote_ghed
from who_data_lakehouse.promote.whs import promote_whs
from who_data_lakehouse.promote.glaas import promote_glaas
from who_data_lakehouse.promote.hidr import promote_hidr
from who_data_lakehouse.promote.gho_facts import promote_gho_facts
from who_data_lakehouse.catalog import build_catalog, search_catalog
```

Add subparsers inside `build_parser()`:
```python
# --- promote ---
sp_promote = subparsers.add_parser("promote", help="Promote raw data to silver parquet")
sp_promote.add_argument("--source", choices=["all", "covid", "ghed", "whs", "glaas", "hidr", "gho-facts"], default="all")
sp_promote.add_argument("--skip-existing", action="store_true")
sp_promote.set_defaults(handler=command_promote)

# --- build-catalog ---
sp_catalog = subparsers.add_parser("build-catalog", help="Build master catalog from silver layer")
sp_catalog.set_defaults(handler=command_build_catalog)

# --- search ---
sp_search = subparsers.add_parser("search", help="Search the master catalog")
sp_search.add_argument("keyword", nargs="?", help="Search term (matches dataset name, description, columns)")
sp_search.add_argument("--source", help="Filter by source (e.g., covid, gho, xmart/wiise)")
sp_search.set_defaults(handler=command_search)
```

Add handler functions:
```python
def command_promote(args):
    source = args.source
    skip = args.skip_existing
    results = {}

    if source in ("all", "covid"):
        raw = RAW_DIR / "covid"
        if raw.exists():
            results["covid"] = promote_covid(raw, SILVER_DIR / "covid", skip_existing=skip)

    if source in ("all", "ghed"):
        raw = RAW_DIR / "ghed" / "GHED_data.XLSX"
        if raw.exists():
            results["ghed"] = promote_ghed(raw, SILVER_DIR / "ghed", skip_existing=skip)

    if source in ("all", "whs"):
        raw = RAW_DIR / "world_health_statistics"
        if raw.exists():
            results["whs"] = promote_whs(raw, SILVER_DIR / "world_health_statistics", skip_existing=skip)

    if source in ("all", "glaas"):
        glaas_files = sorted((RAW_DIR / "glaas").glob("*.xlsx")) if (RAW_DIR / "glaas").exists() else []
        if glaas_files:
            results["glaas"] = promote_glaas(glaas_files[0], SILVER_DIR / "glaas", skip_existing=skip)

    if source in ("all", "hidr"):
        raw = RAW_DIR / "hidr" / "datasets"
        if raw.exists():
            results["hidr"] = promote_hidr(raw, SILVER_DIR / "hidr", skip_existing=skip)

    if source in ("all", "gho-facts"):
        raw = RAW_DIR / "gho" / "facts"
        if raw.exists():
            results["gho_facts"] = promote_gho_facts(raw, SILVER_DIR / "gho", skip_existing=skip)

    # Print summary
    for src, res_list in results.items():
        if isinstance(res_list, list):
            promoted = sum(1 for r in res_list if r.get("status") == "promoted")
            skipped = sum(1 for r in res_list if r.get("status") == "skipped")
            total_rows = sum(r.get("rows") or 0 for r in res_list)
            print(f"  {src}: {promoted} promoted, {skipped} skipped, {total_rows:,} rows")
        elif isinstance(res_list, dict):
            print(f"  {src}: {res_list.get('status', '?')} ({len(res_list.get('sheets', []))} sheets)")

    return write_summary("promote", {"sources": {k: str(v) for k, v in results.items()}})


def command_build_catalog(args):
    catalog = build_catalog(SILVER_DIR, output_path=DATA_DIR / "catalog.parquet")
    print(f"Catalog built: {len(catalog)} datasets")
    print(f"Sources: {', '.join(catalog['source'].unique())}")
    print(f"Total rows across all datasets: {catalog['rows'].sum():,.0f}")
    print(f"Saved to: {DATA_DIR / 'catalog.parquet'}")
    return write_summary("build_catalog", {"datasets": len(catalog), "path": str(DATA_DIR / "catalog.parquet")})


def command_search(args):
    catalog_path = DATA_DIR / "catalog.parquet"
    if not catalog_path.exists():
        print("No catalog found. Run: who-data build-catalog")
        return

    catalog = pd.read_parquet(catalog_path)
    results = search_catalog(catalog, keyword=args.keyword, source=args.source)

    if results.empty:
        print("No matching datasets found.")
        return

    # Print results as a table
    for _, row in results.iterrows():
        print(f"\n  [{row['source']}] {row['dataset']}")
        print(f"    {row['description']}")
        print(f"    {row['rows']:,} rows, {row['columns']} columns")
        print(f"    Path: {row['silver_path']}")

    print(f"\n  {len(results)} dataset(s) found.")
```

- [ ] **Step 6: Run all tests**

Run: `cd C:\Projects\who-data-lakehouse && python -m pytest tests/ -v`
Expected: All tests pass (15 total across 7 test files)

- [ ] **Step 7: Commit**

```bash
git add src/who_data_lakehouse/catalog.py src/who_data_lakehouse/cli.py tests/test_search_cli.py
git commit -m "feat: add promote, build-catalog, and search CLI commands"
```

---

### Task 9: Run Promotion on Real Data and Build Catalog

This task runs the pipeline on actual data and verifies results.

**Files:** None (execution only)

- [ ] **Step 1: Promote all sources**

Run: `cd C:\Projects\who-data-lakehouse && who-data promote --skip-existing`

Expected output showing promoted counts for: covid (~9 files), ghed (3 sheets), whs (4+ files), glaas (9 sheets), hidr (~61 files), gho_facts (~3,057 indicators).

Note: GHO facts will take the longest (3,057 JSONL.gz files). If time-limited, run just the fast sources first:
```
who-data promote --source covid
who-data promote --source ghed
who-data promote --source whs
who-data promote --source glaas
who-data promote --source hidr
```

- [ ] **Step 2: Build the catalog**

Run: `cd C:\Projects\who-data-lakehouse && who-data build-catalog`

Expected: Shows dataset count, sources found, total row count. Saves `data/catalog.parquet`.

- [ ] **Step 3: Test the search command**

Run several searches:
```
who-data search tuberculosis
who-data search --source covid
who-data search mortality
who-data search kenya
who-data search expenditure
```

Verify each returns relevant results.

- [ ] **Step 4: Verify parquet files are loadable**

Run:
```python
python -c "
import pandas as pd
# COVID
df = pd.read_parquet('C:/Projects/who-data-lakehouse/data/silver/covid/WHO-COVID-19-global-data.parquet')
print(f'COVID: {len(df):,} rows, {len(df.columns)} cols')
print(f'  Columns: {list(df.columns[:8])}')

# GHED
df = pd.read_parquet('C:/Projects/who-data-lakehouse/data/silver/ghed/ghed_data.parquet')
print(f'GHED: {len(df):,} rows, {len(df.columns)} cols')

# Catalog
cat = pd.read_parquet('C:/Projects/who-data-lakehouse/data/catalog.parquet')
print(f'Catalog: {len(cat)} datasets across {cat[\"source\"].nunique()} sources')
print(f'Total rows: {cat[\"rows\"].sum():,.0f}')
"
```

- [ ] **Step 5: Commit everything**

```bash
git add -A
git commit -m "feat: run full promotion pipeline and build master catalog"
```

---

## Self-Review Checklist

1. **Spec coverage:** All 5 suggested improvements are covered:
   - Task 1-6: Promote easy sources to silver (COVID, GHED, WHS, GLAAS, HIDR, GHO facts)
   - Task 7: Master catalog builder
   - Task 8: `who-data search` CLI + `promote` + `build-catalog` commands
   - Task 9: Integration run on real data

2. **Placeholder scan:** No TBD/TODO/placeholder steps. All code blocks are complete.

3. **Type consistency:** `promote_*` functions all return `list[dict]` or `dict` with consistent keys (`status`, `rows`, `file`/`sheet`). `build_catalog` returns `pd.DataFrame`. `search_catalog` returns `pd.DataFrame`. CLI handlers use `write_summary` consistently.

4. **Naming:** All functions use existing patterns (`normalize_columns`, `write_summary`, `RAW_DIR`/`SILVER_DIR` constants). New imports match existing codebase style.
