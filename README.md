# WHO Data Lakehouse

`WHO Data Lakehouse` is a bulk-ingest and cleaning repo for public WHO data that is currently scattered across different interfaces, schemas, and download patterns.

The project is built around three realities:

1. WHO data is valuable but operationally messy.
2. The public interfaces are not one thing. They are a mix of OData feeds, JSON APIs, and direct file downloads.
3. The full corpus is too large and too changeable to treat like a normal hand-curated CSV repo. The repo should hold code, manifests, metadata, and small verified samples. Large snapshots should be produced by the pipeline.

## Source map

- `GHO OData API`
  Source: https://www.who.int/data/gho/info/gho-odata-api
  Use: indicators, dimensions, dimension values, and observation facts by indicator code.
- `data.who.int indicator catalog`
  Source site: https://data.who.int/indicators
  Use: richer indicator metadata plus `DataExportUri` links for indicator-specific downloads.
- `HIDR API`
  Source: https://www.who.int/data/inequality-monitor/data/hidr-api
  Use: reference tables plus whole-dataset downloads from the Health Inequality Data Repository.
- `WHO COVID-19 dashboard`
  Source: https://data.who.int/dashboards/covid19/data
  Use: direct CSV releases linked from the dashboard.
- `WHO Global Health Expenditure Database`
  Source: https://apps.who.int/nha/database/Select/Indicators/en
  Use: single workbook export of GHED.
- `GLAAS data portal`
  Source: https://glaas.who.int/
  Use: official full-country workbook for the latest GLAAS cycle.
- `WHO Mortality Database`
  Source: https://www.who.int/data/data-collection-tools/who-mortality-database
  Use: raw mortality ZIP archives and reference files.
- `World Health Statistics`
  Source: https://www.who.int/data/gho/publications/world-health-statistics
  Use: annual annex spreadsheets with compact official summary tables.
- `data.who.int page catalog`
  Source API: https://data.who.int/api/datadot/pages
  Use: portal-wide metadata and auditing of dashboards/platforms for additional download links.
- `WHO XMart OData services`
  Sources: https://platform.who.int/data/maternal-newborn-child-adolescent-ageing/data-export and https://immunizationdata.who.int/
  Use: large WHO OData services exposed through `xmart-api-public.who.int`. Six services are supported:
  - **mncah**: Maternal, Newborn, Child, Adolescent Health (80 entity sets, ~41M rows)
  - **wiise**: Immunization / WIISE (149 entity sets, ~53M rows)
  - **flumart**: Influenza surveillance (21 entity sets)
  - **ntd**: Neglected Tropical Diseases (1 entity set)
  - **ncd**: Non-Communicable Diseases (29 entity sets)
  - **nutrition**: Nutrition data (51 entity sets)

## Repo layout

- `src/who_data_lakehouse/`
  Python package for source connectors, normalization, and CLI entrypoints.
- `data/raw/`
  Unmodified snapshots from WHO interfaces.
- `data/bronze/`
  Minimally cleaned tabular outputs.
- `data/silver/`
  Normalized tables designed for analysis.
- `data/reference/`
  Reference tables and downloaded source documentation.
- `manifests/`
  Small JSON summaries that can live in git.
- `output/playwright/`
  Browser automation artifacts when API access is not enough.

## Installation

```powershell
cd C:\Projects\who-data-lakehouse
python -m pip install -e .
```

## Commands

Fetch GHO metadata:

```powershell
who-data gho-metadata
```

Fetch WHO indicator catalog metadata from `data.who.int`:

```powershell
who-data datadot-catalog
```

Download a sample of `data.who.int` export files:

```powershell
who-data datadot-download --limit 10
```

Fetch `data.who.int` page metadata and scan dashboards for candidate file links:

```powershell
who-data datadot-pages --scan-download-links
```

Download useful non-indicator files discovered across `data.who.int` dashboards:

```powershell
who-data datadot-portal-download
```

Download WHO COVID-19 dashboard releases:

```powershell
who-data covid-download
```

Download the WHO Global Health Expenditure Database workbook:

```powershell
who-data ghed-download
```

Download the latest GLAAS full-country workbook:

```powershell
who-data glaas-download
```

Download HIDR reference documents:

```powershell
who-data hidr-reference
```

Download one HIDR dataset:

```powershell
who-data hidr-download --dataset-id rep_sdg
```

Sample a batch from the HIDR reference list:

```powershell
who-data hidr-download --all-from-reference --limit 10
```

Download WHO Mortality Database raw ZIP files:

```powershell
who-data mortality-download
```

Download World Health Statistics annex spreadsheets:

```powershell
who-data whs-download
```

Audit a WHO XMart service and count its entity sets:

```powershell
who-data xmart-audit --service wiise
who-data xmart-audit --service flumart
```

Download a curated WHO XMart service pull:

```powershell
who-data xmart-download --service mncah --skip-existing
```

Download all entity sets from any XMart service:

```powershell
who-data xmart-download --service nutrition --scope all --continue-on-error
```

Smoke-test one large XMart entity set without pulling the whole service:

```powershell
who-data xmart-download --service wiise --entity-set MT_AD_COV_LONG --row-limit 5000
```

Bulk download all XMart services (standalone long-running script):

```powershell
python scripts/bulk_xmart_download.py --service all --scope all --skip-existing
python scripts/bulk_xmart_download.py --service mncah --exclude "*_VIEW" --exclude "*STAGING*"
```

Download and clean GHO fact tables for a small sample:

```powershell
who-data gho-facts --indicator-limit 3
```

Download and clean one specific GHO indicator:

```powershell
who-data gho-facts --indicator WHOSIS_000001
```

Run an end-to-end verification sample:

```powershell
who-data sample-sync --indicator-limit 3
```

Run the full configured pull:

```powershell
who-data full-sync --skip-existing --concurrency 6
```

`full-sync` now includes GLAAS plus live audits of the `mncah` and `wiise` XMart services. The full XMart bulk pulls are intentionally separate because they are much larger than the existing WHO sources and are better handled as resumable service-specific runs.

## Clean schema

The GHO cleaner writes two analysis-friendly tables per indicator:

- `observations_wide`
  One row per observation with normalized column names, parsed timestamps, and stable `observation_id`.
- `observation_dimensions`
  A long table that explodes WHO's `Dim1/Dim2/Dim3` pattern into explicit `dimension_role`, `dimension_type`, and `dimension_code` records.

This avoids forcing analysts to decode slot-based columns every time.

## Browser fallback

Use APIs first. When a WHO page exposes downloads only through rendered pages or unstable front-end code, use the Playwright CLI wrapper in `C:\Users\user\.codex\skills\playwright\scripts\playwright_cli.sh`.

Typical probe flow:

```powershell
$env:CODEX_HOME = "$HOME\\.codex"
$env:PWCLI = "$env:CODEX_HOME\\skills\\playwright\\scripts\\playwright_cli.sh"
& $env:PWCLI open https://data.who.int/indicators --headed
& $env:PWCLI snapshot
```

Artifacts belong under `output/playwright/`.

## GitHub guidance

Do not commit the full raw WHO corpus into normal git history.

- Commit code, docs, manifests, and small smoke-test outputs.
- Produce large parquet snapshots or `.xlsx` pulls as generated artifacts.
- If you later want public hosting for full outputs, prefer GitHub Releases, Hugging Face datasets, object storage, or a data registry.
