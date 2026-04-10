# E156-PROTOCOL: WHO Data Lakehouse

**Project**: WHO Data Lakehouse
**Created**: 2026-04-09
**Status**: Active

## E156 Body (CURRENT)

What is the completeness and cross-system linkage quality of WHO Global Health Observatory data when extracted through a standardized pipeline covering six health domains? We built a Python lakehouse pipeline ingesting mortality, morbidity, risk factors, health systems, expenditure, and immunization indicators from the WHO GHO OData API, with country-level cross-walks to IHME location IDs and World Bank codes for 66 nations. The pipeline uses rate-limited API extraction, domain-specific indicator mapping, and automated quality checks including completeness scoring, temporal gap detection, and outlier flagging at 3 SD. Across six extractors covering 22 WHO indicator codes, the pipeline produces standardized DataFrames with consistent schema (country_iso3, year, indicator, value, sex, data_source) suitable for downstream meta-analytic linkage. All 81 tests pass including mock-based API, crosswalk, extractor, and quality modules with zero network dependency for offline validation. The pipeline enables reproducible WHO data extraction with proof-carrying provenance for integration with IHME and World Bank lakehouse siblings. Boundary: coverage limited to GHO OData indicators; GHED bulk XLSX and XMart entity sets handled by the existing raw-to-silver promotion pipeline.

## Dashboard

GitHub Pages: https://mahmood726-cyber.github.io/who-data-lakehouse/
