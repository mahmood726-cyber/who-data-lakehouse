from __future__ import annotations

import argparse
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd

from who_data_lakehouse.config import (
    BRONZE_DIR,
    DEFAULT_CONCURRENCY,
    DEFAULT_PAGE_SIZE,
    MANIFEST_DIR,
    RAW_DIR,
    REFERENCE_DIR,
    SILVER_DIR,
    ensure_project_directories,
)
from who_data_lakehouse.http import download_to_path
from who_data_lakehouse.normalize import normalize_columns, normalize_gho_observations
from who_data_lakehouse.sources.datadot import DatadotClient
from who_data_lakehouse.sources.datadot_pages import DatadotPageClient
from who_data_lakehouse.sources.covid import CovidDashboardClient
from who_data_lakehouse.sources.ghed import GhedClient
from who_data_lakehouse.sources.glaas import GlaasClient
from who_data_lakehouse.sources.gho import GhoClient
from who_data_lakehouse.sources.hidr import HidrClient
from who_data_lakehouse.sources.mortality import MortalityDatabaseClient
from who_data_lakehouse.sources.world_health_statistics import WorldHealthStatisticsClient
from who_data_lakehouse.sources.xmart import (
    DEFAULT_CHUNK_ROWS,
    DEFAULT_DOWNLOAD_PAGE_SIZE,
    XmartClient,
)
from who_data_lakehouse.storage import utc_stamp, write_dataframe, write_json, write_jsonl
from who_data_lakehouse.catalog import build_catalog, search_catalog
from who_data_lakehouse.promote.covid import promote_covid
from who_data_lakehouse.promote.ghed import promote_ghed
from who_data_lakehouse.promote.whs import promote_whs
from who_data_lakehouse.promote.glaas import promote_glaas
from who_data_lakehouse.promote.hidr import promote_hidr
from who_data_lakehouse.promote.gho_facts import promote_gho_facts


def command_gho_metadata(args: argparse.Namespace) -> dict:
    client = GhoClient()
    tables = client.build_metadata_tables(page_size=args.page_size)
    stamp = utc_stamp()

    raw_dir = RAW_DIR / "gho" / stamp
    silver_dir = SILVER_DIR / "gho"

    for name, frame in tables.items():
        records = frame.to_dict(orient="records")
        write_json(records, raw_dir / f"{name}.json")
        clean_frame = normalize_columns(frame)
        write_dataframe(clean_frame, silver_dir / f"{name}.parquet")
        write_dataframe(clean_frame, BRONZE_DIR / "gho" / f"{name}.csv")

    summary = {
        "source": "gho",
        "run_at": stamp,
        "indicator_count": int(len(tables["indicators"])),
        "dimension_count": int(len(tables["dimensions"])),
        "dimension_value_count": int(len(tables["dimension_values"])),
    }
    return write_summary("gho_metadata", summary)


def gho_indicator_paths(indicator_code: str) -> tuple[Path, Path, Path]:
    return (
        RAW_DIR / "gho" / "facts" / f"{indicator_code}.jsonl.gz",
        SILVER_DIR / "gho" / "observations_wide" / f"{indicator_code}.parquet",
        SILVER_DIR / "gho" / "observation_dimensions" / f"{indicator_code}.parquet",
    )


def _process_gho_indicator(
    indicator_code: str,
    page_size: int,
    row_limit: int | None,
    skip_existing: bool,
) -> dict:
    raw_path, wide_path, dims_path = gho_indicator_paths(indicator_code)
    if skip_existing and raw_path.exists() and wide_path.exists() and dims_path.exists():
        return {
            "indicator_code": indicator_code,
            "status": "skipped",
            "observation_count": None,
            "dimension_membership_count": None,
        }
    client = GhoClient()
    records = client.fetch_indicator_records(
        indicator_code=indicator_code,
        page_size=page_size,
        row_limit=row_limit,
    )
    wide, dimensions = normalize_gho_observations(records)
    write_jsonl(records, raw_path)
    write_dataframe(wide, wide_path)
    write_dataframe(dimensions, dims_path)
    return {
        "indicator_code": indicator_code,
        "status": "downloaded",
        "observation_count": int(len(wide)),
        "dimension_membership_count": int(len(dimensions)),
    }


def command_gho_facts(args: argparse.Namespace) -> dict:
    indicator_codes = resolve_gho_indicator_codes(args)
    if not indicator_codes:
        raise SystemExit("No GHO indicators resolved")

    skip_existing = getattr(args, "skip_existing", False)
    continue_on_error = getattr(args, "continue_on_error", False)
    results = []
    errors = []
    if args.concurrency <= 1 or len(indicator_codes) == 1:
        for indicator_code in indicator_codes:
            try:
                results.append(
                    _process_gho_indicator(
                        indicator_code,
                        args.page_size,
                        args.row_limit,
                        skip_existing,
                    )
                )
            except Exception as exc:
                if not continue_on_error:
                    raise
                errors.append({"indicator_code": indicator_code, "error": str(exc)})
    else:
        with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
            futures = {
                executor.submit(
                    _process_gho_indicator,
                    indicator_code,
                    args.page_size,
                    args.row_limit,
                    skip_existing,
                ): indicator_code
                for indicator_code in indicator_codes
            }
            for future in as_completed(futures):
                indicator_code = futures[future]
                try:
                    results.append(future.result())
                except Exception as exc:
                    if not continue_on_error:
                        raise
                    errors.append({"indicator_code": indicator_code, "error": str(exc)})

    downloaded = [item for item in results if item["status"] == "downloaded"]
    skipped = [item for item in results if item["status"] == "skipped"]
    summary = {
        "source": "gho",
        "run_at": utc_stamp(),
        "indicator_count": len(results),
        "downloaded_count": len(downloaded),
        "skipped_count": len(skipped),
        "error_count": len(errors),
        "results": sorted(results, key=lambda item: item["indicator_code"]),
    }
    if errors:
        summary["errors"] = errors
    return write_summary("gho_facts", summary)


def command_datadot_catalog(args: argparse.Namespace) -> dict:
    client = DatadotClient()
    frame = client.build_catalog_table(page_size=args.page_size, row_limit=args.row_limit)
    stamp = utc_stamp()

    write_json(frame.to_dict(orient="records"), RAW_DIR / "datadot" / stamp / "catalog.json")
    clean = normalize_columns(frame)
    write_dataframe(clean, SILVER_DIR / "datadot" / "indicator_catalog.parquet")
    write_dataframe(clean, BRONZE_DIR / "datadot" / "indicator_catalog.csv")

    summary = {
        "source": "datadot",
        "run_at": stamp,
        "provider": client.provider,
        "row_count": int(len(frame)),
        "exportable_rows": int(
            clean["data_export_uri"].replace("", None).notna().sum()
        )
        if "data_export_uri" in clean.columns
        else 0,
    }
    return write_summary("datadot_catalog", summary)


def command_hidr_reference(_: argparse.Namespace) -> dict:
    client = HidrClient()
    downloaded = client.download_reference_docs(REFERENCE_DIR / "hidr")
    summary = {
        "source": "hidr",
        "run_at": utc_stamp(),
        "downloaded_files": {key: str(path) for key, path in downloaded.items()},
    }
    return write_summary("hidr_reference", summary)


def command_hidr_download(args: argparse.Namespace) -> dict:
    client = HidrClient()
    target_dir = RAW_DIR / "hidr" / "datasets"
    dataset_ids: list[str]
    skip_existing = getattr(args, "skip_existing", False)
    continue_on_error = getattr(args, "continue_on_error", False)

    if args.dataset_id:
        dataset_ids = [args.dataset_id]
    else:
        if not args.all_from_reference:
            raise SystemExit("Pass --dataset-id or --all-from-reference")
        docs = client.download_reference_docs(REFERENCE_DIR / "hidr")
        dataset_ids = client.dataset_ids_from_reference(docs["dataset_ids"])
        if args.limit is not None:
            dataset_ids = dataset_ids[: args.limit]

    downloaded: dict[str, str] = {}
    skipped: list[str] = []
    errors: list[dict] = []
    for dataset_id in dataset_ids:
        destination = target_dir / f"{dataset_id}.xlsx"
        if skip_existing and destination.exists():
            downloaded[dataset_id] = str(destination)
            skipped.append(dataset_id)
            continue
        try:
            downloaded[dataset_id] = str(client.download_dataset(dataset_id, target_dir))
        except Exception as exc:
            if not continue_on_error:
                raise
            errors.append({"dataset_id": dataset_id, "error": str(exc)})
    summary = {
        "source": "hidr",
        "run_at": utc_stamp(),
        "dataset_count": len(downloaded),
        "skipped_count": len(skipped),
        "error_count": len(errors),
        "downloaded_files": downloaded,
    }
    if errors:
        summary["errors"] = errors
    return write_summary("hidr_download", summary)


def command_sample_sync(args: argparse.Namespace) -> dict:
    metadata_summary = command_gho_metadata(argparse.Namespace(page_size=args.page_size))
    datadot_summary = command_datadot_catalog(
        argparse.Namespace(page_size=args.page_size, row_limit=args.datadot_row_limit)
    )
    hidr_reference_summary = command_hidr_reference(argparse.Namespace())
    hidr_download_summary = command_hidr_download(
        argparse.Namespace(dataset_id="rep_sdg", limit=None)
    )
    gho_facts_summary = command_gho_facts(
        argparse.Namespace(
            indicator=args.indicator,
            indicator_file=None,
            indicator_limit=args.indicator_limit,
            page_size=args.page_size,
            row_limit=args.row_limit,
            concurrency=min(args.concurrency, args.indicator_limit),
        )
    )
    summary = {
        "run_at": utc_stamp(),
        "gho_metadata": metadata_summary,
        "datadot_catalog": datadot_summary,
        "datadot_pages": command_datadot_pages(
            argparse.Namespace(page_size=100, scan_download_links=False, concurrency=8)
        ),
        "datadot_portal_download": command_datadot_portal_download(
            argparse.Namespace(skip_existing=True)
        ),
        "datadot_download": command_datadot_download(
            argparse.Namespace(
                identifier=None,
                limit=5,
                skip_existing=True,
                continue_on_error=True,
            )
        ),
        "covid_download": command_covid_download(
            argparse.Namespace(skip_existing=True, continue_on_error=True)
        ),
        "ghed_download": command_ghed_download(
            argparse.Namespace(skip_existing=True)
        ),
        "hidr_reference": hidr_reference_summary,
        "hidr_download": hidr_download_summary,
        "mortality_download": command_mortality_download(
            argparse.Namespace(skip_existing=True, continue_on_error=True)
        ),
        "whs_download": command_whs_download(
            argparse.Namespace(skip_existing=True, continue_on_error=True)
        ),
        "gho_facts": gho_facts_summary,
    }
    return write_summary("sample_sync", summary)


def resolve_gho_indicator_codes(args: argparse.Namespace) -> list[str]:
    if getattr(args, "indicator", None):
        return dedupe(args.indicator)

    indicator_file = getattr(args, "indicator_file", None)
    if indicator_file:
        values = Path(indicator_file).read_text(encoding="utf-8").splitlines()
        return dedupe([value.strip() for value in values if value.strip()])

    client = GhoClient()
    indicators = client.list_indicators(page_size=args.page_size)
    codes = sorted(
        {item["IndicatorCode"] for item in indicators if item.get("IndicatorCode")}
    )
    if args.indicator_limit is not None:
        codes = codes[: args.indicator_limit]
    return codes


def dedupe(values: list[str]) -> list[str]:
    return list(dict.fromkeys(values))


def command_datadot_download(args: argparse.Namespace) -> dict:
    client = DatadotClient()
    skip_existing = getattr(args, "skip_existing", False)
    continue_on_error = getattr(args, "continue_on_error", False)
    frame = client.build_catalog_table(row_limit=getattr(args, "limit", None))
    identifiers = getattr(args, "identifier", None)
    if identifiers:
        frame = frame[frame["Identifier"].isin(identifiers)]
    exportable = frame[frame["DataExportUri"].notna()].copy()
    downloads: dict[str, str] = {}
    skipped: list[str] = []
    errors: list[dict] = []
    destination_dir = RAW_DIR / "datadot" / "exports"
    for row in exportable.itertuples(index=False):
        identifier = getattr(row, "Identifier", None) or getattr(row, "Id")
        destination = destination_dir / f"{client._slugify(str(identifier))}.csv"
        if skip_existing and destination.exists():
            downloads[str(identifier)] = str(destination)
            skipped.append(str(identifier))
            continue
        try:
            path = download_to_path(
                client.session,
                str(getattr(row, "DataExportUri")).strip(),
                destination,
            )
            downloads[str(identifier)] = str(path)
        except Exception as exc:
            if not continue_on_error:
                raise
            errors.append({"identifier": str(identifier), "error": str(exc)})
    summary = {
        "source": "datadot",
        "run_at": utc_stamp(),
        "download_count": len(downloads),
        "skipped_count": len(skipped),
        "error_count": len(errors),
        "downloaded_files": downloads,
    }
    if errors:
        summary["errors"] = errors
    return write_summary("datadot_download", summary)


def command_datadot_pages(args: argparse.Namespace) -> dict:
    client = DatadotPageClient()
    pages = client.build_pages_table(page_size=args.page_size)
    clean = normalize_columns(pages)
    write_json(clean.to_dict(orient="records"), RAW_DIR / "datadot" / "pages" / "pages.json")
    write_dataframe(clean, SILVER_DIR / "datadot" / "pages.parquet")
    write_dataframe(clean, BRONZE_DIR / "datadot" / "pages.csv")

    file_like = normalize_columns(client.file_like_pages(pages))
    if not file_like.empty:
        write_dataframe(file_like, SILVER_DIR / "datadot" / "file_like_pages.parquet")
        write_dataframe(file_like, BRONZE_DIR / "datadot" / "file_like_pages.csv")

    summary = {
        "source": "datadot",
        "run_at": utc_stamp(),
        "page_count": int(len(clean)),
        "dashboard_or_platform_page_count": int(
            clean["relative_url_path"].fillna("").str.contains("/dashboards/|/platforms/", regex=True).sum()
        )
        if "relative_url_path" in clean.columns
        else 0,
        "file_like_page_count": int(len(file_like)),
    }

    if args.scan_download_links:
        links = normalize_columns(client.scan_download_links(pages, concurrency=args.concurrency))
        if not links.empty:
            write_dataframe(links, SILVER_DIR / "datadot" / "candidate_download_links.parquet")
            write_dataframe(links, BRONZE_DIR / "datadot" / "candidate_download_links.csv")
        summary["candidate_download_link_count"] = int(len(links))
        summary["pages_with_candidate_links"] = int(links["view_url"].nunique()) if not links.empty else 0

    return write_summary("datadot_pages", summary)


def command_datadot_portal_download(args: argparse.Namespace) -> dict:
    client = DatadotPageClient()
    pages = client.build_pages_table(page_size=100)
    links = client.scan_download_links(pages, concurrency=8)
    useful = client.useful_download_links(links)
    write_dataframe(normalize_columns(useful), SILVER_DIR / "datadot" / "useful_portal_links.parquet")
    write_dataframe(normalize_columns(useful), BRONZE_DIR / "datadot" / "useful_portal_links.csv")
    downloads = client.download_useful_links(
        useful,
        RAW_DIR / "datadot" / "portal_downloads",
        skip_existing=getattr(args, "skip_existing", False),
    )
    summary = {
        "source": "datadot",
        "run_at": utc_stamp(),
        "useful_link_count": int(len(useful)),
        "download_count": int(len(downloads)),
        "downloaded_files": {key: str(value) for key, value in downloads.items()},
    }
    return write_summary("datadot_portal_download", summary)


def command_covid_download(args: argparse.Namespace) -> dict:
    client = CovidDashboardClient()
    skip_existing = getattr(args, "skip_existing", False)
    continue_on_error = getattr(args, "continue_on_error", False)
    destination_dir = RAW_DIR / "covid"
    downloads: dict[str, str] = {}
    skipped: list[str] = []
    errors: list[dict] = []
    for filename, url in client.discover_downloads().items():
        destination = destination_dir / filename
        if skip_existing and destination.exists():
            downloads[filename] = str(destination)
            skipped.append(filename)
            continue
        try:
            path = download_to_path(client.session, url, destination)
            downloads[filename] = str(path)
        except Exception as exc:
            if not continue_on_error:
                raise
            errors.append({"filename": filename, "error": str(exc)})
    summary = {
        "source": "covid",
        "run_at": utc_stamp(),
        "download_count": len(downloads),
        "skipped_count": len(skipped),
        "error_count": len(errors),
        "downloaded_files": downloads,
    }
    if errors:
        summary["errors"] = errors
    return write_summary("covid_download", summary)


def command_ghed_download(args: argparse.Namespace) -> dict:
    client = GhedClient()
    path = client.download_export(RAW_DIR / "ghed", skip_existing=getattr(args, "skip_existing", False))
    summary = {
        "source": "ghed",
        "run_at": utc_stamp(),
        "download_count": 1,
        "downloaded_files": {"GHED_data.XLSX": str(path)},
    }
    return write_summary("ghed_download", summary)


def command_mortality_download(args: argparse.Namespace) -> dict:
    client = MortalityDatabaseClient()
    skip_existing = getattr(args, "skip_existing", False)
    continue_on_error = getattr(args, "continue_on_error", False)
    destination_dir = RAW_DIR / "mortality"
    downloads: dict[str, str] = {}
    skipped: list[str] = []
    errors: list[dict] = []
    for filename, url in client.discover_downloads().items():
        destination = destination_dir / filename
        if skip_existing and destination.exists():
            downloads[filename] = str(destination)
            skipped.append(filename)
            continue
        try:
            path = download_to_path(client.session, url, destination)
            downloads[filename] = str(path)
        except Exception as exc:
            if not continue_on_error:
                raise
            errors.append({"filename": filename, "error": str(exc)})
    summary = {
        "source": "mortality",
        "run_at": utc_stamp(),
        "download_count": len(downloads),
        "skipped_count": len(skipped),
        "error_count": len(errors),
        "downloaded_files": downloads,
    }
    if errors:
        summary["errors"] = errors
    return write_summary("mortality_download", summary)


def command_whs_download(args: argparse.Namespace) -> dict:
    client = WorldHealthStatisticsClient()
    skip_existing = getattr(args, "skip_existing", False)
    continue_on_error = getattr(args, "continue_on_error", False)
    downloads: dict[str, str] = {}
    skipped: list[str] = []
    errors: list[dict] = []
    for filename, url in client.discover_downloads().items():
        destination = RAW_DIR / "world_health_statistics" / filename
        if skip_existing and destination.exists():
            downloads[filename] = str(destination)
            skipped.append(filename)
            continue
        try:
            path = download_to_path(client.session, url, destination)
            downloads[filename] = str(path)
        except Exception as exc:
            if not continue_on_error:
                raise
            errors.append({"filename": filename, "error": str(exc)})
    summary = {
        "source": "world_health_statistics",
        "run_at": utc_stamp(),
        "download_count": len(downloads),
        "skipped_count": len(skipped),
        "error_count": len(errors),
        "downloaded_files": downloads,
    }
    if errors:
        summary["errors"] = errors
    return write_summary("whs_download", summary)


def command_glaas_download(args: argparse.Namespace) -> dict:
    client = GlaasClient()
    path = client.download_full_dataset(
        RAW_DIR / "glaas",
        skip_existing=getattr(args, "skip_existing", False),
    )
    summary = {
        "source": "glaas",
        "run_at": utc_stamp(),
        "downloaded_file": str(path),
        "dataset_url": client.discover_full_dataset_url(),
    }
    return write_summary("glaas_download", summary)


def command_xmart_audit(args: argparse.Namespace) -> dict:
    client = XmartClient(args.service)
    frame = client.build_inventory_frame(
        scope=args.scope,
        concurrency=args.concurrency,
        include_patterns=getattr(args, "include_pattern", None),
        exclude_patterns=getattr(args, "exclude_pattern", None),
    )
    stamp = utc_stamp()
    raw_dir = RAW_DIR / "xmart" / args.service / stamp
    write_json(frame.to_dict(orient="records"), raw_dir / "entity_inventory.json")
    clean = normalize_columns(frame)
    write_dataframe(clean, SILVER_DIR / "xmart" / args.service / "entity_inventory.parquet")
    write_dataframe(clean, BRONZE_DIR / "xmart" / args.service / "entity_inventory.csv")
    top_rows = []
    if not clean.empty:
        top = (
            clean.sort_values(["row_count", "entity_set"], ascending=[False, True])
            .head(20)
            .replace({pd.NA: None})
        )
        top_rows = top.to_dict(orient="records")
    summary = {
        "source": "xmart",
        "service": args.service,
        "scope": args.scope,
        "run_at": stamp,
        "entity_set_count": int(len(clean)),
        "known_row_total": int(clean["row_count"].dropna().sum()) if "row_count" in clean.columns else 0,
        "unknown_count_rows": int(clean["row_count"].isna().sum()) if "row_count" in clean.columns else 0,
        "top_entity_sets": top_rows,
    }
    return write_summary(f"xmart_audit_{args.service}", summary)


def command_xmart_download(args: argparse.Namespace) -> dict:
    client = XmartClient(args.service)
    entity_sets = client.resolve_entity_sets(
        entity_sets=getattr(args, "entity_set", None),
        scope=args.scope,
        include_patterns=getattr(args, "include_pattern", None),
        exclude_patterns=getattr(args, "exclude_pattern", None),
    )
    if not entity_sets:
        raise SystemExit(f"No XMart entity sets resolved for service {args.service}")

    total = len(entity_sets)
    print(f"[xmart/{args.service}] {total} entity sets to process (scope={args.scope})", flush=True)
    results = []
    errors = []
    for idx, entity_set in enumerate(entity_sets, 1):
        raw_path = RAW_DIR / "xmart" / args.service / f"{entity_set}.jsonl.gz"
        silver_dir = SILVER_DIR / "xmart" / args.service / entity_set
        print(f"[xmart/{args.service}] [{idx}/{total}] {entity_set} ...", flush=True)
        try:
            result = client.download_entity_set(
                entity_set=entity_set,
                raw_path=raw_path,
                silver_dir=silver_dir,
                page_size=args.page_size,
                row_limit=args.row_limit,
                chunk_rows=args.chunk_rows,
                skip_existing=args.skip_existing,
            )
            results.append(result)
            print(
                f"[xmart/{args.service}] [{idx}/{total}] {entity_set} -> "
                f"{result['status']} ({result.get('row_count', '?')} rows, "
                f"{result.get('chunk_count', 0)} chunks)",
                flush=True,
            )
        except Exception as exc:
            print(f"[xmart/{args.service}] [{idx}/{total}] {entity_set} -> ERROR: {exc}", flush=True)
            if not args.continue_on_error:
                raise
            errors.append({"entity_set": entity_set, "error": str(exc)})
    downloaded = [item for item in results if item["status"] == "downloaded"]
    skipped = [item for item in results if item["status"] == "skipped"]
    summary = {
        "source": "xmart",
        "service": args.service,
        "scope": args.scope,
        "run_at": utc_stamp(),
        "entity_set_count": len(results),
        "downloaded_count": len(downloaded),
        "skipped_count": len(skipped),
        "error_count": len(errors),
        "results": sorted(results, key=lambda item: item["entity_set"]),
    }
    if errors:
        summary["errors"] = errors
    return write_summary(f"xmart_download_{args.service}", summary)


def command_full_sync(args: argparse.Namespace) -> dict:
    summary = {
        "run_at": utc_stamp(),
        "gho_metadata": command_gho_metadata(
            argparse.Namespace(page_size=args.page_size)
        ),
        "datadot_catalog": command_datadot_catalog(
            argparse.Namespace(page_size=args.page_size, row_limit=None)
        ),
        "datadot_pages": command_datadot_pages(
            argparse.Namespace(page_size=100, scan_download_links=True, concurrency=8)
        ),
        "datadot_portal_download": command_datadot_portal_download(
            argparse.Namespace(skip_existing=args.skip_existing)
        ),
        "datadot_download": command_datadot_download(
            argparse.Namespace(
                identifier=None,
                limit=None,
                skip_existing=args.skip_existing,
                continue_on_error=True,
            )
        ),
        "covid_download": command_covid_download(
            argparse.Namespace(skip_existing=args.skip_existing, continue_on_error=True)
        ),
        "ghed_download": command_ghed_download(
            argparse.Namespace(skip_existing=args.skip_existing)
        ),
        "glaas_download": command_glaas_download(
            argparse.Namespace(skip_existing=args.skip_existing)
        ),
        "hidr_reference": command_hidr_reference(argparse.Namespace()),
        "hidr_download": command_hidr_download(
            argparse.Namespace(
                dataset_id=None,
                all_from_reference=True,
                limit=None,
                skip_existing=args.skip_existing,
                continue_on_error=True,
            )
        ),
        "mortality_download": command_mortality_download(
            argparse.Namespace(skip_existing=args.skip_existing, continue_on_error=True)
        ),
        "whs_download": command_whs_download(
            argparse.Namespace(skip_existing=args.skip_existing, continue_on_error=True)
        ),
        "xmart_mncah_audit": command_xmart_audit(
            argparse.Namespace(
                service="mncah",
                scope="all",
                concurrency=min(args.concurrency * 2, 12),
                include_pattern=None,
                exclude_pattern=None,
            )
        ),
        "xmart_wiise_audit": command_xmart_audit(
            argparse.Namespace(
                service="wiise",
                scope="all",
                concurrency=min(args.concurrency * 2, 12),
                include_pattern=None,
                exclude_pattern=None,
            )
        ),
        "gho_facts": command_gho_facts(
            argparse.Namespace(
                indicator=None,
                indicator_file=None,
                indicator_limit=None,
                page_size=args.gho_page_size,
                row_limit=None,
                concurrency=args.concurrency,
                skip_existing=args.skip_existing,
                continue_on_error=True,
            )
        ),
    }
    return write_summary("full_sync", summary)


def command_promote(args: argparse.Namespace) -> dict:
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

    for src, res_list in results.items():
        if isinstance(res_list, list):
            promoted = sum(1 for r in res_list if r.get("status") == "promoted")
            skipped = sum(1 for r in res_list if r.get("status") == "skipped")
            total_rows = sum(r.get("rows") or 0 for r in res_list)
            print(f"  {src}: {promoted} promoted, {skipped} skipped, {total_rows:,} rows")
        elif isinstance(res_list, dict):
            print(f"  {src}: {res_list.get('status', '?')} ({len(res_list.get('sheets', []))} sheets)")

    return write_summary("promote", {"sources": list(results.keys())})


def command_build_catalog(args: argparse.Namespace) -> dict:
    from who_data_lakehouse.config import DATA_DIR
    catalog = build_catalog(SILVER_DIR, output_path=DATA_DIR / "catalog.parquet")
    print(f"Catalog built: {len(catalog)} datasets")
    print(f"Sources: {', '.join(sorted(catalog['source'].unique()))}")
    print(f"Total rows across all datasets: {catalog['rows'].sum():,.0f}")
    print(f"Saved to: {DATA_DIR / 'catalog.parquet'}")
    return write_summary("build_catalog", {"datasets": int(len(catalog)), "path": str(DATA_DIR / "catalog.parquet")})


def command_search(args: argparse.Namespace) -> dict:
    from who_data_lakehouse.config import DATA_DIR
    catalog_path = DATA_DIR / "catalog.parquet"
    if not catalog_path.exists():
        print("No catalog found. Run: who-data build-catalog")
        return {"error": "no catalog"}

    catalog = pd.read_parquet(catalog_path)
    results = search_catalog(catalog, keyword=args.keyword, source=args.source)

    if results.empty:
        print("No matching datasets found.")
        return {"matches": 0}

    for _, row in results.iterrows():
        print(f"\n  [{row['source']}] {row['dataset']}")
        print(f"    {row['description']}")
        print(f"    {row['rows']:,.0f} rows, {int(row['columns'])} columns")
        print(f"    Path: {row['silver_path']}")

    print(f"\n  {len(results)} dataset(s) found.")
    return {"matches": int(len(results))}


def write_summary(prefix: str, summary: dict) -> dict:
    stamp = utc_stamp()
    path = MANIFEST_DIR / f"{prefix}_{stamp}.json"
    write_json(summary, path)
    summary["manifest_path"] = str(path)
    return summary


def print_summary(summary: dict) -> None:
    print(json.dumps(summary, indent=2))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Bulk download and clean public WHO data")
    subparsers = parser.add_subparsers(dest="command", required=True)

    gho_metadata = subparsers.add_parser(
        "gho-metadata",
        help="Fetch GHO indicator and dimension metadata",
    )
    gho_metadata.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE)
    gho_metadata.set_defaults(handler=command_gho_metadata)

    gho_facts = subparsers.add_parser(
        "gho-facts",
        help="Fetch GHO observation facts for one or more indicators",
    )
    gho_facts.add_argument("--indicator", action="append", help="GHO indicator code; repeatable")
    gho_facts.add_argument(
        "--indicator-file",
        help="Path to a newline-delimited list of GHO indicator codes",
    )
    gho_facts.add_argument(
        "--indicator-limit",
        type=int,
        help="Number of indicators to sample from GHO metadata",
    )
    gho_facts.add_argument("--page-size", type=int, default=1000)
    gho_facts.add_argument(
        "--row-limit",
        type=int,
        help="Optional row limit per indicator for smoke tests",
    )
    gho_facts.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY)
    gho_facts.add_argument("--skip-existing", action="store_true")
    gho_facts.add_argument("--continue-on-error", action="store_true")
    gho_facts.set_defaults(handler=command_gho_facts)

    datadot_catalog = subparsers.add_parser(
        "datadot-catalog",
        help="Fetch indicator catalog metadata from data.who.int",
    )
    datadot_catalog.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE)
    datadot_catalog.add_argument("--row-limit", type=int)
    datadot_catalog.set_defaults(handler=command_datadot_catalog)

    datadot_download = subparsers.add_parser(
        "datadot-download",
        help="Download indicator export files exposed by data.who.int",
    )
    datadot_download.add_argument(
        "--identifier",
        action="append",
        help="Specific data.who.int indicator identifier; repeatable",
    )
    datadot_download.add_argument(
        "--limit",
        type=int,
        help="Limit export downloads using the catalog order",
    )
    datadot_download.add_argument("--skip-existing", action="store_true")
    datadot_download.add_argument("--continue-on-error", action="store_true")
    datadot_download.set_defaults(handler=command_datadot_download)

    datadot_pages = subparsers.add_parser(
        "datadot-pages",
        help="Fetch data.who.int page metadata and optionally scan for candidate download links",
    )
    datadot_pages.add_argument("--page-size", type=int, default=100)
    datadot_pages.add_argument("--scan-download-links", action="store_true")
    datadot_pages.add_argument("--concurrency", type=int, default=8)
    datadot_pages.set_defaults(handler=command_datadot_pages)

    datadot_portal_download = subparsers.add_parser(
        "datadot-portal-download",
        help="Download useful non-indicator files discovered across data.who.int dashboards/platforms",
    )
    datadot_portal_download.add_argument("--skip-existing", action="store_true")
    datadot_portal_download.set_defaults(handler=command_datadot_portal_download)

    covid_download = subparsers.add_parser(
        "covid-download",
        help="Download WHO COVID-19 dashboard CSV releases",
    )
    covid_download.add_argument("--skip-existing", action="store_true")
    covid_download.add_argument("--continue-on-error", action="store_true")
    covid_download.set_defaults(handler=command_covid_download)

    ghed_download = subparsers.add_parser(
        "ghed-download",
        help="Download the WHO Global Health Expenditure Database export workbook",
    )
    ghed_download.add_argument("--skip-existing", action="store_true")
    ghed_download.set_defaults(handler=command_ghed_download)

    glaas_download = subparsers.add_parser(
        "glaas-download",
        help="Download the official GLAAS full country dataset workbook",
    )
    glaas_download.add_argument("--skip-existing", action="store_true")
    glaas_download.set_defaults(handler=command_glaas_download)

    hidr_reference = subparsers.add_parser(
        "hidr-reference",
        help="Download HIDR reference documents",
    )
    hidr_reference.set_defaults(handler=command_hidr_reference)

    hidr_download = subparsers.add_parser(
        "hidr-download",
        help="Download one or more HIDR datasets",
    )
    hidr_download.add_argument(
        "--dataset-id",
        help="One HIDR dataset id, for example rep_sdg",
    )
    hidr_download.add_argument(
        "--all-from-reference",
        action="store_true",
        help="Download all dataset ids listed in the HIDR reference file",
    )
    hidr_download.add_argument(
        "--limit",
        type=int,
        help="Limit dataset count when deriving ids from the reference file",
    )
    hidr_download.add_argument("--skip-existing", action="store_true")
    hidr_download.add_argument("--continue-on-error", action="store_true")
    hidr_download.set_defaults(handler=command_hidr_download)

    mortality_download = subparsers.add_parser(
        "mortality-download",
        help="Download WHO Mortality Database raw ZIP files",
    )
    mortality_download.add_argument("--skip-existing", action="store_true")
    mortality_download.add_argument("--continue-on-error", action="store_true")
    mortality_download.set_defaults(handler=command_mortality_download)

    whs_download = subparsers.add_parser(
        "whs-download",
        help="Download World Health Statistics annex spreadsheets",
    )
    whs_download.add_argument("--skip-existing", action="store_true")
    whs_download.add_argument("--continue-on-error", action="store_true")
    whs_download.set_defaults(handler=command_whs_download)

    xmart_audit = subparsers.add_parser(
        "xmart-audit",
        help="Audit WHO XMart OData services and count entity-set rows",
    )
    xmart_audit.add_argument("--service", choices=["mncah", "wiise", "flumart", "ntd", "ncd", "nutrition"], required=True)
    xmart_audit.add_argument("--scope", choices=["all", "curated"], default="all")
    xmart_audit.add_argument("--include-pattern", action="append")
    xmart_audit.add_argument("--exclude-pattern", action="append")
    xmart_audit.add_argument("--concurrency", type=int, default=8)
    xmart_audit.set_defaults(handler=command_xmart_audit)

    xmart_download = subparsers.add_parser(
        "xmart-download",
        help="Download one WHO XMart service into raw JSONL and chunked parquet tables",
    )
    xmart_download.add_argument("--service", choices=["mncah", "wiise", "flumart", "ntd", "ncd", "nutrition"], required=True)
    xmart_download.add_argument("--entity-set", action="append")
    xmart_download.add_argument("--scope", choices=["all", "curated"], default="curated")
    xmart_download.add_argument("--include-pattern", action="append")
    xmart_download.add_argument("--exclude-pattern", action="append")
    xmart_download.add_argument("--page-size", type=int, default=DEFAULT_DOWNLOAD_PAGE_SIZE)
    xmart_download.add_argument("--row-limit", type=int)
    xmart_download.add_argument("--chunk-rows", type=int, default=DEFAULT_CHUNK_ROWS)
    xmart_download.add_argument("--skip-existing", action="store_true")
    xmart_download.add_argument("--continue-on-error", action="store_true")
    xmart_download.set_defaults(handler=command_xmart_download)

    full_sync = subparsers.add_parser(
        "full-sync",
        help="Run the full configured WHO download and cleaning pipeline",
    )
    full_sync.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE)
    full_sync.add_argument("--gho-page-size", type=int, default=1000)
    full_sync.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY)
    full_sync.add_argument("--skip-existing", action="store_true")
    full_sync.set_defaults(handler=command_full_sync)

    sample_sync = subparsers.add_parser(
        "sample-sync",
        help="Run a live smoke test across all configured WHO sources",
    )
    sample_sync.add_argument("--indicator", action="append", help="Explicit GHO indicator code")
    sample_sync.add_argument("--indicator-limit", type=int, default=3)
    sample_sync.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE)
    sample_sync.add_argument("--row-limit", type=int, help="Optional row limit per GHO indicator")
    sample_sync.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY)
    sample_sync.add_argument("--datadot-row-limit", type=int, default=200)
    sample_sync.set_defaults(handler=command_sample_sync)

    # --- promote ---
    sp_promote = subparsers.add_parser("promote", help="Promote raw data to silver parquet")
    sp_promote.add_argument(
        "--source",
        choices=["all", "covid", "ghed", "whs", "glaas", "hidr", "gho-facts"],
        default="all",
    )
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

    return parser


def main() -> None:
    ensure_project_directories()
    parser = build_parser()
    args = parser.parse_args()
    summary = args.handler(args)
    print_summary(summary)
