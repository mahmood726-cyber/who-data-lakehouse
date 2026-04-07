#!/usr/bin/env python
"""Bulk download all XMart entity sets for MNCAH and WIISE services.

Runs as a standalone long-running process. Logs progress to stdout and
writes a final summary JSON. Designed to handle 94M+ rows across 229
entity sets over many hours.

Usage:
    python scripts/bulk_xmart_download.py [--service mncah|wiise|both] [--skip-existing]
    python scripts/bulk_xmart_download.py --service mncah --exclude "*_VIEW" --exclude "*STAGING*"
"""

from __future__ import annotations

import argparse
import fnmatch
import json
import shutil
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
from datetime import datetime, timezone
from pathlib import Path

# Ensure UTF-8 output on Windows
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# Add project to path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from who_data_lakehouse.config import RAW_DIR, SILVER_DIR, MANIFEST_DIR, ensure_project_directories
from who_data_lakehouse.sources.xmart import XmartClient
from who_data_lakehouse.storage import utc_stamp, write_json

# Per-entity timeout: 30 minutes max for any single entity set (even 10M+ row ones)
ENTITY_TIMEOUT_SECONDS = 1800


def download_one_entity(client, entity_set, raw_path, silver_dir, page_size, chunk_rows, skip_existing):
    """Download a single entity set. Runs in a thread so we can timeout."""
    return client.download_entity_set(
        entity_set=entity_set,
        raw_path=raw_path,
        silver_dir=silver_dir,
        page_size=page_size,
        chunk_rows=chunk_rows,
        skip_existing=skip_existing,
    )


def download_service(
    service_name: str,
    scope: str = "all",
    page_size: int = 5000,
    chunk_rows: int = 50000,
    skip_existing: bool = False,
    exclude_patterns: list[str] | None = None,
) -> dict:
    """Download all entity sets for a single XMart service."""
    client = XmartClient(service_name)
    entity_sets = client.resolve_entity_sets(scope=scope)

    # Apply exclude patterns
    if exclude_patterns:
        before = len(entity_sets)
        entity_sets = [
            name for name in entity_sets
            if not any(fnmatch.fnmatch(name, pat) for pat in exclude_patterns)
        ]
        print(f"[{service_name}] Excluded {before - len(entity_sets)} entity sets via patterns: {exclude_patterns}", flush=True)

    total = len(entity_sets)
    started = datetime.now(timezone.utc)

    print(f"\n{'='*70}", flush=True)
    print(f"[{service_name.upper()}] Starting download of {total} entity sets", flush=True)
    print(f"[{service_name.upper()}] Scope: {scope} | Page size: {page_size} | Skip existing: {skip_existing}", flush=True)
    print(f"[{service_name.upper()}] Entity timeout: {ENTITY_TIMEOUT_SECONDS}s", flush=True)
    print(f"[{service_name.upper()}] Started at: {started.isoformat()}", flush=True)
    print(f"{'='*70}\n", flush=True)

    results = []
    errors = []
    total_rows_downloaded = 0

    for idx, entity_set in enumerate(entity_sets, 1):
        raw_path = RAW_DIR / "xmart" / service_name / f"{entity_set}.jsonl.gz"
        silver_dir = SILVER_DIR / "xmart" / service_name / entity_set

        t0 = time.time()
        print(f"[{service_name}] [{idx}/{total}] {entity_set} ...", flush=True)

        try:
            # Use ThreadPoolExecutor for per-entity timeout
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(
                    download_one_entity,
                    client, entity_set, raw_path, silver_dir,
                    page_size, chunk_rows, skip_existing,
                )
                result = future.result(timeout=ENTITY_TIMEOUT_SECONDS)

            results.append(result)
            elapsed = time.time() - t0
            row_count = result.get("row_count") or 0
            total_rows_downloaded += row_count if result["status"] == "downloaded" else 0
            print(
                f"[{service_name}] [{idx}/{total}] {entity_set} -> "
                f"{result['status']} | {row_count:,} rows | "
                f"{result.get('chunk_count', 0)} chunks | {elapsed:.1f}s | "
                f"cumulative: {total_rows_downloaded:,} rows",
                flush=True,
            )
        except FutureTimeout:
            elapsed = time.time() - t0
            print(
                f"[{service_name}] [{idx}/{total}] {entity_set} -> "
                f"TIMEOUT ({elapsed:.1f}s > {ENTITY_TIMEOUT_SECONDS}s limit)",
                flush=True,
            )
            errors.append({
                "entity_set": entity_set,
                "error": f"Timed out after {ENTITY_TIMEOUT_SECONDS}s",
            })
            # Clean up partial downloads
            if raw_path.exists():
                raw_path.unlink()
            if silver_dir.exists():
                shutil.rmtree(silver_dir, ignore_errors=True)
        except Exception as exc:
            elapsed = time.time() - t0
            print(
                f"[{service_name}] [{idx}/{total}] {entity_set} -> "
                f"ERROR ({elapsed:.1f}s): {exc}",
                flush=True,
            )
            errors.append({
                "entity_set": entity_set,
                "error": str(exc),
                "traceback": traceback.format_exc(),
            })

    finished = datetime.now(timezone.utc)
    duration_min = (finished - started).total_seconds() / 60

    downloaded = [r for r in results if r["status"] == "downloaded"]
    skipped = [r for r in results if r["status"] == "skipped"]

    summary = {
        "source": "xmart",
        "service": service_name,
        "scope": scope,
        "started_at": started.isoformat(),
        "finished_at": finished.isoformat(),
        "duration_minutes": round(duration_min, 1),
        "entity_set_count": len(results),
        "downloaded_count": len(downloaded),
        "skipped_count": len(skipped),
        "error_count": len(errors),
        "total_rows_downloaded": total_rows_downloaded,
        "results": sorted(results, key=lambda r: r["entity_set"]),
    }
    if errors:
        summary["errors"] = errors

    # Write manifest
    stamp = utc_stamp()
    manifest_path = MANIFEST_DIR / f"xmart_bulk_{service_name}_{stamp}.json"
    write_json(summary, manifest_path)

    print(f"\n{'='*70}", flush=True)
    print(f"[{service_name.upper()}] COMPLETE", flush=True)
    print(f"  Downloaded: {len(downloaded)} entity sets ({total_rows_downloaded:,} rows)", flush=True)
    print(f"  Skipped:    {len(skipped)}", flush=True)
    print(f"  Errors:     {len(errors)}", flush=True)
    print(f"  Duration:   {duration_min:.1f} minutes", flush=True)
    print(f"  Manifest:   {manifest_path}", flush=True)
    print(f"{'='*70}\n", flush=True)

    return summary


def main():
    parser = argparse.ArgumentParser(description="Bulk XMart download")
    ALL_SERVICES = ["mncah", "wiise", "flumart", "ntd", "ncd", "nutrition"]
    parser.add_argument(
        "--service",
        choices=ALL_SERVICES + ["all"],
        default="all",
        help="Which service(s) to download (default: all)",
    )
    parser.add_argument("--skip-existing", action="store_true")
    parser.add_argument("--page-size", type=int, default=5000)
    parser.add_argument("--scope", choices=["all", "curated"], default="all")
    parser.add_argument("--exclude", action="append", default=[], help="Exclude entity set name patterns")
    args = parser.parse_args()

    ensure_project_directories()

    if args.service == "all":
        services = ["mncah", "wiise", "flumart", "ntd", "ncd", "nutrition"]
    else:
        services = [args.service]
    all_summaries = {}

    for svc in services:
        try:
            all_summaries[svc] = download_service(
                service_name=svc,
                scope=args.scope,
                page_size=args.page_size,
                skip_existing=args.skip_existing,
                exclude_patterns=args.exclude or None,
            )
        except Exception as exc:
            print(f"\n[FATAL] {svc} failed: {exc}", flush=True)
            traceback.print_exc()
            all_summaries[svc] = {"error": str(exc)}

    # Final summary
    print("\n" + "=" * 70, flush=True)
    print("ALL SERVICES COMPLETE", flush=True)
    for svc, s in all_summaries.items():
        if "error" in s and isinstance(s.get("error"), str) and "downloaded_count" not in s:
            print(f"  {svc}: FATAL ERROR - {s['error']}", flush=True)
        else:
            print(
                f"  {svc}: {s.get('downloaded_count', '?')} downloaded, "
                f"{s.get('error_count', '?')} errors, "
                f"{s.get('total_rows_downloaded', '?'):,} rows, "
                f"{s.get('duration_minutes', '?')} min",
                flush=True,
            )
    print("=" * 70, flush=True)


if __name__ == "__main__":
    main()
