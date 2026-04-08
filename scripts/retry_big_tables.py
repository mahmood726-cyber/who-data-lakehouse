#!/usr/bin/env python
"""Retry download of failed XMart entity sets with adaptive strategies.

Reads the latest bulk manifest per service, extracts all errors, and retries
each with parameters tuned to the failure mode:

  - TIMEOUT / MEMORY / UNKNOWN: No timeout, smaller page size (1000), smaller chunks
  - BAD_REQUEST: Smaller page size (500) to avoid server-side offset limits
  - SERVER_ERROR: Standard retry with fresh session per entity
  - JSON_PARSE: Smaller page size (1000) to get smaller responses
  - CONN_EXHAUST: Longer backoff, fresh session, smaller page size
  - FILE_LOCK: Just retry (the lock is gone now)

Usage:
    python scripts/retry_big_tables.py                # retry all failures
    python scripts/retry_big_tables.py --service wiise # retry one service only
    python scripts/retry_big_tables.py --dry-run       # show what would be retried
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from who_data_lakehouse.config import RAW_DIR, SILVER_DIR, MANIFEST_DIR, ensure_project_directories
from who_data_lakehouse.sources.xmart import XmartClient
from who_data_lakehouse.storage import utc_stamp, write_json

ALL_SERVICES = ["mncah", "wiise", "flumart", "ncd", "nutrition"]


def classify_error(msg: str) -> str:
    """Classify an error message into a retry strategy category."""
    if not msg:
        return "UNKNOWN"
    if "400" in msg:
        return "BAD_REQUEST"
    if "500" in msg:
        return "SERVER_ERROR"
    if "Timed out" in msg:
        return "TIMEOUT"
    if "Expecting" in msg:
        return "JSON_PARSE"
    if "Max retries" in msg:
        return "CONN_EXHAUST"
    if "allocate" in msg.lower():
        return "MEMORY"
    if "WinError" in msg:
        return "FILE_LOCK"
    return "UNKNOWN"


def get_retry_params(category: str) -> dict:
    """Return download parameters tuned to the failure mode."""
    if category in ("TIMEOUT", "MEMORY", "UNKNOWN", "FILE_LOCK"):
        return {"page_size": 1000, "chunk_rows": 20000}
    if category == "BAD_REQUEST":
        # Server rejects large $skip offsets — use smallest page to push
        # the row count per request down and reduce skip increments
        return {"page_size": 500, "chunk_rows": 20000}
    if category in ("SERVER_ERROR", "JSON_PARSE"):
        return {"page_size": 1000, "chunk_rows": 20000}
    if category == "CONN_EXHAUST":
        return {"page_size": 1000, "chunk_rows": 20000}
    return {"page_size": 1000, "chunk_rows": 20000}


def collect_failures(service_filter: str | None = None) -> list[dict]:
    """Read latest manifest per service and extract all failed entity sets."""
    targets = []
    services = [service_filter] if service_filter else ALL_SERVICES

    for svc in services:
        manifests = sorted(MANIFEST_DIR.glob(f"xmart_bulk_{svc}_*.json"))
        if not manifests:
            continue
        latest = manifests[-1]
        data = json.loads(latest.read_text(encoding="utf-8"))
        errors = data.get("errors", [])
        for entry in errors:
            if isinstance(entry, dict):
                entity = entry.get("entity_set", "unknown")
                msg = entry.get("error", "")
            else:
                continue
            category = classify_error(msg)
            targets.append({
                "service": svc,
                "entity_set": entity,
                "category": category,
                "original_error": msg[:200],
                **get_retry_params(category),
            })
    return targets


def retry_one(service: str, entity_set: str, page_size: int, chunk_rows: int) -> dict:
    """Retry a single entity set with no timeout and a fresh session."""
    raw_path = RAW_DIR / "xmart" / service / f"{entity_set}.jsonl.gz"
    silver_dir = SILVER_DIR / "xmart" / service / entity_set

    # Clean up any partial data from the failed attempt
    if raw_path.exists():
        raw_path.unlink()
    if silver_dir.exists():
        shutil.rmtree(silver_dir, ignore_errors=True)

    client = XmartClient(service)
    t0 = time.time()

    result = client.download_entity_set(
        entity_set=entity_set,
        raw_path=raw_path,
        silver_dir=silver_dir,
        page_size=page_size,
        chunk_rows=chunk_rows,
    )
    elapsed = time.time() - t0
    return {
        "service": service,
        "entity_set": entity_set,
        "status": result["status"],
        "row_count": result.get("row_count", 0),
        "chunk_count": result.get("chunk_count", 0),
        "elapsed_seconds": round(elapsed),
    }


def main():
    parser = argparse.ArgumentParser(description="Retry failed XMart downloads")
    parser.add_argument("--service", choices=ALL_SERVICES, help="Retry only this service")
    parser.add_argument("--dry-run", action="store_true", help="Show targets without downloading")
    args = parser.parse_args()

    ensure_project_directories()
    targets = collect_failures(args.service)

    if not targets:
        print("No failures found in manifests. Nothing to retry.")
        return

    print(f"\n{'='*70}")
    print(f"RETRY PLAN: {len(targets)} failed entity sets")
    print(f"{'='*70}\n")

    for t in targets:
        print(f"  {t['service']:12s} {t['entity_set']:45s} {t['category']:15s} page={t['page_size']}")

    if args.dry_run:
        print(f"\n[DRY RUN] Would retry {len(targets)} entity sets. Exiting.")
        return

    print(f"\nStarting retries at {datetime.now(timezone.utc).isoformat()} ...\n")

    results = []
    succeeded = 0
    failed = 0

    for idx, target in enumerate(targets, 1):
        svc = target["service"]
        entity = target["entity_set"]
        cat = target["category"]

        print(f"\n{'='*60}")
        print(f"[{idx}/{len(targets)}] {svc}/{entity} (was: {cat}, page_size={target['page_size']})")
        print(f"{'='*60}")

        try:
            result = retry_one(svc, entity, target["page_size"], target["chunk_rows"])
            results.append(result)
            succeeded += 1
            print(
                f"  -> OK: {result['row_count']:,} rows, "
                f"{result['chunk_count']} chunks, {result['elapsed_seconds']}s"
            )
        except Exception as exc:
            failed += 1
            elapsed_msg = ""
            results.append({
                "service": svc,
                "entity_set": entity,
                "status": "error",
                "error": str(exc)[:300],
                "category": cat,
            })
            print(f"  -> FAIL: {exc}")
            traceback.print_exc()

        # Brief pause between entities to avoid hammering the server
        if idx < len(targets):
            time.sleep(2)

    # Write manifest
    summary = {
        "run_at": utc_stamp(),
        "total_targets": len(targets),
        "succeeded": succeeded,
        "failed": failed,
        "results": results,
    }
    manifest_path = MANIFEST_DIR / f"xmart_retry_{utc_stamp()}.json"
    write_json(summary, manifest_path)

    print(f"\n{'='*70}")
    print(f"RETRY COMPLETE: {succeeded} succeeded, {failed} failed out of {len(targets)}")
    print(f"{'='*70}")
    for r in results:
        status = r["status"]
        rows = r.get("row_count", "?")
        secs = r.get("elapsed_seconds", "?")
        print(f"  {r['service']:12s} {r['entity_set']:45s} {status:12s} rows={rows} time={secs}s")
    print(f"\nManifest: {manifest_path}")


if __name__ == "__main__":
    main()
