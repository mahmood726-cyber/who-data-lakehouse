#!/usr/bin/env python
"""Retry download of specific large XMart entity sets that failed in bulk.

These tables are too large for the 30-minute entity timeout. This script
downloads them one by one with no timeout, using the streaming OData
paginator.

Usage:
    python scripts/retry_big_tables.py
"""

from __future__ import annotations

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

# Tables that failed during bulk download — no timeout applied
RETRY_TARGETS = [
    # (service, entity_set, estimated_rows)
    ("mncah", "EXPORT_FACT_DATA_MCA_TABLE", 9_788_154),
    ("mncah", "FACT_DATA_MCA", 9_788_154),
    ("mncah", "MCA_FACT_DATA", 8_374_638),
    ("mncah", "MCA_FACT_DATA_AGGREGATED", 1_413_516),
    ("nutrition", "GTT_ANA_CALCULATOR", 1_500_000),
]


def main():
    ensure_project_directories()
    results = []

    for service, entity_set, est_rows in RETRY_TARGETS:
        raw_path = RAW_DIR / "xmart" / service / f"{entity_set}.jsonl.gz"
        silver_dir = SILVER_DIR / "xmart" / service / entity_set

        # Skip if already successfully downloaded (has both raw + silver parts)
        silver_parts = sorted(silver_dir.glob("part-*.parquet")) if silver_dir.exists() else []
        if raw_path.exists() and silver_parts:
            print(f"[SKIP] {service}/{entity_set} — already complete ({len(silver_parts)} parts)", flush=True)
            results.append({"service": service, "entity_set": entity_set, "status": "skipped"})
            continue

        print(f"\n{'='*60}", flush=True)
        print(f"[RETRY] {service}/{entity_set} (~{est_rows:,} rows, no timeout)", flush=True)
        print(f"{'='*60}", flush=True)

        client = XmartClient(service)
        t0 = time.time()

        try:
            result = client.download_entity_set(
                entity_set=entity_set,
                raw_path=raw_path,
                silver_dir=silver_dir,
                page_size=5000,
                chunk_rows=50000,
            )
            elapsed = time.time() - t0
            row_count = result.get("row_count", 0)
            print(
                f"[DONE] {service}/{entity_set} -> {row_count:,} rows, "
                f"{result.get('chunk_count', 0)} chunks, {elapsed:.0f}s",
                flush=True,
            )
            results.append({
                "service": service,
                "entity_set": entity_set,
                "status": "downloaded",
                "row_count": row_count,
                "elapsed_seconds": round(elapsed),
            })
        except Exception as exc:
            elapsed = time.time() - t0
            print(f"[FAIL] {service}/{entity_set} -> {exc} ({elapsed:.0f}s)", flush=True)
            traceback.print_exc()
            results.append({
                "service": service,
                "entity_set": entity_set,
                "status": "error",
                "error": str(exc),
                "elapsed_seconds": round(elapsed),
            })

    # Write manifest
    summary = {
        "run_at": utc_stamp(),
        "targets": len(RETRY_TARGETS),
        "results": results,
    }
    manifest_path = MANIFEST_DIR / f"xmart_retry_big_{utc_stamp()}.json"
    write_json(summary, manifest_path)

    print(f"\n{'='*60}", flush=True)
    print("RETRY COMPLETE", flush=True)
    for r in results:
        print(f"  {r['service']}/{r['entity_set']}: {r['status']}", flush=True)
    print(f"Manifest: {manifest_path}", flush=True)
    print(f"{'='*60}", flush=True)


if __name__ == "__main__":
    main()
