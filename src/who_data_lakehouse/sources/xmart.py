from __future__ import annotations

import fnmatch
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from who_data_lakehouse.http import build_session, get_json
from who_data_lakehouse.normalize import normalize_columns
from who_data_lakehouse.storage import append_jsonl, write_dataframe


DEFAULT_DOWNLOAD_PAGE_SIZE = 5000
DEFAULT_CHUNK_ROWS = 50000


@dataclass(frozen=True)
class XmartServiceProfile:
    name: str
    base_url: str
    curated_prefixes: tuple[str, ...]
    curated_exclude_patterns: tuple[str, ...]


SERVICE_PROFILES: dict[str, XmartServiceProfile] = {
    "mncah": XmartServiceProfile(
        name="mncah",
        base_url="https://xmart-api-public.who.int/mncah",
        curated_prefixes=("EXPORT_", "REF_", "QED_", "PS_", "BD_"),
        curated_exclude_patterns=("*_VIEW", "*STAGING*",),
    ),
    "wiise": XmartServiceProfile(
        name="wiise",
        base_url="https://xmart-api-public.who.int/WIISE",
        curated_prefixes=("MT_", "REF_", "V_", "RI_", "SUPP_", "COV_"),
        curated_exclude_patterns=("*TEMP*", "*_RAW"),
    ),
    "flumart": XmartServiceProfile(
        name="flumart",
        base_url="https://xmart-api-public.who.int/FLUMART",
        curated_prefixes=("VIW_",),
        curated_exclude_patterns=(),
    ),
    "ntd": XmartServiceProfile(
        name="ntd",
        base_url="https://xmart-api-public.who.int/NTD",
        curated_prefixes=(),
        curated_exclude_patterns=(),
    ),
    "ncd": XmartServiceProfile(
        name="ncd",
        base_url="https://xmart-api-public.who.int/NCD",
        curated_prefixes=(),
        curated_exclude_patterns=("*_VIEW", "*STAGING*", "*TEMP*"),
    ),
    "nutrition": XmartServiceProfile(
        name="nutrition",
        base_url="https://xmart-api-public.who.int/NUTRITION",
        curated_prefixes=(),
        curated_exclude_patterns=("*_VIEW", "*STAGING*", "*TEMP*"),
    ),
}


class XmartClient:
    def __init__(self, service_name: str) -> None:
        try:
            self.profile = SERVICE_PROFILES[service_name.lower()]
        except KeyError as exc:
            raise ValueError(f"Unsupported XMart service: {service_name}") from exc
        self.session = build_session()

    @property
    def service_name(self) -> str:
        return self.profile.name

    def list_entity_sets(self) -> list[dict]:
        payload = get_json(self.session, f"{self.profile.base_url}/")
        return payload.get("value", [])

    def resolve_entity_sets(
        self,
        entity_sets: list[str] | None = None,
        scope: str = "curated",
        include_patterns: list[str] | None = None,
        exclude_patterns: list[str] | None = None,
    ) -> list[str]:
        available = [item["name"] for item in self.list_entity_sets()]
        if entity_sets:
            missing = sorted(set(entity_sets) - set(available))
            if missing:
                raise ValueError(
                    f"Unknown entity set(s) for {self.service_name}: {', '.join(missing)}"
                )
            selected = list(entity_sets)
        elif scope == "all":
            selected = list(available)
        else:
            selected = [
                name
                for name in available
                if name.startswith(self.profile.curated_prefixes)
                and not self._matches_any(name, self.profile.curated_exclude_patterns)
            ]
        if include_patterns:
            selected.extend(
                name
                for name in available
                if self._matches_any(name, include_patterns)
            )
        if exclude_patterns:
            selected = [
                name for name in selected if not self._matches_any(name, exclude_patterns)
            ]
        return sorted(dict.fromkeys(selected))

    def fetch_entity_count(self, entity_set: str) -> int | None:
        try:
            payload = get_json(
                self.session,
                f"{self.profile.base_url}/{entity_set}",
                params={"$count": "true", "$top": 1},
            )
        except Exception:
            return None
        count = payload.get("@odata.count")
        return int(count) if count is not None else None

    def build_inventory_frame(
        self,
        scope: str = "all",
        concurrency: int = 8,
        include_patterns: list[str] | None = None,
        exclude_patterns: list[str] | None = None,
    ) -> pd.DataFrame:
        entity_sets = self.list_entity_sets()
        entity_map = {item["name"]: item for item in entity_sets}
        selected = self.resolve_entity_sets(
            scope=scope,
            include_patterns=include_patterns,
            exclude_patterns=exclude_patterns,
        )
        rows: list[dict] = []
        if concurrency <= 1:
            for name in selected:
                rows.append(self._inventory_row(entity_map[name]))
        else:
            with ThreadPoolExecutor(max_workers=concurrency) as executor:
                futures = {
                    executor.submit(self._inventory_row, entity_map[name]): name for name in selected
                }
                for future in as_completed(futures):
                    rows.append(future.result())
        frame = pd.DataFrame.from_records(rows)
        if frame.empty:
            return frame
        return frame.sort_values(["row_count", "entity_set"], ascending=[False, True]).reset_index(
            drop=True
        )

    def iter_entity_batches(
        self,
        entity_set: str,
        page_size: int = DEFAULT_DOWNLOAD_PAGE_SIZE,
        row_limit: int | None = None,
    ):
        skip = 0
        remaining = row_limit
        while True:
            batch_size = page_size if remaining is None else min(page_size, remaining)
            payload = get_json(
                self.session,
                f"{self.profile.base_url}/{entity_set}",
                params={"$top": batch_size, "$skip": skip},
            )
            batch = payload.get("value", [])
            if not batch:
                break
            yield batch
            skip += len(batch)
            if remaining is not None:
                remaining -= len(batch)
                if remaining <= 0:
                    break
            if len(batch) < batch_size:
                break

    def download_entity_set(
        self,
        entity_set: str,
        raw_path: Path,
        silver_dir: Path,
        page_size: int = DEFAULT_DOWNLOAD_PAGE_SIZE,
        row_limit: int | None = None,
        chunk_rows: int = DEFAULT_CHUNK_ROWS,
        skip_existing: bool = False,
    ) -> dict:
        silver_parts = sorted(silver_dir.glob("part-*.parquet")) if silver_dir.exists() else []
        if skip_existing and raw_path.exists() and silver_parts:
            return {
                "entity_set": entity_set,
                "status": "skipped",
                "row_count": None,
                "chunk_count": len(silver_parts),
                "raw_path": str(raw_path),
                "silver_dir": str(silver_dir),
            }
        if raw_path.exists():
            raw_path.unlink()
        if silver_dir.exists():
            shutil.rmtree(silver_dir)
        silver_dir.mkdir(parents=True, exist_ok=True)

        first_batch = True
        total_rows = 0
        chunk_index = 0
        buffer_frames: list[pd.DataFrame] = []
        buffered_rows = 0
        page_num = 0

        for batch in self.iter_entity_batches(
            entity_set=entity_set,
            page_size=page_size,
            row_limit=row_limit,
        ):
            append_jsonl(batch, raw_path, append=not first_batch)
            first_batch = False
            frame = normalize_columns(pd.DataFrame.from_records(batch))
            if frame.empty:
                continue
            buffer_frames.append(frame)
            buffered_rows += len(frame)
            total_rows += len(frame)
            page_num += 1
            if page_num % 100 == 0:
                print(
                    f"  [{entity_set}] {total_rows:,} rows fetched ({page_num} pages) ...",
                    flush=True,
                )
            if buffered_rows >= chunk_rows:
                self._flush_chunk(buffer_frames, silver_dir, chunk_index)
                buffer_frames = []
                buffered_rows = 0
                chunk_index += 1

        if buffer_frames:
            self._flush_chunk(buffer_frames, silver_dir, chunk_index)
            chunk_index += 1

        return {
            "entity_set": entity_set,
            "status": "downloaded",
            "row_count": total_rows,
            "chunk_count": chunk_index,
            "raw_path": str(raw_path),
            "silver_dir": str(silver_dir),
        }

    def _inventory_row(self, entity: dict) -> dict:
        name = entity["name"]
        return {
            "service": self.service_name,
            "entity_set": name,
            "kind": entity.get("kind"),
            "url": entity.get("url"),
            "row_count": self.fetch_entity_count(name),
        }

    @staticmethod
    def _matches_any(value: str, patterns: tuple[str, ...] | list[str]) -> bool:
        return any(fnmatch.fnmatch(value, pattern) for pattern in patterns)

    @staticmethod
    def _flush_chunk(frames: list[pd.DataFrame], silver_dir: Path, chunk_index: int) -> None:
        chunk = pd.concat(frames, ignore_index=True)
        write_dataframe(chunk, silver_dir / f"part-{chunk_index:05d}.parquet")
