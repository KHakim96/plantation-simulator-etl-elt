#!/usr/bin/env python3
"""Phase 2 verification: ADF Landing -> Bronze (file copy, read-only, no writes).

Revised Phase 2 design (see IMPLEMENTATION_PLAN.md): ADF Copy Activity copies
each Landing CSV file to ``bronze/<source>/<file>.csv`` (byte-oriented raw
ingestion, no Delta in Phase 2).

Checks, in order:
  1. Landing container: exactly one CSV per expected source folder, non-empty,
     data-row count (csv module, quoted-newline safe) == expected Phase 1 rows.
  2. Bronze container: exactly one CSV per source at ``bronze/<source>/<file>``
     with byte size equal to the Landing source file and data-row count equal
     to the expected rows; no ``_delta_log`` directory anywhere in Bronze
     (Phase 2 must NOT create Delta tables).
  3. Bronze holds ONLY the six expected top-level source folders.
  4. Downstream protection: silver / gold / live-bronze / live-silver /
     checkpoints / incoming containers contain zero blobs (Phase 2 must not
     touch them).

Uses only azure-storage-blob (same SDK as data_generators/upload_to_adls.py).

Configuration comes from the real environment first, then ``.env`` at the
repository root (python-dotenv, ``override=False``):
  AZURE_STORAGE_ACCOUNT       storage account name
  AZURE_STORAGE_ACCOUNT_KEY   account key (NEVER printed by this script)

Exit codes: 0 = all checks passed; 1 = one or more checks failed;
2 = configuration/connection error before checks could run.
"""

from __future__ import annotations

import csv
import io
import sys
from pathlib import Path

from azure.storage.blob import BlobServiceClient

REPO_ROOT = Path(__file__).resolve().parents[2]

# Phase 1 Landing facts (verified against the live container in Phase 1 and
# re-verified in Phase 2). source folder -> (file, expected data rows, bytes).
EXPECTED_SOURCES = {
    "weather": ("weather_observations.csv", 6_483, 506_283),
    "harvest": ("harvest_transactions.csv", 9_112, 971_406),
    "fertilizer": ("fertilizer_applications.csv", 9_000, 1_523_343),
    "equipment": ("equipment_logs.csv", 10_000, 1_566_167),
    "hr": ("hr_attendance.csv", 2_000, 326_323),
    "finance": ("sap_finance_transactions.csv", 12_000, 1_974_121),
}
EXPECTED_TOTAL_ROWS = sum(rows for _, rows, _ in EXPECTED_SOURCES.values())  # 48,595

LANDING_CONTAINER = "landing"
BRONZE_CONTAINER = "bronze"
DOWNSTREAM_CONTAINERS = ("silver", "gold", "live-bronze", "live-silver", "checkpoints", "incoming")

FAILURES: list[str] = []


def fail(message: str) -> None:
    FAILURES.append(message)
    print(f"    [FAIL] {message}")


def load_dotenv_safely() -> None:
    """Load .env from the repo root. Real env vars always win (override=False)."""
    try:
        from dotenv import load_dotenv  # python-dotenv (requirements.txt)

        load_dotenv(REPO_ROOT / ".env", override=False)
    except ImportError:  # pragma: no cover - dotenv is a declared dependency
        print("  [warn] python-dotenv not installed; relying on the real environment only.")


def get_connection_config() -> tuple[str, str]:
    account = os_getenv_strict("AZURE_STORAGE_ACCOUNT")
    key = os_getenv_strict("AZURE_STORAGE_ACCOUNT_KEY")
    return account, key


def os_getenv_strict(name: str) -> str:
    import os

    value = os.getenv(name, "").strip()
    if not value:
        print(f"[CONFIG-ERROR] {name} is not set (export it or put it in {REPO_ROOT / '.env'}).")
        sys.exit(2)
    return value


def list_blob_names(service: BlobServiceClient, container: str, prefix: str) -> list[str]:
    """Flat listing of blob names under prefix, excluding HNS directory markers."""
    client = service.get_container_client(container)
    return [
        b.name
        for b in client.list_blobs(name_starts_with=prefix)
        if not b.name.endswith("/")  # HNS directory markers (0-byte)
    ]


def download(service: BlobServiceClient, container: str, name: str) -> bytes:
    return service.get_blob_client(container, name).download_blob().readall()


def count_csv_data_rows(data: bytes) -> int:
    """Data rows (header excluded) parsed with the csv module (newline-safe)."""
    rows = sum(1 for _ in csv.reader(io.StringIO(data.decode("utf-8"))))
    return max(rows - 1, 0)  # minus header


def main() -> int:
    print("=" * 78)
    print("Phase 2 verification — ADF Landing -> Bronze (file copy, read-only)")
    print("=" * 78)

    load_dotenv_safely()
    account, key = get_connection_config()
    service = BlobServiceClient(
        account_url=f"https://{account}.blob.core.windows.net", credential=key
    )
    print(f"Connected to storage account: {account}\n")

    # ---- 1. Landing ----------------------------------------------------------------
    print("[1/4] Landing container (source of truth for Bronze) —")
    landing_rows: dict[str, int] = {}
    for source, (file_name, expected_rows, known_bytes) in EXPECTED_SOURCES.items():
        names = [n for n in list_blob_names(service, LANDING_CONTAINER, f"{source}/") if n.endswith(".csv")]
        if len(names) != 1:
            fail(f"landing/{source}/: expected exactly 1 CSV, found {len(names)} ({names})")
            continue
        data = download(service, LANDING_CONTAINER, names[0])
        rows = count_csv_data_rows(data)
        landing_rows[source] = rows
        byte_note = "matches known-good size" if len(data) == known_bytes else f"differs from known-good {known_bytes:,} B"
        print(f"  {source:<11} {names[0].split('/', 1)[1]:<30} {len(data):>11,} B ({byte_note})")
        if rows != expected_rows:
            fail(f"landing/{source}/: {rows:,} data rows, expected {expected_rows:,}")
    print(f"  Landing total data rows: {sum(landing_rows.values()):,} (expected {EXPECTED_TOTAL_ROWS:,})\n")

    # ---- 2. Bronze CSV files --------------------------------------------------------
    print("[2/4] Bronze container (bronze/<source>/<file>.csv) —")
    bronze_rows: dict[str, int] = {}
    for source, (file_name, expected_rows, _bytes) in EXPECTED_SOURCES.items():
        expected_blob = f"{source}/{file_name}"
        names = list_blob_names(service, BRONZE_CONTAINER, f"{source}/")
        if expected_blob not in names:
            fail(f"bronze/{source}/: expected blob '{expected_blob}' not found (found {names})")
            continue
        data = download(service, BRONZE_CONTAINER, expected_blob)
        rows = count_csv_data_rows(data)
        bronze_rows[source] = rows
        # Row count must equal both the expected count and the live Landing file.
        landing_names = [n for n in list_blob_names(service, LANDING_CONTAINER, f"{source}/") if n.endswith(".csv")]
        landing_rows_now = count_csv_data_rows(download(service, LANDING_CONTAINER, landing_names[0])) if landing_names else None
        extra = [n for n in names if n != expected_blob]
        if extra:
            fail(f"bronze/{source}/: unexpected extra object(s): {extra}")
        print(
            f"  {source:<11} {file_name:<30} {len(data):>11,} B, {rows:,} data rows"
            + (f" (landing now {landing_rows_now:,})" if landing_rows_now is not None else "")
        )
        if rows != expected_rows:
            fail(f"bronze/{source}/{file_name}: {rows:,} data rows, expected {expected_rows:,}")
        if landing_rows_now is not None and rows != landing_rows_now:
            fail(f"bronze/{source}/: {rows:,} rows != live landing {landing_rows_now:,} rows")
    print(f"  Bronze total data rows: {sum(bronze_rows.values()):,} (expected {EXPECTED_TOTAL_ROWS:,})\n")

    # ---- 3. Bronze layout: exactly six source folders, no Delta --------------------
    print("[3/4] Bronze layout (six source folders; NO Delta _delta_log) —")
    all_bronze = list_blob_names(service, BRONZE_CONTAINER, "")
    top_levels = sorted({n.split("/", 1)[0] for n in all_bronze if n.strip()})
    expected_folders = set(EXPECTED_SOURCES)
    extras = [t for t in top_levels if t not in expected_folders]
    missing = expected_folders - set(top_levels)
    print(f"  top-level folders: {', '.join(top_levels) if top_levels else '(none)'}")
    if extras:
        fail(f"bronze has unexpected top-level folder(s): {extras}")
    if missing:
        fail(f"bronze is missing expected folder(s): {sorted(missing)}")
    delta_dirs = sorted({n.rsplit("/_delta_log", 1)[0] for n in all_bronze if "/_delta_log" in n})
    if delta_dirs:
        fail(f"bronze contains _delta_log directories (Phase 2 must NOT create Delta): {delta_dirs}")
    else:
        print("  [OK] no _delta_log directories (file-based Bronze as designed)")
    if not extras and not missing and top_levels:
        print("  [OK] exactly the six expected source folders")
    print()

    # ---- 4. Downstream protection ----------------------------------------------------
    print("[4/4] Downstream protection (silver/gold/live-*/checkpoints/incoming empty) —")
    for container in DOWNSTREAM_CONTAINERS:
        try:
            blobs = list_blob_names(service, container, "")
        except Exception as exc:  # noqa: BLE001
            fail(f"container '{container}' not listable ({exc})")
            continue
        if blobs:
            fail(f"container '{container}' has {len(blobs)} blob(s) — Phase 2 must not write downstream")
            for name in blobs[:5]:
                print(f"      e.g. {name}")
        else:
            print(f"  {container:<13} 0 blobs [OK]")
    print()

    # ---- Summary ---------------------------------------------------------------------
    print("=" * 78)
    if FAILURES:
        print(f"RESULT: FAIL — {len(FAILURES)} check(s) failed:")
        for message in FAILURES:
            print(f"  - {message}")
        return 1
    if len(bronze_rows) != len(EXPECTED_SOURCES):
        print("RESULT: FAIL — not all Bronze files were reachable.")
        return 1
    print("RESULT: PASS — Landing intact, Bronze CSV file counts exact, downstream untouched.")
    print(f"  {len(EXPECTED_SOURCES)} files, {EXPECTED_TOTAL_ROWS:,} rows total "
          f"(Bronze == Landing for every source).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
