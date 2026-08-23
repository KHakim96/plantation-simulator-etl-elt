"""
Phase 1 tests: source-data generators and the ADLS Landing uploader.

Covers:
  * config.yaml loads and contains the required sections/keys
  * the six batch generators run and create their CSV files
  * generated CSVs have non-zero row counts
  * the uploader targets ADLS Landing ONLY (never Bronze/Silver/Gold)

These tests generate into a temporary directory (monkeypatched output_paths) so
they never touch the real ``data/`` scratch folders and run quickly on a small
date window. They do NOT create any Azure resources and do NOT require network
or credentials.
"""

import csv
import importlib
from pathlib import Path

import pytest
import yaml

from data_generators.fetch_weather_api import load_config
from data_generators import run_batch_generators as runner
from data_generators import upload_to_adls as uploader

CONFIG_PATH = "data_generators/config.yaml"

# Expected CSV filename per batch source (derived from the generator code).
EXPECTED_FILES = {
    "weather": "weather_observations.csv",
    "harvest": "harvest_transactions.csv",
    "fertilizer": "fertilizer_applications.csv",
    "equipment": "equipment_logs.csv",
    "hr": "hr_attendance.csv",
    "finance": "sap_finance_transactions.csv",
}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def base_config():
    """Load the real master config."""
    return load_config(CONFIG_PATH)


@pytest.fixture()
def small_config(tmp_path, base_config):
    """
    Return a config scaled down for a fast test run: a 5-day window, tiny target
    row counts, and output redirected into a pytest tmp_path. Written to a real
    file so generators that re-load config from disk behave identically.
    """
    cfg = dict(base_config)
    cfg["global_settings"] = dict(base_config["global_settings"])
    cfg["global_settings"]["generation_period"] = {
        "start_date": "2024-01-01",
        "end_date": "2024-01-05",
    }
    cfg["dataset_sizes"] = {
        "harvest": 200,
        "fertilizer": 150,
        "equipment": 150,
        "hr": 200,
        "finance": 200,
        "sensors": 100,
    }
    out_root = tmp_path / "raw"
    cfg["output_settings"] = {
        "output_paths": {
            name: str(out_root / name) for name in EXPECTED_FILES
        }
    }
    cfg_path = tmp_path / "config.yaml"
    with open(cfg_path, "w", encoding="utf-8") as f:
        yaml.safe_dump(cfg, f)
    return cfg, str(cfg_path), out_root


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------

def test_config_file_exists():
    assert Path(CONFIG_PATH).is_file(), "data_generators/config.yaml is missing"


def test_config_loads_and_has_required_sections(base_config):
    for section in (
        "global_settings",
        "output_settings",
        "dataset_sizes",
        "generator_settings",
        "weather_stations",
        "blocks",
        "crop_types",
        "employee_master",
        "equipment_master",
        "materials",
        "cost_centers",
    ):
        assert section in base_config, f"config missing section: {section}"


def test_config_output_paths_cover_all_batch_sources(base_config):
    paths = base_config["output_settings"]["output_paths"]
    for source in EXPECTED_FILES:
        assert source in paths, f"output_paths missing: {source}"


def test_config_master_data_non_empty(base_config):
    assert len(base_config["weather_stations"]) >= 1
    assert len(base_config["blocks"]) >= 1
    assert len(base_config["employee_master"]) >= 1
    assert len(base_config["equipment_master"]) >= 1
    assert len(base_config["materials"]) >= 1
    assert len(base_config["cost_centers"]) >= 1


def test_config_blocks_reference_known_regions_and_crops(base_config):
    region_ids = {s["region_id"] for s in base_config["weather_stations"]}
    crop_names = {c["name"] for c in base_config["crop_types"]}
    for block in base_config["blocks"]:
        assert block["region_id"] in region_ids, f"block {block['id']} unknown region"
        assert block["crop_type"] in crop_names, f"block {block['id']} unknown crop"


# ---------------------------------------------------------------------------
# Generator execution: files created + non-zero row counts
# ---------------------------------------------------------------------------

def test_generators_run_and_produce_nonzero_csvs(small_config):
    cfg, cfg_path, out_root = small_config
    results = runner.run_batch(cfg_path)

    # Every planned step ran and returned a path.
    assert set(results.keys()) == set(EXPECTED_FILES.keys())

    for source, filename in EXPECTED_FILES.items():
        out_file = out_root / source / filename
        assert out_file.is_file(), f"missing output: {out_file}"
        with open(out_file, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)
            rows = list(reader)
        assert len(header) > 0, f"{source}: empty header"
        assert len(rows) > 0, f"{source}: zero data rows generated"


def test_runner_order_is_weather_first_finance_last():
    plan_names = [name for name, _ in runner.GENERATION_PLAN]
    assert plan_names[0] == "weather"
    assert plan_names[-1] == "finance"
    # HR must come after harvest & equipment (active-worker cross-check).
    assert plan_names.index("hr") > plan_names.index("harvest")
    assert plan_names.index("hr") > plan_names.index("equipment")
    # Sensors must NOT be part of the batch plan (Phase 7).
    assert "sensors" not in plan_names


# ---------------------------------------------------------------------------
# Uploader: Landing-only contract
# ---------------------------------------------------------------------------

def test_uploader_discovers_only_batch_sources(small_config):
    cfg, cfg_path, out_root = small_config
    runner.run_batch(cfg_path)
    files = uploader.collect_batch_files(cfg)
    sources = {f["source"] for f in files}
    # All six batch sources discovered; sensors excluded.
    assert sources == set(EXPECTED_FILES.keys())
    assert "sensors" not in sources
    # Blob paths preserve the <source>/<filename> structure.
    for f in files:
        assert f["blob_path"].startswith(f["source"] + "/")
        assert f["blob_path"].endswith(".csv")


def test_uploader_container_is_landing(monkeypatch):
    monkeypatch.delenv("ADLS_LANDING_CONTAINER", raising=False)
    container = uploader.resolve_landing_container()
    assert container == "landing"
    assert uploader._guard_landing_only(container) == "landing"


def test_uploader_rejects_downstream_layers():
    for forbidden in ("bronze", "silver", "gold", "incoming", "live-bronze", "live-silver", "checkpoints"):
        with pytest.raises(uploader.LandingOnlyViolation):
            uploader._guard_landing_only(forbidden)


def test_uploader_dry_run_makes_no_network_and_reports_landing(small_config, monkeypatch):
    # Ensure dry-run (no credentials) so no network call is made.
    monkeypatch.delenv("AZURE_STORAGE_ACCOUNT_KEY", raising=False)
    monkeypatch.delenv("AZURE_STORAGE_ACCOUNT", raising=False)
    cfg, cfg_path, out_root = small_config
    runner.run_batch(cfg_path)

    summary = uploader.upload_to_landing(cfg_path)
    assert summary["dry_run"] is True
    assert summary["layer"] == "landing"
    assert summary["container"] == "landing"
    assert summary["file_count"] == len(EXPECTED_FILES)
    for dest in summary["destinations"]:
        assert dest.startswith("landing/")
        # No destination may point at a downstream layer.
        assert not any(
            dest.startswith(layer + "/")
            for layer in ("bronze", "silver", "gold", "incoming")
        )


# ---------------------------------------------------------------------------
# Uploader: Shared Key string-to-sign construction (no network, no real key)
# ---------------------------------------------------------------------------

# A dummy base64-encoded 32-byte key — NOT a real secret, used only to make the
# signing code deterministic and assertable offline.
_TEST_KEY = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="


def _sign(account, key, method, path, headers, length=0):
    return uploader._shared_key_authorization(account, key, method, path, headers, length)


def test_shared_key_signature_places_query_params_on_separate_lines():
    """Regression test for the HTTP 403 signature bug: query parameters must be
    signed as separate 'name:value' lines AFTER the canonicalized resource,
    sorted by lowercase parameter name — never embedded in the resource path."""
    headers = {"x-ms-date": "Sat, 22 Aug 2026 19:19:15 GMT", "x-ms-version": "2021-08-06"}
    auth = _sign("myaccount", _TEST_KEY, "GET", "landing?resource=filesystem&recursive=true", headers)

    # Reconstruct the string-to-sign the same way the implementation does and
    # verify its structure independently of the signing itself.
    import base64
    import hmac as hmac_mod
    import hashlib

    expected_sts = "\n".join(
        [
            "GET", "", "", "", "", "", "", "", "", "", "", "",
            "x-ms-date:Sat, 22 Aug 2026 19:19:15 GMT",
            "x-ms-version:2021-08-06",
            "/myaccount/landing",
            "recursive:true",
            "resource:filesystem",
        ]
    )
    expected_sig = base64.b64encode(
        hmac_mod.new(base64.b64decode(_TEST_KEY), expected_sts.encode(), hashlib.sha256).digest()
    ).decode()
    assert auth == f"SharedKey myaccount:{expected_sig}"

    # The query string must NOT leak into the canonicalized resource line.
    assert "/myaccount/landing?resource=filesystem" not in auth
    # Signing is deterministic for identical inputs.
    assert auth == _sign("myaccount", _TEST_KEY, "GET", "landing?resource=filesystem&recursive=true", headers)


def test_shared_key_signature_sorts_query_params_by_name():
    """Shared Key requires query parameters sorted by (lowercased) name."""
    headers = {"x-ms-date": "Sat, 22 Aug 2026 19:19:15 GMT", "x-ms-version": "2021-08-06"}
    auth_sorted = _sign("a", _TEST_KEY, "GET", "x?resource=filesystem&recursive=true", headers)
    auth_unsorted = _sign("a", _TEST_KEY, "GET", "x?recursive=true&resource=filesystem", headers)
    # Different raw order, identical canonical signature.
    assert auth_sorted == auth_unsorted


def test_shared_key_signature_strips_leading_slash_and_keeps_path():
    headers = {"x-ms-date": "Sat, 22 Aug 2026 19:19:15 GMT", "x-ms-version": "2021-08-06"}
    # Deep blob path with a query string, as used by the append/flush calls.
    auth = _sign("acc", _TEST_KEY, "PATCH", "/landing/hr/hr_attendance.csv?action=append&position=0", headers)
    import base64, hmac as hmac_mod, hashlib
    expected_sts = "\n".join(
        [
            "PATCH", "", "", "", "", "", "", "", "", "", "", "",
            "x-ms-date:Sat, 22 Aug 2026 19:19:15 GMT",
            "x-ms-version:2021-08-06",
            "/acc/landing/hr/hr_attendance.csv",
            "action:append",
            "position:0",
        ]
    )
    expected_sig = base64.b64encode(
        hmac_mod.new(base64.b64decode(_TEST_KEY), expected_sts.encode(), hashlib.sha256).digest()
    ).decode()
    assert auth == f"SharedKey acc:{expected_sig}"


def test_uploader_dry_run_when_only_account_set(monkeypatch, small_config):
    """An account name without a key must still dry-run (no network, no crash)."""
    monkeypatch.delenv("AZURE_STORAGE_ACCOUNT_KEY", raising=False)
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT", "someaccount")
    cfg, cfg_path, out_root = small_config
    runner.run_batch(cfg_path)
    summary = uploader.upload_to_landing(cfg_path)
    assert summary["dry_run"] is True
