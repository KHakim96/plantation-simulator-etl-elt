"""
Phase 1 tests: generated CSV schema contracts.

Validates that the actual generated CSV headers match the column contracts the
generators are documented to emit (and that downstream Bronze/Silver schemas
will be derived from). These run against a small temporary generation so they
are fast and never touch the real ``data/`` folders.

The expected headers below were read directly from each generator's ``headers``
list — they are not invented. If a generator's output changes, update the
generator and these contracts together.
"""

import csv

import pytest
import yaml

from data_generators.fetch_weather_api import load_config
from data_generators import run_batch_generators as runner

CONFIG_PATH = "data_generators/config.yaml"

# Header contracts per source, mirroring the `headers` lists in the generators.
EXPECTED_HEADERS = {
    "weather": [
        "timestamp", "station_id", "region_id", "temperature_c", "humidity_pct",
        "rainfall_mm", "wind_speed_kmh", "weather_condition", "pressure_hpa",
    ],
    "harvest": [
        "harvest_id", "timestamp", "block_id", "crop_type", "employee_id",
        "equipment_id", "harvested_weight_kg", "quality_grade", "moisture_pct",
        "collection_duration_minutes", "destination", "status",
    ],
    "fertilizer": [
        "application_id", "timestamp", "block_id", "crop_type", "employee_id",
        "material_id", "quantity_kg", "application_method", "equipment_id",
        "weather_station_id", "weather_condition", "rainfall_mm",
        "application_status", "notes",
    ],
    "equipment": [
        "operation_id", "timestamp", "equipment_id", "equipment_type", "block_id",
        "operator_id", "operation_type", "start_time", "end_time",
        "duration_minutes", "engine_hours", "fuel_consumption_liters",
        "distance_km", "maintenance_flag", "maintenance_type", "status",
    ],
    "hr": [
        "attendance_id", "employee_id", "employee_name", "role", "department",
        "cost_center_id", "attendance_date", "shift", "check_in_time",
        "check_out_time", "working_hours", "overtime_hours", "attendance_status",
        "leave_type", "work_location", "remarks",
    ],
    "finance": [
        "document_id", "posting_date", "posting_timestamp", "fiscal_year",
        "fiscal_period", "company_code", "cost_center_id", "gl_account",
        "transaction_type", "reference_document", "employee_id", "equipment_id",
        "material_id", "amount", "currency", "debit_credit_indicator", "description",
    ],
}

EXPECTED_FILES = {
    "weather": "weather_observations.csv",
    "harvest": "harvest_transactions.csv",
    "fertilizer": "fertilizer_applications.csv",
    "equipment": "equipment_logs.csv",
    "hr": "hr_attendance.csv",
    "finance": "sap_finance_transactions.csv",
}


@pytest.fixture(scope="module")
def generated(tmp_path_factory):
    """Run the batch once into a temp dir and return (config, out_root)."""
    tmp_path = tmp_path_factory.mktemp("schema")
    cfg = load_config(CONFIG_PATH)
    cfg["global_settings"] = dict(cfg["global_settings"])
    cfg["global_settings"]["generation_period"] = {
        "start_date": "2024-01-01",
        "end_date": "2024-01-05",
    }
    cfg["dataset_sizes"] = {
        "harvest": 200, "fertilizer": 150, "equipment": 150,
        "hr": 200, "finance": 200, "sensors": 100,
    }
    out_root = tmp_path / "raw"
    cfg["output_settings"] = {"output_paths": {s: str(out_root / s) for s in EXPECTED_FILES}}
    cfg_path = tmp_path / "config.yaml"
    with open(cfg_path, "w", encoding="utf-8") as f:
        yaml.safe_dump(cfg, f)
    runner.run_batch(str(cfg_path))
    return cfg, out_root


def _read_header_and_rows(path):
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        rows = list(reader)
    return header, rows


@pytest.mark.parametrize("source", sorted(EXPECTED_HEADERS.keys()))
def test_csv_header_matches_contract(generated, source):
    _, out_root = generated
    out_file = out_root / source / EXPECTED_FILES[source]
    assert out_file.is_file(), f"missing output: {out_file}"
    header, rows = _read_header_and_rows(out_file)
    assert header == EXPECTED_HEADERS[source], (
        f"{source} header mismatch.\n  expected: {EXPECTED_HEADERS[source]}\n  actual:   {header}"
    )
    assert len(rows) > 0


def test_finance_is_double_entry(generated):
    """Every SAP document must have exactly one S (debit) and one H (credit)."""
    _, out_root = generated
    header, rows = _read_header_and_rows(out_root / "finance" / EXPECTED_FILES["finance"])
    doc_idx = header.index("document_id")
    dc_idx = header.index("debit_credit_indicator")
    indicators = {}
    for r in rows:
        indicators.setdefault(r[doc_idx], []).append(r[dc_idx])
    for doc_id, flags in indicators.items():
        assert sorted(flags) == ["H", "S"], f"{doc_id} not a balanced S/H pair: {flags}"


def test_harvest_cancelled_rows_use_sentinel_values(generated):
    """CANCELLED harvest rows carry the documented sentinel pattern (weight 0,
    destination N/A, grade REJECT) — recorded for downstream Silver cleaning."""
    _, out_root = generated
    header, rows = _read_header_and_rows(out_root / "harvest" / EXPECTED_FILES["harvest"])
    s_idx = header.index("status")
    if not any(r[s_idx] == "CANCELLED" for r in rows):
        pytest.skip("no CANCELLED rows in the small sample window")
    w_idx = header.index("harvested_weight_kg")
    d_idx = header.index("destination")
    g_idx = header.index("quality_grade")
    for r in rows:
        if r[s_idx] == "CANCELLED":
            assert float(r[w_idx]) == 0.0
            assert r[d_idx] == "N/A"
            assert r[g_idx] == "REJECT"
