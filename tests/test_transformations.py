"""
Phase 3 tests: Bronze → Silver Spark transformation job.

Covers two layers:

  * Pure (no Spark session required): environment selection, deterministic
    ADLS/local path resolution, schema/registry wiring, and confirmation that
    the Databricks execution path does NOT configure any Azure storage account
    key / SAS / PAT. These always run.

  * Transformation behaviour (require a local Spark + Java runtime): each of the
    six ``transform_*`` functions applied to small in-memory DataFrames,
    asserting cleaning, casting, deduplication, null-handling, and the
    ``_ingested_at`` audit column. These are skipped automatically when no Java
    runtime is available (e.g. this workstation), and do NOT touch ADLS.

These tests never require Azure credentials and never create Azure resources.
"""

import importlib.util
import io
import os
import shutil
import subprocess
import sys
import tokenize
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = REPO_ROOT / "databricks" / "batch" / "bronze_to_silver.py"


def _load_module():
    """Load bronze_to_silver.py by path (databricks/batch has no __init__.py)."""
    spec = importlib.util.spec_from_file_location("bronze_to_silver", MODULE_PATH)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load module from {MODULE_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules["bronze_to_silver"] = module
    spec.loader.exec_module(module)
    return module


bts = _load_module()


def _java_works() -> bool:
    """Return True only if a usable Java runtime is present (Spark needs it).

    ``shutil.which('java')`` is not sufficient: on some systems a stub ``java``
    exists that exits without launching a JVM, which makes the Spark gateway
    fail. We actually invoke ``java -version`` to confirm it runs.
    """
    if not (shutil.which("java") or os.environ.get("JAVA_HOME")):
        return False
    try:
        proc = subprocess.run(
            ["java", "-version"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=20,
            check=False,
        )
        return proc.returncode == 0
    except Exception:  # noqa: BLE001 - any failure means "no usable Java"
        return False


# A local Spark session needs a working Java runtime; skip otherwise.
JAVA_AVAILABLE = _java_works()
requires_spark = pytest.mark.skipif(
    not JAVA_AVAILABLE, reason="local Spark requires a working Java runtime"
)


# ---------------------------------------------------------------------------
# Pure: environment selection & path resolution (no Spark session needed)
# ---------------------------------------------------------------------------


def test_detect_environment_defaults_to_local(monkeypatch):
    monkeypatch.delenv("PIPELINE_ENV", raising=False)
    monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)
    assert bts.detect_environment() == "local"


def test_detect_environment_auto_detects_databricks(monkeypatch):
    monkeypatch.delenv("PIPELINE_ENV", raising=False)
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "16.4.x-snapshot-scala2.13")
    assert bts.detect_environment() == "databricks"


def test_detect_environment_explicit_override(monkeypatch):
    monkeypatch.setenv("PIPELINE_ENV", "local")
    assert bts.detect_environment() == "local"
    monkeypatch.setenv("PIPELINE_ENV", "databricks")
    assert bts.detect_environment() == "databricks"


def test_detect_environment_rejects_invalid_value(monkeypatch):
    monkeypatch.setenv("PIPELINE_ENV", "staging")
    with pytest.raises(bts.PipelineConfigError):
        bts.detect_environment()


def test_databricks_paths_are_deterministic_abfss():
    bronze = bts.get_bronze_path("weather", "databricks")
    silver = bts.get_silver_path("weather", "databricks")
    assert bronze == (
        "abfss://bronze@plantationsimulatorrg.dfs.core.windows.net/"
        "weather/weather_observations.csv"
    )
    assert silver == (
        "abfss://silver@plantationsimulatorrg.dfs.core.windows.net/weather"
    )


def test_local_paths_point_at_repo_data_dirs():
    bronze = bts.get_bronze_path("finance", "local")
    silver = bts.get_silver_path("finance", "local")
    assert bronze.endswith(
        os.path.join("data", "raw", "finance", "sap_finance_transactions.csv")
    )
    assert silver.endswith(os.path.join("data", "silver", "finance"))


def test_no_silent_fallback_every_source_resolves_to_adls_on_databricks():
    for source in bts.SOURCE_FILES:
        assert bts.get_bronze_path(source, "databricks").startswith(
            "abfss://bronze@plantationsimulatorrg.dfs.core.windows.net/"
        )
        assert bts.get_silver_path(source, "databricks").startswith(
            "abfss://silver@plantationsimulatorrg.dfs.core.windows.net/"
        )


def test_local_bronze_validation_fails_loudly_for_missing_file(tmp_path):
    missing = str(tmp_path / "nope" / "missing.csv")
    with pytest.raises(bts.PipelineConfigError):
        bts.validate_local_bronze_path(missing, "weather")


def test_registry_covers_all_six_sources():
    assert (
        set(bts.TRANSFORMATION_REGISTRY)
        == set(bts.SOURCE_FILES)
        == set(bts.SOURCE_ORDER)
    )
    for source, schema in bts.BRONZE_SCHEMAS.items():
        assert source in bts.SOURCE_FILES
        assert len(schema.fields) > 0


def _code_tokens_only(source: str) -> str:
    """Return source with comments and string/docstring literals removed.

    Lets us assert on *executable code* only: the module docstring legitimately
    documents that AZURE_STORAGE_ACCOUNT_KEY is NOT read, so a naive substring
    scan would false-positive on that documentation.
    """
    out = []
    for tok in tokenize.generate_tokens(io.StringIO(source).readline):
        if tok.type in (tokenize.COMMENT, tokenize.STRING):
            continue
        out.append(tok.string)
    return " ".join(out)


def test_no_storage_key_sas_or_pat_in_databricks_path():
    """The Databricks execution path must not configure a storage key/SAS/PAT."""
    code = _code_tokens_only(MODULE_PATH.read_text(encoding="utf-8"))
    assert "AZURE_STORAGE_ACCOUNT_KEY" not in code
    assert "fs.azure.account.key" not in code
    assert "SharedAccessSignature" not in code


# ---------------------------------------------------------------------------
# Transformation behaviour (local Spark; skipped when Java is unavailable)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def spark():
    if not JAVA_AVAILABLE:
        pytest.skip("local Spark requires a Java runtime (not present)")
    os.environ.setdefault("PIPELINE_ENV", "local")
    session = bts.get_spark_session(env="local")
    yield session
    session.stop()


def _rows(df):
    return df.collect()


@requires_spark
def test_transform_weather_dedupes_and_standardizes(spark):
    data = [
        (
            "2024-01-01 00:00:00",
            "stn-north",
            "reg-north",
            26.0,
            85.0,
            0.0,
            5.0,
            "cloudy",
            1010.0,
        ),
        (
            "2024-01-01 00:00:00",
            "STN-NORTH",
            "REG-NORTH",
            26.0,
            85.0,
            0.0,
            5.0,
            "Cloudy",
            1010.0,
        ),
        (
            "2024-01-01 01:00:00",
            "stn-north",
            "reg-north",
            27.0,
            84.0,
            0.0,
            6.0,
            "sunny",
            1011.0,
        ),
        (None, "stn-north", "reg-north", 27.0, 84.0, 0.0, 6.0, "sunny", 1011.0),
    ]
    df = spark.createDataFrame(data, bts.BRONZE_SCHEMAS["weather"])
    out = bts.transform_weather(df)
    assert out.count() == 2
    assert "_ingested_at" in out.columns
    assert str(out.schema["timestamp"].dataType).startswith("TimestampType")
    for r in _rows(out):
        assert r["station_id"] == "STN-NORTH"
        assert r["weather_condition"] in ("CLOUDY", "SUNNY")


@requires_spark
def test_transform_harvest_nullifies_blanks_and_dedupes(spark):
    data = [
        (
            "hvt-1",
            "2024-01-01 08:00:00",
            "blk-1",
            "palm",
            "emp1",
            "",
            100.0,
            "a",
            20.0,
            30,
            "",
            "completed",
        ),
        (
            "HVT-1",
            "2024-01-01 09:00:00",
            "BLK-1",
            "PALM",
            "EMP1",
            "EQ-1",
            100.0,
            "A",
            20.0,
            30,
            "MILL",
            "COMPLETED",
        ),
    ]
    df = spark.createDataFrame(data, bts.BRONZE_SCHEMAS["harvest"])
    out = bts.transform_harvest(df)
    assert out.count() == 1  # deduplicated on harvest_id
    row = _rows(out)[0]
    assert row["harvest_id"] == "HVT-1"
    assert row["status"] == "COMPLETED"
    assert "_ingested_at" in out.columns


@requires_spark
def test_transform_hr_parses_date_and_standardizes(spark):
    data = [
        (
            "att-1",
            "emp1",
            "John Doe",
            "harvester",
            "ops",
            "cc101",
            "2024-01-05",
            "morning",
            None,
            None,
            8.0,
            0.0,
            "present",
            None,
            "field",
            None,
        ),
        (
            "ATT-1",
            "EMP1",
            "John Doe",
            "Harvester",
            "Ops",
            "CC101",
            "2024-01-05",
            "MORNING",
            None,
            None,
            8.0,
            0.0,
            "PRESENT",
            None,
            "FIELD",
            None,
        ),
    ]
    df = spark.createDataFrame(data, bts.BRONZE_SCHEMAS["hr"])
    out = bts.transform_hr(df)
    assert out.count() == 1
    assert str(out.schema["attendance_date"].dataType).startswith("DateType")
    row = _rows(out)[0]
    assert row["attendance_id"] == "ATT-1"
    assert row["cost_center_id"] == "CC101"


@requires_spark
def test_transform_finance_casts_amount_to_decimal(spark):
    data = [
        (
            "doc-1",
            "2024-01-01",
            "2024-01-01 07:30:00",
            2024,
            1,
            "my10",
            "cc101",
            "500100",
            "harvest_labor",
            "ref",
            "",
            "",
            "",
            1671.51,
            "myr",
            "s",
            "debit",
        ),
        (
            "DOC-1",
            "2024-01-01",
            "2024-01-01 07:30:00",
            2024,
            1,
            "MY10",
            "CC101",
            "200100",
            "HARVEST_LABOR",
            "REF",
            "EMP1",
            "",
            "",
            1671.51,
            "MYR",
            "H",
            "credit",
        ),
    ]
    df = spark.createDataFrame(data, bts.BRONZE_SCHEMAS["finance"])
    out = bts.transform_finance(df)
    # distinct (document_id, debit_credit_indicator, gl_account) pairs kept
    assert out.count() == 2
    assert str(out.schema["amount"].dataType) == "DecimalType(18,2)"
    assert str(out.schema["posting_date"].dataType).startswith("DateType")
