"""
Phase 5 tests: Silver → Gold Spark transformation job.

Two layers, mirroring tests/test_transformations.py and
tests/test_data_quality.py:

  * Pure (no Spark session required): module loading, Gold model registry
    coverage, Gold path resolution, key-column definitions, no-storage-key
    guard, Serverless-compatibility guard, and idempotent write mode. These
    always run.

  * Transformation behaviour (require a local Spark + Java runtime): each
    ``build_*`` function applied to small in-memory DataFrames, asserting
    output columns, grain/deduplication, null handling, and correct types.
    These are skipped automatically when no Java runtime is available and do
    NOT touch ADLS.

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
MODULE_PATH = REPO_ROOT / "databricks" / "batch" / "silver_to_gold.py"
BTS_PATH = REPO_ROOT / "databricks" / "batch" / "bronze_to_silver.py"

EXPECTED_MODELS = (
    "dim_equipment",
    "dim_employee",
    "fact_harvest",
    "fact_revenue",
    "fact_fertilizer",
    "fact_equipment",
)


def _load_module(path: Path, name: str):
    """Load a module by path (databricks/batch has no __init__.py)."""
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load module from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


bts = _load_module(BTS_PATH, "bronze_to_silver")
stg = _load_module(MODULE_PATH, "silver_to_gold")


def _java_works() -> bool:
    """Return True only if a usable Java runtime is present (Spark needs it)."""
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


JAVA_AVAILABLE = _java_works()
requires_spark = pytest.mark.skipif(
    not JAVA_AVAILABLE, reason="local Spark requires a working Java runtime"
)


# ---------------------------------------------------------------------------
# Pure: module loading, registry coverage, paths, keys, no-secrets, Serverless
# ---------------------------------------------------------------------------


def test_module_loads_and_reuses_phase3_helpers():
    """Silver_to_gold must reuse Phase 3 helpers, not duplicate them."""
    assert stg.detect_environment is not None
    assert stg.get_spark_session is not None
    assert stg.get_silver_path is not None
    assert stg.is_databricks_environment is not None
    # Behavior parity on a representative path resolution.
    assert stg.get_silver_path("finance", "databricks") == bts.get_silver_path(
        "finance", "databricks"
    )


def test_gold_model_registry_covers_all_models():
    """Every model in GOLD_MODEL_ORDER has a registry entry and key columns."""
    assert set(stg.GOLD_MODEL_ORDER) == set(stg.GOLD_MODEL_REGISTRY)
    assert set(stg.GOLD_MODEL_ORDER) == set(stg.GOLD_KEY_COLUMNS)
    assert stg.GOLD_MODEL_ORDER == EXPECTED_MODELS


def test_each_registry_entry_has_source_and_callable():
    """Each registry entry maps to (source_name, transform_function)."""
    for model_name, (source, func) in stg.GOLD_MODEL_REGISTRY.items():
        assert isinstance(source, str), f"{model_name}: source not a string"
        assert callable(func), f"{model_name}: transform not callable"
        # Source must be one of the six Silver datasets.
        assert source in bts.SOURCE_FILES, (
            f"{model_name}: unknown source '{source}'"
        )


def test_dim_plantation_is_excluded():
    """dim_plantation must NOT be implemented — no plantation master table
    exists in Silver to build it from without fabricating data."""
    assert "dim_plantation" not in stg.GOLD_MODEL_REGISTRY
    assert "dim_plantation" not in stg.GOLD_MODEL_ORDER


def test_gold_paths_are_deterministic_abfss():
    """Gold paths on Databricks must be abfss:// in the gold container."""
    for model in EXPECTED_MODELS:
        path = stg.get_gold_path(model, "databricks")
        assert path == (
            f"abfss://gold@plantationsimulatorrg.dfs.core.windows.net/{model}"
        )


def test_gold_paths_local_point_at_repo_data_gold():
    """Local Gold paths must resolve inside the repo data/gold directory."""
    for model in EXPECTED_MODELS:
        path = stg.get_gold_path(model, "local")
        assert path.endswith(os.path.join("data", "gold", model))


def test_silver_input_paths_match_phase3():
    """Silver read paths must be identical to Phase 3 output paths."""
    for source in bts.SOURCE_FILES:
        assert stg.get_silver_path(
            source, "databricks"
        ) == bts.get_silver_path(source, "databricks")


def test_gold_key_columns_match_model_grain():
    """Key columns define the grain of each Gold model."""
    assert stg.GOLD_KEY_COLUMNS["dim_equipment"] == ["equipment_id"]
    assert stg.GOLD_KEY_COLUMNS["dim_employee"] == ["employee_id"]
    assert stg.GOLD_KEY_COLUMNS["fact_harvest"] == ["harvest_id"]
    assert stg.GOLD_KEY_COLUMNS["fact_revenue"] == [
        "document_id",
        "debit_credit_indicator",
        "gl_account",
    ]
    assert stg.GOLD_KEY_COLUMNS["fact_fertilizer"] == ["application_id"]
    assert stg.GOLD_KEY_COLUMNS["fact_equipment"] == ["operation_id"]


def _code_tokens_only(source: str) -> str:
    """Return source with comments and string/docstring literals removed."""
    out = []
    for tok in tokenize.generate_tokens(io.StringIO(source).readline):
        if tok.type in (tokenize.COMMENT, tokenize.STRING):
            continue
        out.append(tok.string)
    return " ".join(out)


def test_no_storage_key_sas_or_pat_in_module():
    """The module must not configure a storage key/SAS/PAT."""
    code = _code_tokens_only(MODULE_PATH.read_text(encoding="utf-8"))
    assert "AZURE_STORAGE_ACCOUNT_KEY" not in code
    assert "fs.azure.account.key" not in code
    assert "SharedAccessSignature" not in code


def test_no_serverless_incompatible_calls():
    """Serverless (Spark Connect) rejects cache/persist/PERSIST TABLE and
    temp views. The Gold job must not use any of these."""
    code = _code_tokens_only(MODULE_PATH.read_text(encoding="utf-8"))
    for forbidden in (
        ".cache(",
        ".persist(",
        ".unpersist(",
        "saveAsTable",
        "createOrReplaceTempView",
        "createTempView",
        "PERSIST TABLE",
    ):
        assert forbidden not in code, (
            f"Serverless-incompatible call found: {forbidden}"
        )


def test_write_mode_is_overwrite():
    """Gold writes must use mode=overwrite for idempotent full refresh.

    Check the raw source (not tokenized code) because the mode value is a
    string literal that _code_tokens_only would strip.
    """
    raw = MODULE_PATH.read_text(encoding="utf-8")
    assert '"overwrite"' in raw
    assert '"overwriteSchema"' in raw


def test_no_dbt_references():
    """Phase 5 must not reintroduce dbt."""
    code = MODULE_PATH.read_text(encoding="utf-8").lower()
    assert "dbt" not in code


# ---------------------------------------------------------------------------
# Transformation behaviour (local Spark; skipped when Java is unavailable)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def spark():
    if not JAVA_AVAILABLE:
        pytest.skip("local Spark requires a Java runtime (not present)")
    os.environ.setdefault("PIPELINE_ENV", "local")
    session = bts.get_spark_session(
        app_name="Plantation_Gold_Tests", env="local"
    )
    yield session
    session.stop()


@requires_spark
def test_dim_equipment_dedupes_and_has_correct_columns(spark):
    """dim_equipment: one row per equipment_id, correct columns."""
    rows = [
        ("op-1", "2024-01-01 08:00:00", "EQP001", "TRACTOR", "BLK01",
         "EMP1", "HARVESTING", "2024-01-01 08:00:00", "2024-01-01 12:00:00",
         240, 4.0, 50.0, 12.5, False, None, "COMPLETED"),
        ("op-2", "2024-01-02 08:00:00", "EQP001", "TRACTOR", "BLK02",
         "EMP2", "TRANSPORT", "2024-01-02 08:00:00", "2024-01-02 10:00:00",
         120, 2.0, 25.0, 8.0, False, None, "COMPLETED"),
        ("op-3", "2024-01-01 09:00:00", "EQP002", "HARVESTER", "BLK01",
         "EMP1", "HARVESTING", "2024-01-01 09:00:00", "2024-01-01 15:00:00",
         360, 6.0, 80.0, 15.0, False, None, "COMPLETED"),
    ]
    df = spark.createDataFrame(rows, bts.BRONZE_SCHEMAS["equipment"])
    # Apply the Silver transformation first (simulating real Silver data).
    silver_df = bts.transform_equipment(df)
    result = stg.build_dim_equipment(silver_df)
    assert result.count() == 2  # EQP001 and EQP002
    assert set(result.columns) == {"equipment_id", "equipment_type"}
    collected = {r["equipment_id"]: r["equipment_type"] for r in result.collect()}
    assert collected == {"EQP001": "TRACTOR", "EQP002": "HARVESTER"}


@requires_spark
def test_dim_employee_dedupes_and_has_correct_columns(spark):
    """dim_employee: one row per employee_id, correct columns."""
    rows = [
        ("att-1", "EMP1", "John Doe", "Harvester", "Ops", "CC101",
         "2024-01-05", "MORNING", None, None, 8.0, 0.0, "PRESENT",
         None, "FIELD", None),
        ("att-2", "EMP1", "John Doe", "Harvester", "Ops", "CC101",
         "2024-01-06", "MORNING", None, None, 8.0, 0.0, "PRESENT",
         None, "FIELD", None),
        ("att-3", "EMP2", "Jane Smith", "Driver", "Logistics", "CC102",
         "2024-01-05", "AFTERNOON", None, None, 7.5, 1.0, "PRESENT",
         None, "FIELD", None),
    ]
    df = spark.createDataFrame(rows, bts.BRONZE_SCHEMAS["hr"])
    silver_df = bts.transform_hr(df)
    result = stg.build_dim_employee(silver_df)
    assert result.count() == 2  # EMP1 and EMP2
    assert set(result.columns) == {
        "employee_id", "employee_name", "role", "department",
        "cost_center_id",
    }
    collected = {r["employee_id"]: r["employee_name"] for r in result.collect()}
    assert collected == {"EMP1": "John Doe", "EMP2": "Jane Smith"}


@requires_spark
def test_fact_harvest_grain_and_columns(spark):
    """fact_harvest: one row per harvest_id, has date + timestamp + measures."""
    rows = [
        ("hvt-1", "2024-01-15 08:30:00", "BLK01", "PALM", "EMP1", "EQP001",
         500.0, "A", 22.0, 45, "MILL", "COMPLETED"),
        ("hvt-2", "2024-01-15 10:00:00", "BLK02", "PALM", "EMP2", None,
         350.0, "B", 25.0, 30, "MILL", "COMPLETED"),
    ]
    df = spark.createDataFrame(rows, bts.BRONZE_SCHEMAS["harvest"])
    silver_df = bts.transform_harvest(df)
    result = stg.build_fact_harvest(silver_df)
    assert result.count() == 2
    assert "harvest_date" in result.columns
    assert "harvest_timestamp" in result.columns
    assert "harvested_weight_kg" in result.columns
    assert "moisture_pct" in result.columns
    assert str(result.schema["harvest_date"].dataType).startswith("DateType")
    # No duplicates on business key.
    assert result.dropDuplicates(["harvest_id"]).count() == result.count()


@requires_spark
def test_fact_revenue_grain_and_decimal_type(spark):
    """fact_revenue: grain is (document_id, debit_credit_indicator, gl_account),
    amount is DecimalType(18,2)."""
    rows = [
        ("doc-1", "2024-01-15", "2024-01-15 07:30:00", 2024, 1,
         "MY10", "CC101", "500100", "HARVEST_LABOR", "HVT-1",
         "EMP1", "", "", 1671.51, "MYR", "S", "Harvest labor cost"),
        ("doc-1", "2024-01-15", "2024-01-15 07:30:00", 2024, 1,
         "MY10", "CC101", "200100", "HARVEST_LABOR", "HVT-1",
         "EMP1", "", "", 1671.51, "MYR", "H", "Harvest labor offset"),
    ]
    df = spark.createDataFrame(rows, bts.BRONZE_SCHEMAS["finance"])
    silver_df = bts.transform_finance(df)
    result = stg.build_fact_revenue(silver_df)
    assert result.count() == 2  # S and H lines
    assert str(result.schema["amount"].dataType) == "DecimalType(18,2)"
    # No duplicates on business key.
    keys = ["document_id", "debit_credit_indicator", "gl_account"]
    assert result.dropDuplicates(keys).count() == result.count()


@requires_spark
def test_fact_fertilizer_grain_and_columns(spark):
    """fact_fertilizer: one row per application_id, has date + timestamp."""
    rows = [
        ("app-1", "2024-01-10 06:00:00", "BLK01", "PALM", "EMP1", "MAT01",
         50.0, "BROADCAST", "EQP001", "STN-NORTH", "SUNNY", 0.0,
         "COMPLETED", None),
        ("app-2", "2024-01-11 06:00:00", "BLK02", "PALM", "EMP2", "MAT02",
         30.0, "SPRAY", None, "STN-SOUTH", "CLOUDY", 2.0,
         "COMPLETED", None),
    ]
    df = spark.createDataFrame(rows, bts.BRONZE_SCHEMAS["fertilizer"])
    silver_df = bts.transform_fertilizer(df)
    result = stg.build_fact_fertilizer(silver_df)
    assert result.count() == 2
    assert "application_date" in result.columns
    assert "application_timestamp" in result.columns
    assert "quantity_kg" in result.columns
    assert str(result.schema["application_date"].dataType).startswith("DateType")
    assert result.dropDuplicates(["application_id"]).count() == result.count()


@requires_spark
def test_fact_equipment_grain_and_columns(spark):
    """fact_equipment: one row per operation_id, has date + timestamp +
    numeric measures."""
    rows = [
        ("op-1", "2024-01-01 08:00:00", "EQP001", "TRACTOR", "BLK01",
         "EMP1", "HARVESTING", "2024-01-01 08:00:00", "2024-01-01 12:00:00",
         240, 4.0, 50.0, 12.5, False, None, "COMPLETED"),
        ("op-2", "2024-01-02 08:00:00", "EQP002", "HARVESTER", "BLK02",
         "EMP2", "HARVESTING", "2024-01-02 08:00:00", "2024-01-02 16:00:00",
         480, 8.0, 120.0, 20.0, True, "ENGINE", "BREAKDOWN"),
    ]
    df = spark.createDataFrame(rows, bts.BRONZE_SCHEMAS["equipment"])
    silver_df = bts.transform_equipment(df)
    result = stg.build_fact_equipment(silver_df)
    assert result.count() == 2
    assert "operation_date" in result.columns
    assert "operation_timestamp" in result.columns
    assert "duration_minutes" in result.columns
    assert "engine_hours" in result.columns
    assert "fuel_consumption_liters" in result.columns
    assert str(result.schema["operation_date"].dataType).startswith("DateType")
    assert result.dropDuplicates(["operation_id"]).count() == result.count()


@requires_spark
def test_fact_harvest_null_equipment_id_preserved(spark):
    """Harvest rows with NULL equipment_id (manual harvest) must be kept."""
    rows = [
        ("hvt-1", "2024-01-15 08:30:00", "BLK01", "PALM", "EMP1", None,
         500.0, "A", 22.0, 45, "MILL", "COMPLETED"),
    ]
    df = spark.createDataFrame(rows, bts.BRONZE_SCHEMAS["harvest"])
    silver_df = bts.transform_harvest(df)
    result = stg.build_fact_harvest(silver_df)
    assert result.count() == 1
    row = result.collect()[0]
    assert row["equipment_id"] is None


@requires_spark
def test_dim_equipment_null_equipment_id_excluded(spark):
    """Equipment rows with NULL equipment_id must not appear in the dim."""
    rows = [
        ("op-1", "2024-01-01 08:00:00", None, "TRACTOR", "BLK01",
         "EMP1", "MAINTENANCE", "2024-01-01 08:00:00", "2024-01-01 09:00:00",
         60, 1.0, 5.0, 0.0, True, "ENGINE", "COMPLETED"),
    ]
    df = spark.createDataFrame(rows, bts.BRONZE_SCHEMAS["equipment"])
    silver_df = bts.transform_equipment(df)
    result = stg.build_dim_equipment(silver_df)
    assert result.count() == 0
