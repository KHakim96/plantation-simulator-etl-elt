"""Phase 7 tests: live sensor streaming path.

Two layers, mirroring tests/test_transformations.py and tests/test_data_quality.py:

  * Pure (no Spark session required): module loading, deterministic ADLS/local
    path resolution, sensor CSV schema contract, Incoming-only guard,
    no-storage-key/SAS/PAT guard, Serverless-compatibility guard, and
    checkpoint path determinism. These always run.

  * Transformation behaviour (require a local Spark + Java runtime): the live
    Silver transformation applied to a small in-memory DataFrame, asserting
    type casting, standardization, deduplication, and the ``_ingested_at``
    audit column. These are skipped automatically when no Java runtime is
    available (e.g. this workstation), and do NOT touch ADLS.

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
MODULE_PATH = REPO_ROOT / "databricks" / "streaming" / "sensors_stream.py"
BTS_PATH = REPO_ROOT / "databricks" / "batch" / "bronze_to_silver.py"
GENERATOR_MODULE_PATH = REPO_ROOT / "data_generators" / "sensor_stream_to_adls.py"


def _load_module(path: Path, name: str):
    """Load a module by path (databricks/streaming has no __init__.py)."""
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load module from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


bts = _load_module(BTS_PATH, "bronze_to_silver")
ss = _load_module(MODULE_PATH, "sensors_stream")
gen = _load_module(GENERATOR_MODULE_PATH, "sensor_stream_to_adls")


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
# Pure: module loading, path resolution, schema, guards (no Spark session)
# ---------------------------------------------------------------------------


def test_module_loads_and_reuses_phase3_helpers():
    """sensors_stream must reuse Phase 3 helpers, not duplicate them."""
    assert ss.detect_environment is not None
    assert ss.get_spark_session is not None
    assert ss.is_databricks_environment is not None
    # Behavior parity on a representative path resolution.
    assert ss.get_incoming_path("databricks").startswith(
        "abfss://incoming@plantationsimulatorrg.dfs.core.windows.net/"
    )


def _exec_module_no_file(module_globals: dict, source: str) -> dict:
    """Execute module source in a namespace WITHOUT __file__ defined.

    Simulates the Databricks Git-backed Serverless execution environment, where
    the executed Python file has no ``__file__`` global. ``__file__`` is
    explicitly removed from the namespace before exec.
    """
    module_globals.pop("__file__", None)
    exec(compile(source, "<sensors_stream_databricks>", "exec"), module_globals)  # noqa: S102
    return module_globals


def test_bronze_to_silver_import_resolves_without___file__(tmp_path, monkeypatch):
    """Reproduce the Databricks failure: no __file__, batch not pre-imported.

    Simulates Databricks Git-backed execution by:
      * executing the real sensors_stream.py source in a namespace with NO
        ``__file__`` defined,
      * pointing the CWD at the repo root (as Databricks does),
      * ensuring ``bronze_to_silver`` is not already importable from the
        streaming folder (it lives in the sibling ``batch`` folder).
    The loader must discover databricks/batch by probing the filesystem and
    import bronze_to_silver via a normal module import.
    """
    # Ensure a clean import slate for the module name under test.
    monkeypatch.delitem(sys.modules, "bronze_to_silver", raising=False)
    monkeypatch.delitem(sys.modules, "sensors_stream", raising=False)
    # Remove any pre-seeded path to databricks/batch so the probe must find it.
    batch_dir = str(REPO_ROOT / "databricks" / "batch")
    monkeypatch.setattr(
        sys, "path", [p for p in sys.path if os.path.abspath(p or os.curdir) != batch_dir]
    )
    # Databricks runs with the repo root as CWD.
    monkeypatch.chdir(REPO_ROOT)

    source = MODULE_PATH.read_text(encoding="utf-8")
    namespace = {"__name__": "sensors_stream_databricks_sim"}
    _exec_module_no_file(namespace, source)

    # The executed module namespace must expose the reused Phase 3 helpers.
    assert "__file__" not in namespace
    assert callable(namespace["detect_environment"])
    assert callable(namespace["get_spark_session"])
    assert callable(namespace["is_databricks_environment"])
    # And it must have imported the REAL bronze_to_silver module.
    assert namespace["bts"].__name__ == "bronze_to_silver"
    assert namespace["bts"].STORAGE_ACCOUNT == "plantationsimulatorrg"


def test_candidate_batch_dirs_find_real_batch_dir_from_repo_root(monkeypatch):
    """The CWD probe must locate databricks/batch from the repo root."""
    monkeypatch.chdir(REPO_ROOT)
    dirs = ss._candidate_batch_dirs()
    batch_dir = os.path.normpath(str(REPO_ROOT / "databricks" / "batch"))
    assert batch_dir in [os.path.normpath(d) for d in dirs]
    # The located directory must actually contain bronze_to_silver.py.
    assert os.path.isfile(os.path.join(batch_dir, "bronze_to_silver.py"))


def test_candidate_batch_dirs_find_real_batch_dir_from_streaming_dir(monkeypatch):
    """The upward CWD walk must find databricks/batch from a nested folder."""
    monkeypatch.chdir(REPO_ROOT / "databricks" / "streaming")
    dirs = [os.path.normpath(d) for d in ss._candidate_batch_dirs()]
    batch_dir = os.path.normpath(str(REPO_ROOT / "databricks" / "batch"))
    assert batch_dir in dirs


def test_databricks_paths_are_deterministic_abfss():
    """All streaming paths on Databricks must be deterministic abfss://."""
    assert ss.get_incoming_path("databricks") == (
        "abfss://incoming@plantationsimulatorrg.dfs.core.windows.net/sensors"
    )
    assert ss.get_live_bronze_path("databricks") == (
        "abfss://live-bronze@plantationsimulatorrg.dfs.core.windows.net/sensors"
    )
    assert ss.get_live_silver_path("databricks") == (
        "abfss://live-silver@plantationsimulatorrg.dfs.core.windows.net/sensors"
    )
    assert ss.get_checkpoint_path("sensors_live_bronze", "databricks") == (
        "abfss://checkpoints@plantationsimulatorrg.dfs.core.windows.net/"
        "sensors_stream/sensors_live_bronze"
    )
    assert ss.get_checkpoint_path("sensors_live_silver", "databricks") == (
        "abfss://checkpoints@plantationsimulatorrg.dfs.core.windows.net/"
        "sensors_stream/sensors_live_silver"
    )


def test_local_paths_point_at_repo_data_dirs():
    """Local streaming paths must resolve inside the repo data/ directory."""
    assert ss.get_incoming_path("local").endswith(
        os.path.join("data", "raw", "sensors_stream")
    )
    assert ss.get_live_bronze_path("local").endswith(
        os.path.join("data", "live_bronze", "sensors")
    )
    assert ss.get_live_silver_path("local").endswith(
        os.path.join("data", "live_silver", "sensors")
    )
    assert ss.get_checkpoint_path("sensors_live_bronze", "local").endswith(
        os.path.join("data", "checkpoints", "sensors_stream", "sensors_live_bronze")
    )


def test_checkpoint_paths_are_deterministic_per_stream():
    """Checkpoint locations must be deterministic and distinct per stream."""
    bronze_cp = ss.get_checkpoint_path("sensors_live_bronze", "databricks")
    silver_cp = ss.get_checkpoint_path("sensors_live_silver", "databricks")
    assert bronze_cp != silver_cp
    assert "sensors_stream" in bronze_cp
    assert "sensors_stream" in silver_cp


def test_sensor_csv_schema_matches_generator_contract():
    """The streaming schema must match the generator's CSV header contract."""
    stream_fields = [f.name for f in ss.SENSOR_CSV_SCHEMA.fields]
    assert stream_fields == gen.SENSOR_CSV_HEADERS
    assert len(stream_fields) == 11
    # Key columns are present in the schema.
    for key in ss.SENSOR_KEY_COLUMNS:
        assert key in stream_fields


def test_sensor_key_columns_define_grain():
    """The live Silver grain is one reading per (sensor_id, timestamp)."""
    assert ss.SENSOR_KEY_COLUMNS == ["sensor_id", "timestamp"]


def _code_tokens_only(source: str) -> str:
    """Return source with comments and string/docstring literals removed."""
    out = []
    for tok in tokenize.generate_tokens(io.StringIO(source).readline):
        if tok.type in (tokenize.COMMENT, tokenize.STRING):
            continue
        out.append(tok.string)
    return " ".join(out)


def test_no_storage_key_sas_or_pat_in_streaming_module():
    """The streaming module must not configure a storage key/SAS/PAT."""
    code = _code_tokens_only(MODULE_PATH.read_text(encoding="utf-8"))
    assert "AZURE_STORAGE_ACCOUNT_KEY" not in code
    assert "fs.azure.account.key" not in code
    assert "SharedAccessSignature" not in code


def test_no_serverless_incompatible_persistence_calls():
    """Serverless (Spark Connect) rejects cache/persist/PERSIST TABLE and
    temp views. The streaming job must not use any of these."""
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
            f"Serverless-incompatible persistence call found: {forbidden}"
        )


def test_streaming_uses_available_now_trigger():
    """Streams must use availableNow=True (micro-batch, not continuous)."""
    raw = MODULE_PATH.read_text(encoding="utf-8")
    assert "availableNow=True" in raw


def test_streaming_uses_checkpoint_locations():
    """Both streams must configure checkpointLocation.

    Check the raw source (not tokenized code) because the option name is a
    string literal that _code_tokens_only would strip.
    """
    raw = MODULE_PATH.read_text(encoding="utf-8")
    assert '"checkpointLocation"' in raw
    assert "get_checkpoint_path" in raw


def test_streaming_reads_with_explicit_schema_no_inference():
    """Auto Loader must use the explicit SENSOR_CSV_SCHEMA, not inference."""
    code = _code_tokens_only(MODULE_PATH.read_text(encoding="utf-8"))
    assert "SENSOR_CSV_SCHEMA" in code
    # No cloudFiles schema inference option.
    assert "cloudFiles.inferColumnTypes" not in code


def test_streaming_is_decoupled_from_adf_gold_synapse():
    """The streaming path must not reference ADF, Gold, or Synapse."""
    code = _code_tokens_only(MODULE_PATH.read_text(encoding="utf-8")).lower()
    assert "datafactory" not in code
    assert "synapse" not in code
    # Gold container is not used.
    assert "abfss://gold" not in code


# ---------------------------------------------------------------------------
# Pure: generator Incoming-only contract
# ---------------------------------------------------------------------------


def test_generator_container_is_incoming(monkeypatch):
    """The simulator resolves to the incoming container by default."""
    monkeypatch.delenv("ADLS_INCOMING_CONTAINER", raising=False)
    container = gen.resolve_incoming_container()
    assert container == "incoming"
    assert gen._guard_incoming_only(container) == "incoming"


def test_generator_rejects_non_incoming_layers():
    """The simulator must refuse to write to any non-Incoming layer."""
    for forbidden in (
        "landing",
        "bronze",
        "silver",
        "gold",
        "live-bronze",
        "live-silver",
        "checkpoints",
    ):
        with pytest.raises(gen.IncomingOnlyViolation):
            gen._guard_incoming_only(forbidden)


def test_generator_csv_headers_match_streaming_schema():
    """Generator CSV headers must match the streaming schema field names."""
    assert gen.SENSOR_CSV_HEADERS == [f.name for f in ss.SENSOR_CSV_SCHEMA.fields]


def test_generator_produces_rows_for_all_sensors(tmp_path):
    """The generator produces one reading per sensor per interval."""
    config = gen.load_config("data_generators/config.yaml")
    import random

    random.seed(config.get("global_settings", {}).get("random_seed", 42))
    sensors_list = gen._build_sensor_roster(config)
    sensor_states = gen._init_sensor_states(config, sensors_list)
    from datetime import datetime, timezone

    rows = gen.generate_interval_readings(
        config, sensor_states, sensors_list,
        datetime(2024, 1, 1, 8, 0, 0, tzinfo=timezone.utc),
    )
    # 14 sensors (2 for BLK01/05/06/10, 1 for the other 6 blocks).
    assert len(sensors_list) == 14
    assert len(rows) == 14
    # Each row has exactly the header contract length.
    for row in rows:
        assert len(row) == len(gen.SENSOR_CSV_HEADERS)
    # CSV serialization round-trips.
    csv_bytes = gen._rows_to_csv_bytes(rows)
    assert csv_bytes.startswith(b"timestamp,block_id,sensor_id")


def test_generator_sensor_roster_matches_blocks():
    """The sensor roster covers all 10 blocks with 2 sensors for 4 of them."""
    config = gen.load_config("data_generators/config.yaml")
    sensors_list = gen._build_sensor_roster(config)
    block_ids = {s[0] for s in sensors_list}
    assert len(block_ids) == 10
    # Two-sensor blocks.
    two_sensor_blocks = {s[0] for s in sensors_list if s[1].endswith("-02")}
    assert two_sensor_blocks == {"BLK01", "BLK05", "BLK06", "BLK10"}
    # Sensor IDs are deterministic.
    sensor_ids = [s[1] for s in sensors_list]
    assert "SNS-BLK01-01" in sensor_ids
    assert "SNS-BLK10-02" in sensor_ids


# ---------------------------------------------------------------------------
# Transformation behaviour (local Spark; skipped when Java is unavailable)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def spark():
    if not JAVA_AVAILABLE:
        pytest.skip("local Spark requires a Java runtime (not present)")
    os.environ.setdefault("PIPELINE_ENV", "local")
    session = bts.get_spark_session(app_name="Plantation_Sensors_Tests", env="local")
    yield session
    session.stop()


@requires_spark
def test_transform_live_silver_casts_and_standardizes(spark):
    """Live Silver transform: casts types, standardizes IDs, dedupes, audits."""
    data = [
        # timestamp, block_id, sensor_id, soil_moisture_pct, soil_temperature_c,
        # air_temperature_c, humidity_pct, soil_ph, light_intensity_lux,
        # battery_level_pct, sensor_status
        (
            "2024-01-01 08:00:00",
            "blk01",
            "sns-blk01-01",
            "65.5",
            "25.3",
            "27.1",
            "80.2",
            "6.4",
            "50000.0",
            "95.1",
            "ok",
        ),
        # Duplicate business key -> deduplicated away.
        (
            "2024-01-01 08:00:00",
            "BLK01",
            "SNS-BLK01-01",
            "65.5",
            "25.3",
            "27.1",
            "80.2",
            "6.4",
            "50000.0",
            "95.1",
            "OK",
        ),
        # Blank measure -> NULL after cast.
        (
            "2024-01-01 08:15:00",
            "blk02",
            "sns-blk02-01",
            "",
            "24.9",
            "26.8",
            "81.0",
            "6.5",
            "51000.0",
            "94.8",
            "ok",
        ),
        # Missing key (sensor_id blank->NULL after trim? no: blank->NULL then filtered).
        (
            "2024-01-01 08:15:00",
            "blk02",
            "",
            "60.0",
            "24.9",
            "26.8",
            "81.0",
            "6.5",
            "51000.0",
            "94.8",
            "ok",
        ),
    ]
    df = spark.createDataFrame(data, ss.SENSOR_CSV_SCHEMA)
    out = ss.transform_live_silver(df)

    # 4 input rows -> 2 kept (1 duplicate removed, 1 missing key removed).
    assert out.count() == 2
    assert "_ingested_at" in out.columns
    # Timestamp cast.
    assert str(out.schema["timestamp"].dataType).startswith("TimestampType")
    # Numeric measures cast to double.
    assert str(out.schema["soil_moisture_pct"].dataType) == "DoubleType()"
    # IDs standardized.
    rows = {r["sensor_id"]: r for r in out.collect()}
    assert "SNS-BLK01-01" in rows
    assert "SNS-BLK02-01" in rows
    assert rows["SNS-BLK01-01"]["block_id"] == "BLK01"
    assert rows["SNS-BLK01-01"]["sensor_status"] == "OK"
    # Blank soil_moisture_pct became NULL.
    assert rows["SNS-BLK02-01"]["soil_moisture_pct"] is None


@requires_spark
def test_transform_live_silver_dedupes_on_business_key(spark):
    """Live Silver grain: one row per (sensor_id, timestamp)."""
    data = [
        (
            "2024-01-01 08:00:00",
            "BLK01",
            "SNS-BLK01-01",
            "65.5",
            "25.3",
            "27.1",
            "80.2",
            "6.4",
            "50000.0",
            "95.1",
            "OK",
        ),
        (
            "2024-01-01 08:00:00",
            "BLK01",
            "SNS-BLK01-01",
            "66.0",
            "25.4",
            "27.2",
            "80.5",
            "6.5",
            "50100.0",
            "95.0",
            "OK",
        ),
    ]
    df = spark.createDataFrame(data, ss.SENSOR_CSV_SCHEMA)
    out = ss.transform_live_silver(df)
    assert out.count() == 1
    assert out.dropDuplicates(ss.SENSOR_KEY_COLUMNS).count() == 1
