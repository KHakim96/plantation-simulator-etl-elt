"""
Phase 4 tests: Data Quality gate for the six Silver Delta datasets.

Two layers, mirroring tests/test_transformations.py:

  * Pure (no Spark session required): module loading, six-source coverage,
    expected row counts, critical/non-critical classification, Databricks
    Silver path resolution, and confirmation that the DQ module does NOT
    configure any Azure storage account key / SAS / PAT. These always run.

  * Gate behaviour (require a local Spark + Java runtime): individual check
    functions applied to small in-memory DataFrames, asserting good data
    passes and bad data (duplicates, wrong row count, null keys) fails, and
    that a critical failure drives the overall gate to FAIL (exit 1). These
    are skipped automatically when no Java runtime is available and do NOT
    touch ADLS.

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
MODULE_PATH = REPO_ROOT / "databricks" / "batch" / "dq_checks.py"
BTS_PATH = REPO_ROOT / "databricks" / "batch" / "bronze_to_silver.py"

SIX_SOURCES = ("weather", "harvest", "fertilizer", "equipment", "hr", "finance")


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
dq = _load_module(MODULE_PATH, "dq_checks")


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
# Pure: module loading, coverage, config, classification, paths, no-secrets
# ---------------------------------------------------------------------------


def test_module_loads_and_reuses_phase3_helpers():
    # dq_checks loads its own reference to bronze_to_silver (no __init__.py),
    # so the function objects are distinct instances of the SAME source. Assert
    # functional reuse: identical behavior and the same defining source file.
    assert dq.SOURCE_ORDER == bts.SOURCE_ORDER
    for name in (
        "detect_environment",
        "get_spark_session",
        "get_silver_path",
        "get_bronze_path",
    ):
        dq_fn = getattr(dq, name)
        bts_fn = getattr(bts, name)
        assert dq_fn.__code__.co_filename == bts_fn.__code__.co_filename
        assert dq_fn.__code__.co_firstlineno == bts_fn.__code__.co_firstlineno
    # Behavior parity on a representative path resolution.
    assert dq.get_silver_path("finance", "databricks") == bts.get_silver_path(
        "finance", "databricks"
    )


def test_six_source_coverage():
    assert tuple(dq.SOURCE_ORDER) == SIX_SOURCES
    for registry in (
        dq.EXPECTED_ROW_COUNTS,
        dq.KEY_COLUMNS,
        dq.REQUIRED_COLUMNS,
        dq.VALID_RANGES,
    ):
        assert set(registry) == set(SIX_SOURCES)


def test_expected_row_counts_and_total():
    assert dq.EXPECTED_ROW_COUNTS == {
        "weather": 6483,
        "harvest": 9112,
        "fertilizer": 9000,
        "equipment": 10000,
        "hr": 2000,
        "finance": 12000,
    }
    assert dq.EXPECTED_TOTAL_ROWS == 48595


def test_critical_noncritical_classification():
    assert dq.CRITICAL_CHECKS == {
        "schema",
        "row_count",
        "duplicates",
        "nulls",
        "reconciliation",
    }
    assert dq.NON_CRITICAL_CHECKS == {"valid_ranges", "freshness"}
    # The two sets are disjoint and cover all 7 checks.
    assert dq.CRITICAL_CHECKS.isdisjoint(dq.NON_CRITICAL_CHECKS)
    assert dq.CRITICAL_CHECKS | dq.NON_CRITICAL_CHECKS == {
        "schema",
        "nulls",
        "duplicates",
        "row_count",
        "freshness",
        "valid_ranges",
        "reconciliation",
    }


def test_databricks_silver_path_is_abfss():
    path = dq.get_silver_path("weather", "databricks")
    assert path == (
        "abfss://silver@plantationsimulatorrg.dfs.core.windows.net/weather"
    )
    for source in SIX_SOURCES:
        assert dq.get_silver_path(source, "databricks").startswith(
            "abfss://silver@plantationsimulatorrg.dfs.core.windows.net/"
        )


def _code_tokens_only(source: str) -> str:
    """Return source with comments and string/docstring literals removed."""
    out = []
    for tok in tokenize.generate_tokens(io.StringIO(source).readline):
        if tok.type in (tokenize.COMMENT, tokenize.STRING):
            continue
        out.append(tok.string)
    return " ".join(out)


def test_no_storage_key_sas_or_pat_in_dq_module():
    """The DQ module must not configure a storage key/SAS/PAT."""
    code = _code_tokens_only(MODULE_PATH.read_text(encoding="utf-8"))
    assert "AZURE_STORAGE_ACCOUNT_KEY" not in code
    assert "fs.azure.account.key" not in code
    assert "SharedAccessSignature" not in code


def test_no_serverless_incompatible_persistence_calls():
    """Serverless (Spark Connect) rejects DataFrame cache/persist as
    ``PERSIST TABLE``. The DQ gate must not use any persistence operation."""
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


def test_evaluate_overall_blocks_only_on_critical():
    # Critical failure -> overall FAIL
    bad = dq.CheckResult("weather", "row_count", False, True, "mismatch")
    assert dq.evaluate_overall([bad]) is False
    # Non-critical failure only -> overall PASS
    warn = dq.CheckResult("weather", "freshness", False, False, "stale")
    assert dq.evaluate_overall([warn]) is True
    # All pass -> PASS
    good = dq.CheckResult("weather", "row_count", True, True, "ok")
    assert dq.evaluate_overall([good]) is True


# ---------------------------------------------------------------------------
# Gate behaviour (local Spark; skipped when Java is unavailable)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def spark():
    if not JAVA_AVAILABLE:
        pytest.skip("local Spark requires a Java runtime (not present)")
    os.environ.setdefault("PIPELINE_ENV", "local")
    session = bts.get_spark_session(app_name="Plantation_DQ_Tests", env="local")
    yield session
    session.stop()


def _weather_df(spark, rows):
    return spark.createDataFrame(rows, bts.BRONZE_SCHEMAS["weather"])


def _good_weather_row(i=0):
    return (
        f"2024-01-01 0{i % 10}:00:00",  # timestamp (string; cast not required)
        "STN-NORTH",
        "REG-NORTH",
        26.0,
        85.0,
        0.0,
        5.0,
        "CLOUDY",
        1010.0,
    )


@requires_spark
def test_good_data_passes_core_checks(spark):
    # Build a small weather-like Silver frame with unique keys and no nulls.
    # Freshness/_ingested_at is not exercised here.
    rows = [_good_weather_row(i) for i in range(3)]
    df = _weather_df(spark, rows)
    assert dq.check_nulls("weather", df).passed is True
    assert dq.check_duplicates("weather", df).passed is True
    assert dq.check_valid_ranges("weather", df).passed is True


@requires_spark
def test_duplicate_keys_fail(spark):
    rows = [_good_weather_row(0), _good_weather_row(0), _good_weather_row(1)]
    df = _weather_df(spark, rows)
    result = dq.check_duplicates("weather", df)
    assert result.passed is False
    assert result.critical is True


@requires_spark
def test_wrong_row_count_fails(spark):
    rows = [_good_weather_row(i) for i in range(3)]
    df = _weather_df(spark, rows)
    result = dq.check_row_count("weather", df)  # expected 6483, actual 3
    assert result.passed is False
    assert result.critical is True
    assert "expected=6483" in result.detail


@requires_spark
def test_null_key_fails(spark):
    bad = (
        "2024-01-01 05:00:00",
        None,  # station_id NULL -> key null violation
        "REG-NORTH",
        26.0,
        85.0,
        0.0,
        5.0,
        "CLOUDY",
        1010.0,
    )
    df = _weather_df(spark, [_good_weather_row(0), bad])
    result = dq.check_nulls("weather", df)
    assert result.passed is False
    assert result.critical is True


@requires_spark
def test_critical_failure_blocks_gate(spark, monkeypatch):
    # Simulate a full-gate run where one critical check fails by feeding bad
    # data through the real check functions and the real evaluate_overall.
    rows = [_good_weather_row(0), _good_weather_row(0)]  # duplicate keys
    df = _weather_df(spark, rows)
    results = [
        dq.check_duplicates("weather", df),
        dq.check_nulls("weather", df),
    ]
    assert dq.evaluate_overall(results) is False  # critical dup -> BLOCK


@requires_spark
def test_main_raises_on_critical_failure(spark, monkeypatch):
    # Force run_dq_for_source to yield a critical failure. On Databricks
    # Serverless a returned exit-code int is ignored and any SystemExit fails
    # the task, so the DQ gate must signal a CRITICAL failure by RAISING a
    # non-SystemExit exception (this blocks the downstream silver_to_gold task).
    failing = dq.CheckResult("weather", "row_count", False, True, "forced")
    monkeypatch.setattr(
        dq, "run_dq_for_source", lambda s, src, env: [failing]
    )
    monkeypatch.setattr(dq, "get_spark_session", lambda **kw: spark)
    monkeypatch.setenv("PIPELINE_ENV", "local")
    # Prevent spark.stop() from killing the shared fixture session.
    monkeypatch.setattr(spark, "stop", lambda: None)
    with pytest.raises(RuntimeError):
        dq.main()


@requires_spark
def test_main_returns_0_when_all_pass(spark, monkeypatch):
    passing = dq.CheckResult("weather", "row_count", True, True, "ok")
    monkeypatch.setattr(
        dq, "run_dq_for_source", lambda s, src, env: [passing]
    )
    monkeypatch.setattr(dq, "get_spark_session", lambda **kw: spark)
    monkeypatch.setenv("PIPELINE_ENV", "local")
    monkeypatch.setattr(spark, "stop", lambda: None)
    assert dq.main() == 0
