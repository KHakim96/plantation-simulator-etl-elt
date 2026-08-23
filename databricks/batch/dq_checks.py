"""Phase 4: Data Quality gate for the six Silver Delta datasets.

Validates the Silver layer produced by Phase 3 (``bronze_to_silver.py``) and
acts as a **gate** before any downstream processing (dbt / Gold): any CRITICAL
check failure makes this program exit with code 1 so orchestration (Phase 9)
can stop the pipeline. Non-critical issues are reported but do not block.

Checks implemented (per IMPLEMENTATION_PLAN.md Phase 4):
  1. schema
  2. nulls
  3. duplicates
  4. row counts
  5. freshness
  6. valid ranges
  7. Bronze/Silver reconciliation

Execution environments (explicit — same pattern as Phase 3, no silent fallback):
  * Databricks (Azure Databricks Serverless, Unity Catalog enabled):
      Silver/Bronze ADLS paths are used. Storage authentication is delegated to
      the Unity Catalog storage credential ``plantation_external_adls`` bound to
      the external locations. This script does NOT configure any storage account
      key, SAS token, PAT, or secret (``fs.azure.account.key.*`` is never set).
  * Local development (only when explicitly selected via ``PIPELINE_ENV=local``):
      reads Silver Delta from ``data/silver`` and Bronze CSVs from ``data/raw``.

Storage access logic is NOT duplicated here: environment detection, the Spark
session, and ADLS path resolution are imported from ``bronze_to_silver``.
"""

from __future__ import annotations

# Reuse the Phase 3 environment/path/session helpers and source ordering.
# databricks/batch has no __init__.py, so bronze_to_silver is imported as a
# top-level module. Two loaders are tried in order:
#   1. ``importlib.import_module`` — works on Databricks Serverless, where a
#      Git-backed repo file executed in the same folder has its directory on
#      ``sys.path`` (this is how Phase 3's bronze_to_silver.py itself runs).
#      It does NOT rely on ``__file__``, which Databricks does not define.
#   2. A file-relative load keyed off ``__file__`` — the local pytest path,
#      where ``__file__`` is always defined. ``__file__`` is only evaluated
#      inside this branch, so it is never touched on Databricks.
import importlib
import importlib.util
import os
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def _load_bronze_to_silver():
    """Import bronze_to_silver without requiring ``__file__`` (Databricks-safe)."""
    try:
        return importlib.import_module("bronze_to_silver")
    except ImportError:
        pass
    # Local fallback: resolve relative to this file. Guarded so ``__file__`` is
    # only referenced when it actually exists (never on Databricks).
    this_file = globals().get("__file__")
    if this_file is None:
        raise ImportError(
            "Cannot import bronze_to_silver: it is not on sys.path and "
            "__file__ is unavailable in this execution environment. Ensure the "
            "script runs from the same directory as bronze_to_silver.py "
            "(Databricks Git-backed folder) or on sys.path."
        )
    bts_path = os.path.join(os.path.dirname(os.path.abspath(this_file)),
                            "bronze_to_silver.py")
    spec = importlib.util.spec_from_file_location("bronze_to_silver", bts_path)
    if spec is None or spec.loader is None:  # pragma: no cover - import guard
        raise ImportError(f"Cannot load bronze_to_silver from {bts_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules["bronze_to_silver"] = module
    spec.loader.exec_module(module)
    return module


bts = _load_bronze_to_silver()

detect_environment = bts.detect_environment
get_spark_session = bts.get_spark_session
get_silver_path = bts.get_silver_path
get_bronze_path = bts.get_bronze_path
is_databricks_environment = bts.is_databricks_environment
SOURCE_ORDER = bts.SOURCE_ORDER

# ==============================================================================
# 1. DQ CONFIGURATION (derived from inspected Phase 1/2/3 ground truth)
# ==============================================================================

# Expected Silver row counts (Phase 3 verified: total 48,595).
EXPECTED_ROW_COUNTS: dict[str, int] = {
    "weather": 6483,
    "harvest": 9112,
    "fertilizer": 9000,
    "equipment": 10000,
    "hr": 2000,
    "finance": 12000,
}
EXPECTED_TOTAL_ROWS = sum(EXPECTED_ROW_COUNTS.values())  # 48595

# Primary/business key columns per source (dedup keys from Phase 3 transforms).
KEY_COLUMNS: dict[str, list[str]] = {
    "weather": ["station_id", "timestamp"],
    "harvest": ["harvest_id"],
    "fertilizer": ["application_id"],
    "equipment": ["operation_id"],
    "hr": ["attendance_id"],
    "finance": ["document_id", "debit_credit_indicator", "gl_account"],
}

# Required columns that must exist in each Silver dataset (schema check).
# Every Silver dataset also carries the Phase 3 `_ingested_at` audit column.
REQUIRED_COLUMNS: dict[str, list[str]] = {
    "weather": ["timestamp", "station_id", "region_id", "temperature_c",
                "humidity_pct", "rainfall_mm", "wind_speed_kmh",
                "weather_condition", "pressure_hpa", "_ingested_at"],
    "harvest": ["harvest_id", "timestamp", "block_id", "crop_type",
                "employee_id", "equipment_id", "harvested_weight_kg",
                "quality_grade", "moisture_pct", "collection_duration_minutes",
                "destination", "status", "_ingested_at"],
    "fertilizer": ["application_id", "timestamp", "block_id", "crop_type",
                   "employee_id", "material_id", "quantity_kg",
                   "application_method", "equipment_id", "weather_station_id",
                   "weather_condition", "rainfall_mm", "application_status",
                   "notes", "_ingested_at"],
    "equipment": ["operation_id", "timestamp", "equipment_id", "equipment_type",
                  "block_id", "operator_id", "operation_type", "start_time",
                  "end_time", "duration_minutes", "engine_hours",
                  "fuel_consumption_liters", "distance_km", "maintenance_flag",
                  "maintenance_type", "status", "_ingested_at"],
    "hr": ["attendance_id", "employee_id", "employee_name", "role",
           "department", "cost_center_id", "attendance_date", "shift",
           "check_in_time", "check_out_time", "working_hours",
           "overtime_hours", "attendance_status", "leave_type",
           "work_location", "remarks", "_ingested_at"],
    "finance": ["document_id", "posting_date", "posting_timestamp",
                "fiscal_year", "fiscal_period", "company_code",
                "cost_center_id", "gl_account", "transaction_type",
                "reference_document", "employee_id", "equipment_id",
                "material_id", "amount", "currency",
                "debit_credit_indicator", "description", "_ingested_at"],
}

# Valid-range rules: (column, min, max). Bounds are None for an open side.
# Non-critical: out-of-range values are reported, not blocking.
VALID_RANGES: dict[str, list[tuple[str, float | None, float | None]]] = {
    "weather": [
        ("temperature_c", -10.0, 50.0),
        ("humidity_pct", 0.0, 100.0),
        ("rainfall_mm", 0.0, None),
        ("wind_speed_kmh", 0.0, None),
        ("pressure_hpa", 900.0, 1100.0),
    ],
    "harvest": [
        ("harvested_weight_kg", 0.0, None),
        ("moisture_pct", 0.0, 100.0),
        ("collection_duration_minutes", 0.0, None),
    ],
    "fertilizer": [
        ("quantity_kg", 0.0, None),
        ("rainfall_mm", 0.0, None),
    ],
    "equipment": [
        ("duration_minutes", 0.0, None),
        ("engine_hours", 0.0, None),
        ("fuel_consumption_liters", 0.0, None),
        ("distance_km", 0.0, None),
    ],
    "hr": [
        ("working_hours", 0.0, 24.0),
        ("overtime_hours", 0.0, 24.0),
    ],
    "finance": [
        ("amount", 0.0, None),
        ("fiscal_period", 1.0, 16.0),
    ],
}

# Freshness: max allowed age of the newest _ingested_at, in days. Non-critical.
FRESHNESS_MAX_AGE_DAYS = 30

# Which checks are CRITICAL (block the pipeline on failure).
CRITICAL_CHECKS = frozenset(
    {"schema", "row_count", "duplicates", "nulls", "reconciliation"}
)
NON_CRITICAL_CHECKS = frozenset({"valid_ranges", "freshness"})


# ==============================================================================
# 2. DQ RESULT MODEL
# ==============================================================================


class CheckResult:
    """One DQ check outcome for one source."""

    def __init__(
        self,
        source: str,
        check: str,
        passed: bool,
        critical: bool,
        detail: str = "",
    ) -> None:
        self.source = source
        self.check = check
        self.passed = passed
        self.critical = critical
        self.detail = detail

    @property
    def status(self) -> str:
        return "PASS" if self.passed else "FAIL"

    @property
    def severity(self) -> str:
        return "CRITICAL" if self.critical else "NON-CRITICAL"

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return (
            f"CheckResult({self.source!r}, {self.check!r}, "
            f"{self.status}, {self.severity}, {self.detail!r})"
        )


def _is_critical(check: str) -> bool:
    return check in CRITICAL_CHECKS


# ==============================================================================
# 3. INDIVIDUAL CHECK IMPLEMENTATIONS (operate on an in-memory DataFrame)
# ==============================================================================


def check_schema(source: str, df: DataFrame) -> CheckResult:
    """CRITICAL: all required columns (incl. _ingested_at) must be present."""
    required = REQUIRED_COLUMNS[source]
    missing = [c for c in required if c not in df.columns]
    passed = not missing
    detail = "all required columns present" if passed else (
        f"missing columns: {missing}"
    )
    return CheckResult(source, "schema", passed, _is_critical("schema"), detail)


def check_nulls(source: str, df: DataFrame) -> CheckResult:
    """CRITICAL: key columns must contain no NULLs."""
    keys = [c for c in KEY_COLUMNS[source] if c in df.columns]
    null_counts: dict[str, int] = {}
    for col_name in keys:
        n = df.filter(F.col(col_name).isNull()).count()
        if n:
            null_counts[col_name] = n
    passed = not null_counts
    detail = "no nulls in key columns" if passed else (
        f"nulls in key columns: {null_counts}"
    )
    return CheckResult(source, "nulls", passed, _is_critical("nulls"), detail)


def check_duplicates(source: str, df: DataFrame) -> CheckResult:
    """CRITICAL: business key must be unique (0 duplicate groups)."""
    keys = [c for c in KEY_COLUMNS[source] if c in df.columns]
    total = df.count()
    distinct = df.select(*keys).distinct().count()
    dup_rows = total - distinct
    passed = dup_rows == 0
    detail = "0 duplicate key rows" if passed else (
        f"{dup_rows} duplicate key rows on {keys}"
    )
    return CheckResult(
        source, "duplicates", passed, _is_critical("duplicates"), detail
    )


def check_row_count(source: str, df: DataFrame) -> CheckResult:
    """CRITICAL: Silver row count must equal the expected count."""
    expected = EXPECTED_ROW_COUNTS[source]
    actual = df.count()
    passed = actual == expected
    detail = f"rows={actual} expected={expected}"
    return CheckResult(
        source, "row_count", passed, _is_critical("row_count"), detail
    )


def check_valid_ranges(source: str, df: DataFrame) -> CheckResult:
    """NON-CRITICAL: numeric measures must fall within plausible ranges."""
    violations: dict[str, int] = {}
    for col_name, low, high in VALID_RANGES[source]:
        if col_name not in df.columns:
            continue
        cond = F.col(col_name).isNotNull()
        out_of_range = F.lit(False)
        if low is not None:
            out_of_range = out_of_range | (F.col(col_name) < low)
        if high is not None:
            out_of_range = out_of_range | (F.col(col_name) > high)
        n = df.filter(cond & out_of_range).count()
        if n:
            violations[col_name] = n
    passed = not violations
    detail = "all measures within valid ranges" if passed else (
        f"out-of-range values: {violations}"
    )
    return CheckResult(
        source, "valid_ranges", passed, _is_critical("valid_ranges"), detail
    )


def check_freshness(source: str, df: DataFrame) -> CheckResult:
    """NON-CRITICAL: newest _ingested_at must be within the freshness window."""
    if "_ingested_at" not in df.columns:
        return CheckResult(
            source, "freshness", False, _is_critical("freshness"),
            "missing _ingested_at column",
        )
    row = df.agg(F.max("_ingested_at").alias("latest")).first()
    latest = row["latest"] if row else None
    if latest is None:
        return CheckResult(
            source, "freshness", False, _is_critical("freshness"),
            "no _ingested_at values",
        )
    age_days = df.agg(
        F.max(F.datediff(F.current_timestamp(), F.col("_ingested_at")))
    ).first()[0]
    passed = age_days is not None and age_days <= FRESHNESS_MAX_AGE_DAYS
    detail = (
        f"newest _ingested_at={latest} age_days={age_days} "
        f"max_allowed={FRESHNESS_MAX_AGE_DAYS}"
    )
    return CheckResult(
        source, "freshness", passed, _is_critical("freshness"), detail
    )


def check_reconciliation(
    source: str, silver_df: DataFrame, bronze_count: int
) -> CheckResult:
    """CRITICAL: Silver count must reconcile with Bronze post-dedup expectation.

    Bronze is the raw CSV input; Phase 3 dedups on the business key. The
    reconciliation target is the distinct Bronze key count, which must equal
    the Silver row count.
    """
    silver_count = silver_df.count()
    passed = silver_count == bronze_count
    detail = (
        f"silver={silver_count} bronze_distinct_keys={bronze_count} "
        f"{'reconciled' if passed else 'MISMATCH'}"
    )
    return CheckResult(
        source, "reconciliation", passed, _is_critical("reconciliation"),
        detail,
    )


# ==============================================================================
# 4. READERS (Silver Delta + Bronze CSV for reconciliation)
# ==============================================================================


def read_silver(spark: SparkSession, source: str, env: str) -> DataFrame:
    """Read a Silver Delta dataset from ADLS (Databricks) or local data."""
    path = get_silver_path(source, env)
    return spark.read.format("delta").load(path)


def read_bronze_distinct_key_count(
    spark: SparkSession, source: str, env: str
) -> int:
    """Distinct business-key count in the Bronze CSV (reconciliation target)."""
    path = get_bronze_path(source, env)
    schema = bts.BRONZE_SCHEMAS[source]
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("delimiter", ",")
        .schema(schema)
        .load(path)
    )
    # Phase 3 standardizes IDs to upper/trim before dedup; mirror that here so
    # the Bronze reconciliation key matches the Silver dedup key semantics.
    key_cols = [c for c in KEY_COLUMNS[source] if c in df.columns]
    normalized = df.select(
        *[
            F.upper(F.trim(F.col(c))).alias(c)
            if str(df.schema[c].dataType) == "StringType()"
            else F.col(c)
            for c in key_cols
        ]
    )
    return normalized.distinct().count()


# ==============================================================================
# 5. GATE RUNNER
# ==============================================================================


def run_dq_for_source(
    spark: SparkSession, source: str, env: str
) -> list[CheckResult]:
    """Run all 7 checks for one source and return their results."""
    silver_df = read_silver(spark, source, env)
    silver_df.cache()

    bronze_keys = read_bronze_distinct_key_count(spark, source, env)

    results = [
        check_schema(source, silver_df),
        check_nulls(source, silver_df),
        check_duplicates(source, silver_df),
        check_row_count(source, silver_df),
        check_valid_ranges(source, silver_df),
        check_freshness(source, silver_df),
        check_reconciliation(source, silver_df, bronze_keys),
    ]
    silver_df.unpersist()
    return results


def evaluate_overall(results: list[CheckResult]) -> bool:
    """Return True only if no CRITICAL check failed."""
    return not any(r.critical and not r.passed for r in results)


def print_report(results: list[CheckResult]) -> None:
    """Print a clear human-readable DQ report."""
    print("\n" + "=" * 78)
    print("DATA QUALITY REPORT — Silver layer")
    print("=" * 78)
    for r in results:
        line = (
            f"[{r.status:4s}] [{r.severity:12s}] "
            f"{r.source:11s} | {r.check:14s} | {r.detail}"
        )
        print(line)
    print("-" * 78)
    failed_critical = [r for r in results if r.critical and not r.passed]
    failed_noncritical = [r for r in results if not r.critical and not r.passed]
    total = len(results)
    passed = sum(1 for r in results if r.passed)
    print(f"Checks: {passed}/{total} passed")
    if failed_critical:
        print(f"CRITICAL FAILURES: {len(failed_critical)}")
        for r in failed_critical:
            print(f"  - {r.source}.{r.check}: {r.detail}")
        print("OVERALL RESULT: FAIL (downstream processing BLOCKED)")
    else:
        if failed_noncritical:
            print(
                f"Non-critical issues (not blocking): {len(failed_noncritical)}"
            )
            for r in failed_noncritical:
                print(f"  - {r.source}.{r.check}: {r.detail}")
        print("OVERALL RESULT: PASS")
    print("=" * 78)


def main() -> int:
    """Run the DQ gate. Exit 0 on PASS, 1 on any critical failure."""
    env = detect_environment()
    print(f"Execution environment: {env}")
    if is_databricks_environment(env):
        print(
            "Storage auth: Unity Catalog external locations "
            "(credential 'plantation_external_adls'). No storage key/SAS/PAT."
        )

    spark = get_spark_session(app_name="Plantation_DQ_Gate", env=env)
    try:
        results: list[CheckResult] = []
        for source in SOURCE_ORDER:
            results.extend(run_dq_for_source(spark, source, env))
        print_report(results)
        return 0 if evaluate_overall(results) else 1
    except Exception as exc:  # noqa: BLE001 - gate entry point: fail loudly
        print(f"\nDQ gate ERROR (treating as critical failure): {exc}",
              file=sys.stderr)
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
