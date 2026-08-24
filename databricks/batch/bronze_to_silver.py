"""Phase 3: Databricks Spark Bronze → Silver Processing Job.

Reads 6 raw Bronze CSV datasets from ADLS Gen2 (or local data when explicitly
running in the local development environment), applies cleaning, validation,
deduplication, type casting, and standardization, and writes the cleaned output
as Delta Lake tables to the Silver container.

Execution environments (explicit — there is NO silent fallback to local files):
  * Databricks (Azure Databricks Serverless, Unity Catalog enabled):
      ADLS Gen2 Bronze/Silver paths are used. Storage authentication is
      provided by the Unity Catalog storage credential
      ``plantation_external_adls`` bound to the ``plantation_bronze`` and
      ``plantation_silver`` external locations. This script does NOT configure
      any storage account key, SAS token, PAT, or secret.
  * Local development (only when explicitly selected via ``PIPELINE_ENV=local``):
      reads CSVs from ``data/raw`` and writes Delta to ``data/silver`` inside
      the repository, using ``delta-spark`` for the Delta writer.

This script intentionally does NOT read ``AZURE_STORAGE_ACCOUNT_KEY`` and never
sets ``fs.azure.account.key.<account>.dfs.core.windows.net``.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Source name -> Raw Bronze CSV file name mapping (from Phase 1 & 2 contracts)
SOURCE_FILES: dict[str, str] = {
    "weather": "weather_observations.csv",
    "harvest": "harvest_transactions.csv",
    "fertilizer": "fertilizer_applications.csv",
    "equipment": "equipment_logs.csv",
    "hr": "hr_attendance.csv",
    "finance": "sap_finance_transactions.csv",
}

# Deterministic ADLS Gen2 coordinates for this environment.
STORAGE_ACCOUNT = "plantationsimulatorrg"
BRONZE_CONTAINER = "bronze"
SILVER_CONTAINER = "silver"

ENV_DATABRICKS = "databricks"
ENV_LOCAL = "local"

try:
    from delta import configure_spark_with_delta_pip
except ImportError:
    configure_spark_with_delta_pip = None


class PipelineConfigError(RuntimeError):
    """Raised when the execution environment or required path is misconfigured."""


# ==============================================================================
# 1. EXECUTION ENVIRONMENT & SPARK SESSION
# ==============================================================================


def detect_environment() -> str:
    """Return the execution environment: ``databricks`` or ``local``.

    Selection is explicit:
      * ``PIPELINE_ENV=databricks``  -> force Databricks/ADLS behaviour.
      * ``PIPELINE_ENV=local``       -> force local repository data paths.
      * unset                        -> auto-detect Databricks via the
        ``DATABRICKS_RUNTIME_VERSION`` env var; otherwise ``local``.

    Any other ``PIPELINE_ENV`` value is a configuration error (fail loudly).
    """
    requested = os.getenv("PIPELINE_ENV", "").strip().lower()
    if requested in (ENV_DATABRICKS, ENV_LOCAL):
        return requested
    if requested:
        raise PipelineConfigError(
            f"Invalid PIPELINE_ENV={requested!r}. "
            f"Expected {ENV_DATABRICKS!r} or {ENV_LOCAL!r}."
        )
    # Auto-detect: Databricks sets DATABRICKS_RUNTIME_VERSION on all runtimes,
    # including Serverless.
    if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        return ENV_DATABRICKS
    return ENV_LOCAL


def is_databricks_environment(env: str) -> bool:
    """True when running on Databricks (ADLS paths, Unity Catalog auth)."""
    return env == ENV_DATABRICKS


def get_spark_session(
    app_name: str = "Plantation_Bronze_To_Silver", env: str | None = None
) -> SparkSession:
    """Build or retrieve a SparkSession for the selected environment.

    No Azure storage account key, SAS token, PAT, or secret is configured here.
    On Databricks, ADLS access is delegated to Unity Catalog external locations
    backed by the ``plantation_external_adls`` storage credential.
    """
    env = env or detect_environment()

    builder = SparkSession.builder.appName(app_name)

    if is_databricks_environment(env):
        # Delta Lake is native on Databricks Runtime / Serverless; nothing to
        # configure. Unity Catalog governs ADLS access via external locations.
        return builder.getOrCreate()

    # Local development: enable Delta via delta-spark (Maven JARs).
    builder = builder.config(
        "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
    ).config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    if configure_spark_with_delta_pip is not None:
        return configure_spark_with_delta_pip(builder).getOrCreate()
    return builder.getOrCreate()


# ==============================================================================
# 2. PATH RESOLUTION (ADLS Gen2 on Databricks, explicit local paths otherwise)
# ==============================================================================


def _repo_root() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


def get_bronze_path(source_name: str, env: str) -> str:
    """Return the deterministic Bronze CSV path for a source."""
    file_name = SOURCE_FILES[source_name]
    if is_databricks_environment(env):
        return (
            f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}"
            f".dfs.core.windows.net/{source_name}/{file_name}"
        )
    return os.path.join(_repo_root(), "data", "raw", source_name, file_name)


def get_silver_path(source_name: str, env: str) -> str:
    """Return the deterministic Silver Delta path for a source."""
    if is_databricks_environment(env):
        return (
            f"abfss://{SILVER_CONTAINER}@{STORAGE_ACCOUNT}"
            f".dfs.core.windows.net/{source_name}"
        )
    return os.path.join(_repo_root(), "data", "silver", source_name)


def validate_local_bronze_path(path: str, source_name: str) -> None:
    """Fail loudly if a required local Bronze CSV is missing."""
    if not os.path.exists(path):
        raise PipelineConfigError(
            f"Local Bronze source '{source_name}' not found at expected path: {path}\n"
            "Run the Phase 1 generators first, or run this job on Databricks "
            "against ADLS Bronze."
        )


# ==============================================================================
# 3. EXPLICIT BRONZE CSV INGESTION SCHEMAS
# ==============================================================================
# Derived from inspected Phase 1 / Phase 2 raw data contracts.

BRONZE_SCHEMAS: dict[str, StructType] = {
    "weather": StructType(
        [
            StructField("timestamp", StringType(), True),
            StructField("station_id", StringType(), True),
            StructField("region_id", StringType(), True),
            StructField("temperature_c", DoubleType(), True),
            StructField("humidity_pct", DoubleType(), True),
            StructField("rainfall_mm", DoubleType(), True),
            StructField("wind_speed_kmh", DoubleType(), True),
            StructField("weather_condition", StringType(), True),
            StructField("pressure_hpa", DoubleType(), True),
        ]
    ),
    "harvest": StructType(
        [
            StructField("harvest_id", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("block_id", StringType(), True),
            StructField("crop_type", StringType(), True),
            StructField("employee_id", StringType(), True),
            StructField("equipment_id", StringType(), True),
            StructField("harvested_weight_kg", DoubleType(), True),
            StructField("quality_grade", StringType(), True),
            StructField("moisture_pct", DoubleType(), True),
            StructField("collection_duration_minutes", IntegerType(), True),
            StructField("destination", StringType(), True),
            StructField("status", StringType(), True),
        ]
    ),
    "fertilizer": StructType(
        [
            StructField("application_id", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("block_id", StringType(), True),
            StructField("crop_type", StringType(), True),
            StructField("employee_id", StringType(), True),
            StructField("material_id", StringType(), True),
            StructField("quantity_kg", DoubleType(), True),
            StructField("application_method", StringType(), True),
            StructField("equipment_id", StringType(), True),
            StructField("weather_station_id", StringType(), True),
            StructField("weather_condition", StringType(), True),
            StructField("rainfall_mm", DoubleType(), True),
            StructField("application_status", StringType(), True),
            StructField("notes", StringType(), True),
        ]
    ),
    "equipment": StructType(
        [
            StructField("operation_id", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("equipment_id", StringType(), True),
            StructField("equipment_type", StringType(), True),
            StructField("block_id", StringType(), True),
            StructField("operator_id", StringType(), True),
            StructField("operation_type", StringType(), True),
            StructField("start_time", StringType(), True),
            StructField("end_time", StringType(), True),
            StructField("duration_minutes", IntegerType(), True),
            StructField("engine_hours", DoubleType(), True),
            StructField("fuel_consumption_liters", DoubleType(), True),
            StructField("distance_km", DoubleType(), True),
            StructField("maintenance_flag", BooleanType(), True),
            StructField("maintenance_type", StringType(), True),
            StructField("status", StringType(), True),
        ]
    ),
    "hr": StructType(
        [
            StructField("attendance_id", StringType(), True),
            StructField("employee_id", StringType(), True),
            StructField("employee_name", StringType(), True),
            StructField("role", StringType(), True),
            StructField("department", StringType(), True),
            StructField("cost_center_id", StringType(), True),
            StructField("attendance_date", StringType(), True),
            StructField("shift", StringType(), True),
            StructField("check_in_time", StringType(), True),
            StructField("check_out_time", StringType(), True),
            StructField("working_hours", DoubleType(), True),
            StructField("overtime_hours", DoubleType(), True),
            StructField("attendance_status", StringType(), True),
            StructField("leave_type", StringType(), True),
            StructField("work_location", StringType(), True),
            StructField("remarks", StringType(), True),
        ]
    ),
    "finance": StructType(
        [
            StructField("document_id", StringType(), True),
            StructField("posting_date", StringType(), True),
            StructField("posting_timestamp", StringType(), True),
            StructField("fiscal_year", IntegerType(), True),
            StructField("fiscal_period", IntegerType(), True),
            StructField("company_code", StringType(), True),
            StructField("cost_center_id", StringType(), True),
            StructField("gl_account", StringType(), True),
            StructField("transaction_type", StringType(), True),
            StructField("reference_document", StringType(), True),
            StructField("employee_id", StringType(), True),
            StructField("equipment_id", StringType(), True),
            StructField("material_id", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("currency", StringType(), True),
            StructField("debit_credit_indicator", StringType(), True),
            StructField("description", StringType(), True),
        ]
    ),
}


# ==============================================================================
# 4. TRANSFORMATION & CLEANING HELPERS
# ==============================================================================


def nullify_blank_strings(df: DataFrame, columns: list[str]) -> DataFrame:
    """Replace empty strings or whitespace-only strings with NULL."""
    for col_name in columns:
        if col_name in df.columns:
            df = df.withColumn(
                col_name,
                F.when(F.trim(F.col(col_name)) == "", None).otherwise(
                    F.trim(F.col(col_name))
                ),
            )
    return df


def transform_weather(df: DataFrame) -> DataFrame:
    """Clean and standardize weather observations."""
    df_cleaned = (
        df.withColumn("timestamp", F.to_timestamp(F.col("timestamp")))
        .withColumn("station_id", F.upper(F.trim(F.col("station_id"))))
        .withColumn("region_id", F.upper(F.trim(F.col("region_id"))))
        .withColumn("weather_condition", F.upper(F.trim(F.col("weather_condition"))))
        .filter(F.col("station_id").isNotNull() & F.col("timestamp").isNotNull())
        .dropDuplicates(["station_id", "timestamp"])
        .withColumn("_ingested_at", F.current_timestamp())
    )
    return df_cleaned


def transform_harvest(df: DataFrame) -> DataFrame:
    """Clean, deduplicate, and standardize harvest transactions."""
    string_cols_to_nullify = ["equipment_id", "destination"]
    df_nullified = nullify_blank_strings(df, string_cols_to_nullify)

    df_cleaned = (
        df_nullified.withColumn("timestamp", F.to_timestamp(F.col("timestamp")))
        .withColumn("harvest_id", F.upper(F.trim(F.col("harvest_id"))))
        .withColumn("block_id", F.upper(F.trim(F.col("block_id"))))
        .withColumn("crop_type", F.upper(F.trim(F.col("crop_type"))))
        .withColumn("employee_id", F.upper(F.trim(F.col("employee_id"))))
        .withColumn("quality_grade", F.upper(F.trim(F.col("quality_grade"))))
        .withColumn("status", F.upper(F.trim(F.col("status"))))
        .filter(F.col("harvest_id").isNotNull() & F.col("timestamp").isNotNull())
        .dropDuplicates(["harvest_id"])
        .withColumn("_ingested_at", F.current_timestamp())
    )
    return df_cleaned


def transform_fertilizer(df: DataFrame) -> DataFrame:
    """Clean and standardize fertilizer applications."""
    string_cols_to_nullify = ["equipment_id", "notes", "weather_station_id"]
    df_nullified = nullify_blank_strings(df, string_cols_to_nullify)

    df_cleaned = (
        df_nullified.withColumn("timestamp", F.to_timestamp(F.col("timestamp")))
        .withColumn("application_id", F.upper(F.trim(F.col("application_id"))))
        .withColumn("block_id", F.upper(F.trim(F.col("block_id"))))
        .withColumn("crop_type", F.upper(F.trim(F.col("crop_type"))))
        .withColumn("material_id", F.upper(F.trim(F.col("material_id"))))
        .withColumn("employee_id", F.upper(F.trim(F.col("employee_id"))))
        .withColumn("application_method", F.upper(F.trim(F.col("application_method"))))
        .withColumn("application_status", F.upper(F.trim(F.col("application_status"))))
        .filter(F.col("application_id").isNotNull() & F.col("timestamp").isNotNull())
        .dropDuplicates(["application_id"])
        .withColumn("_ingested_at", F.current_timestamp())
    )
    return df_cleaned


def transform_equipment(df: DataFrame) -> DataFrame:
    """Clean and standardize equipment operational logs."""
    string_cols_to_nullify = ["block_id", "operator_id", "maintenance_type"]
    df_nullified = nullify_blank_strings(df, string_cols_to_nullify)

    df_cleaned = (
        df_nullified.withColumn("timestamp", F.to_timestamp(F.col("timestamp")))
        .withColumn("start_time", F.to_timestamp(F.col("start_time")))
        .withColumn("end_time", F.to_timestamp(F.col("end_time")))
        .withColumn("operation_id", F.upper(F.trim(F.col("operation_id"))))
        .withColumn("equipment_id", F.upper(F.trim(F.col("equipment_id"))))
        .withColumn("equipment_type", F.upper(F.trim(F.col("equipment_type"))))
        .withColumn("operation_type", F.upper(F.trim(F.col("operation_type"))))
        .withColumn("status", F.upper(F.trim(F.col("status"))))
        .filter(F.col("operation_id").isNotNull() & F.col("timestamp").isNotNull())
        .dropDuplicates(["operation_id"])
        .withColumn("_ingested_at", F.current_timestamp())
    )
    return df_cleaned


def transform_hr(df: DataFrame) -> DataFrame:
    """Clean and standardize HR daily attendance."""
    string_cols_to_nullify = [
        "leave_type",
        "remarks",
        "check_in_time",
        "check_out_time",
    ]
    df_nullified = nullify_blank_strings(df, string_cols_to_nullify)

    df_cleaned = (
        df_nullified.withColumn(
            "attendance_date", F.to_date(F.col("attendance_date"), "yyyy-MM-dd")
        )
        .withColumn("attendance_id", F.upper(F.trim(F.col("attendance_id"))))
        .withColumn("employee_id", F.upper(F.trim(F.col("employee_id"))))
        .withColumn("employee_name", F.trim(F.col("employee_name")))
        .withColumn("role", F.trim(F.col("role")))
        .withColumn("department", F.trim(F.col("department")))
        .withColumn("cost_center_id", F.upper(F.trim(F.col("cost_center_id"))))
        .withColumn("shift", F.upper(F.trim(F.col("shift"))))
        .withColumn("attendance_status", F.upper(F.trim(F.col("attendance_status"))))
        .filter(
            F.col("attendance_id").isNotNull() & F.col("attendance_date").isNotNull()
        )
        .dropDuplicates(["attendance_id"])
        .withColumn("_ingested_at", F.current_timestamp())
    )
    return df_cleaned


def transform_finance(df: DataFrame) -> DataFrame:
    """Clean and standardize SAP finance double-entry ledger transactions."""
    string_cols_to_nullify = [
        "reference_document",
        "employee_id",
        "equipment_id",
        "material_id",
    ]
    df_nullified = nullify_blank_strings(df, string_cols_to_nullify)

    df_cleaned = (
        df_nullified.withColumn(
            "posting_date", F.to_date(F.col("posting_date"), "yyyy-MM-dd")
        )
        .withColumn("posting_timestamp", F.to_timestamp(F.col("posting_timestamp")))
        .withColumn("document_id", F.upper(F.trim(F.col("document_id"))))
        .withColumn("company_code", F.upper(F.trim(F.col("company_code"))))
        .withColumn("cost_center_id", F.upper(F.trim(F.col("cost_center_id"))))
        .withColumn("gl_account", F.trim(F.col("gl_account")))
        .withColumn("transaction_type", F.upper(F.trim(F.col("transaction_type"))))
        .withColumn(
            "debit_credit_indicator", F.upper(F.trim(F.col("debit_credit_indicator")))
        )
        .withColumn("currency", F.upper(F.trim(F.col("currency"))))
        .withColumn("amount", F.col("amount").cast(DecimalType(18, 2)))
        .filter(F.col("document_id").isNotNull() & F.col("posting_date").isNotNull())
        .dropDuplicates(["document_id", "debit_credit_indicator", "gl_account"])
        .withColumn("_ingested_at", F.current_timestamp())
    )
    return df_cleaned


# Map source name -> transformation function
TRANSFORMATION_REGISTRY: dict[str, Callable[[DataFrame], DataFrame]] = {
    "weather": transform_weather,
    "harvest": transform_harvest,
    "fertilizer": transform_fertilizer,
    "equipment": transform_equipment,
    "hr": transform_hr,
    "finance": transform_finance,
}

# Processing order (kept stable/deterministic).
SOURCE_ORDER = (
    "weather",
    "harvest",
    "fertilizer",
    "equipment",
    "hr",
    "finance",
)


# ==============================================================================
# 5. BATCH PIPELINE RUNNER & DELTA WRITER
# ==============================================================================


def read_bronze_source(spark: SparkSession, source_name: str, env: str) -> DataFrame:
    """Read a Bronze CSV source file using the defined explicit schema."""
    if source_name not in SOURCE_FILES:
        raise ValueError(f"Unknown source: {source_name}")

    source_path = get_bronze_path(source_name, env)
    if not is_databricks_environment(env):
        validate_local_bronze_path(source_path, source_name)

    schema = BRONZE_SCHEMAS[source_name]

    print(f"Reading Bronze CSV: {source_path}")
    try:
        return (
            spark.read.format("csv")
            .option("header", "true")
            .option("delimiter", ",")
            .schema(schema)
            .load(source_path)
        )
    except Exception as exc:
        raise RuntimeError(
            f"Failed to read Bronze source '{source_name}' from {source_path}: {exc}"
        ) from exc


def write_silver_delta(
    df: DataFrame, source_name: str, env: str, mode: str = "overwrite"
) -> str:
    """Write cleaned DataFrame as a Delta Lake table in the Silver container."""
    target_path = get_silver_path(source_name, env)
    print(f"Writing Silver Delta: {target_path} (mode: {mode})")
    try:
        (
            df.write.format("delta")
            .mode(mode)
            .option("overwriteSchema", "true")
            .save(target_path)
        )
    except Exception as exc:
        raise RuntimeError(
            f"Failed to write Silver Delta for '{source_name}' to {target_path}: {exc}"
        ) from exc
    return target_path


def run_all_transformations(spark: SparkSession, env: str) -> dict[str, int]:
    """Execute Bronze → Silver transformation for all 6 sources.

    Fails loudly: any read/transform/write error aborts the run with a clear
    message identifying the source that failed.
    """
    results: dict[str, int] = {}

    for source_name in SOURCE_ORDER:
        print(f"\n--- Processing source: {source_name} ---")
        try:
            raw_df = read_bronze_source(spark, source_name, env)
            transform_func = TRANSFORMATION_REGISTRY[source_name]
            silver_df = transform_func(raw_df)
            write_silver_delta(silver_df, source_name, env)
            count = silver_df.count()
        except Exception as exc:
            raise RuntimeError(
                f"Bronze → Silver processing failed for source '{source_name}': {exc}"
            ) from exc

        results[source_name] = count
        print(f"Completed {source_name}: {count} rows in Silver Delta table")

    return results


def main() -> int:
    """Main batch entry point for Databricks job or local execution."""
    env = detect_environment()
    print(f"Execution environment: {env}")
    if is_databricks_environment(env):
        print(
            "Storage auth: Unity Catalog external locations "
            "(credential 'plantation_external_adls'). No storage key/SAS/PAT used."
        )

    spark = get_spark_session(env=env)
    try:
        results = run_all_transformations(spark, env)
        total = sum(results.values())
        print("\n========================================================")
        print("Bronze → Silver Processing Finished Successfully")
        print("========================================================")
        for src in SOURCE_ORDER:
            print(f"  - {src:12s}: {results[src]:>6d} rows")
        print("  ----------------------------------------")
        print(f"  - {'TOTAL':12s}: {total:>6d} rows")
        return 0
    except Exception as exc:
        print("\n========================================================")
        print("Bronze → Silver Processing FAILED")
        print("========================================================")
        print(f"ERROR: {exc}", file=sys.stderr)
        # Databricks Serverless python-task semantics (verified live): a
        # returned exit-code int is ignored, and ANY SystemExit (even code 0)
        # is surfaced as a task failure. Signal failure with a raised
        # non-SystemExit exception instead of returning 1.
        raise RuntimeError(f"Bronze → Silver processing failed: {exc}") from exc
    finally:
        spark.stop()


if __name__ == "__main__":
    # No sys.exit(): on Databricks Serverless a SystemExit (even code 0) fails
    # the task. Success = main() returns; failure = main() raises.
    main()
