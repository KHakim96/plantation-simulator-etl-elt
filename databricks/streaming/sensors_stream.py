"""Phase 7: Databricks Structured Streaming — Live Sensor Path.

Reads live sensor CSV files arriving in the ADLS Gen2 **Incoming** container
via **Auto Loader** (cloudFiles incremental file discovery), processes them with
**Structured Streaming**, and writes two Delta layers on ADLS Gen2:

  * **Live Bronze** (``live-bronze`` container): raw-fidelity live sensor
    stream — the parsed CSV rows with minimal change (append-only).
  * **Live Silver** (``live-silver`` container): processed live sensor state —
    cleaned, standardized, deduplicated readings with the ``_ingested_at``
    audit column.

Streaming **checkpoints** live in the ADLS Gen2 ``checkpoints`` container
(deterministic locations per stream) for exactly-once / incremental recovery.
This path is intentionally decoupled from ADF, Gold, and Synapse
(ARCHITECTURE.md §5).

Execution environments (explicit — same pattern as Phase 3/4/5, no silent
fallback):
  * Databricks (Azure Databricks Serverless, Unity Catalog enabled):
      ADLS Gen2 Incoming/live-Bronze/live-Silver/checkpoint paths are used.
      Storage authentication is delegated to the Unity Catalog storage
      credential ``plantation_external_adls`` bound to the external locations.
      This script does NOT configure any storage account key, SAS token, PAT,
      or secret (``fs.azure.account.key.*`` is never set).
  * Local development (only when explicitly selected via
    ``PIPELINE_ENV=local``):
      reads CSVs from ``data/raw/sensors_stream`` and writes Delta to
      ``data/live_bronze`` / ``data/live_silver`` inside the repository, using
      ``delta-spark`` for the Delta writer. Checkpoints go to
      ``data/checkpoints``.

Serverless compatibility: no ``cache()``, ``persist()``, ``unpersist()``,
``saveAsTable``, ``createTempView``, or ``PERSIST TABLE`` is used. Streams use
``trigger(availableNow=True)`` so each run drains the currently available
micro-batches and stops — appropriate for a cost/trial-sensitive demo and for
Databricks Serverless (no always-on cluster).

Storage access logic is NOT duplicated here: environment detection and the
Spark session are imported from ``bronze_to_silver`` (Phase 3 helpers).
"""

from __future__ import annotations

# Reuse the Phase 3 environment/session helpers (same import pattern as Phase 4
# dq_checks.py and Phase 5 silver_to_gold.py — databricks/batch has no
# __init__.py). Two loaders are tried in order:
#   1. ``importlib.import_module`` — works on Databricks Serverless, where a
#      Git-backed repo file executed in the same folder has its directory on
#      ``sys.path``. It does NOT rely on ``__file__``, which Databricks does
#      not define.
#   2. A file-relative load keyed off ``__file__`` — the local pytest path,
#      where ``__file__`` is always defined. ``__file__`` is only evaluated
#      inside this branch, so it is never touched on Databricks.
import importlib
import importlib.util
import os
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)


def _load_bronze_to_silver():
    """Import bronze_to_silver without requiring ``__file__`` (Databricks-safe)."""
    try:
        return importlib.import_module("bronze_to_silver")
    except ImportError:
        pass
    this_file = globals().get("__file__")
    if this_file is None:
        raise ImportError(
            "Cannot import bronze_to_silver: it is not on sys.path and "
            "__file__ is unavailable in this execution environment. Ensure the "
            "script runs from a directory where bronze_to_silver.py is "
            "importable (Databricks Git-backed repo) or on sys.path."
        )
    # databricks/streaming -> databricks/batch/bronze_to_silver.py
    bts_path = os.path.join(
        os.path.dirname(os.path.abspath(this_file)),
        "..",
        "batch",
        "bronze_to_silver.py",
    )
    bts_path = os.path.normpath(bts_path)
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
is_databricks_environment = bts.is_databricks_environment
PipelineConfigError = bts.PipelineConfigError

# ==============================================================================
# 1. DETERMINISTIC ADLS COORDINATES & STREAM NAMES
# ==============================================================================

STORAGE_ACCOUNT = bts.STORAGE_ACCOUNT  # "plantationsimulatorrg"
INCOMING_CONTAINER = "incoming"
LIVE_BRONZE_CONTAINER = "live-bronze"
LIVE_SILVER_CONTAINER = "live-silver"
CHECKPOINT_CONTAINER = "checkpoints"

# Blob prefix inside Incoming where the live sensor simulator delivers files.
INCOMING_SENSORS_PREFIX = "sensors"

# Stream names (used for checkpoint paths and app naming).
BRONZE_STREAM_NAME = "sensors_live_bronze"
SILVER_STREAM_NAME = "sensors_live_silver"


# ==============================================================================
# 2. PATH RESOLUTION (ADLS Gen2 on Databricks, explicit local paths otherwise)
# ==============================================================================


def _repo_root() -> str:
    """Repository root (local dev only). Never used on Databricks."""
    this_file = globals().get("__file__")
    if this_file is None:  # pragma: no cover - Databricks path
        raise PipelineConfigError(
            "__file__ is unavailable; local path resolution requires a "
            "file-backed module (local development only)."
        )
    return os.path.abspath(os.path.join(os.path.dirname(this_file), "..", ".."))


def get_incoming_path(env: str) -> str:
    """Return the deterministic Incoming sensor path for Auto Loader."""
    if is_databricks_environment(env):
        return (
            f"abfss://{INCOMING_CONTAINER}@{STORAGE_ACCOUNT}"
            f".dfs.core.windows.net/{INCOMING_SENSORS_PREFIX}"
        )
    return os.path.join(_repo_root(), "data", "raw", "sensors_stream")


def get_live_bronze_path(env: str) -> str:
    """Return the deterministic live Bronze Delta path."""
    if is_databricks_environment(env):
        return (
            f"abfss://{LIVE_BRONZE_CONTAINER}@{STORAGE_ACCOUNT}"
            f".dfs.core.windows.net/sensors"
        )
    return os.path.join(_repo_root(), "data", "live_bronze", "sensors")


def get_live_silver_path(env: str) -> str:
    """Return the deterministic live Silver Delta path."""
    if is_databricks_environment(env):
        return (
            f"abfss://{LIVE_SILVER_CONTAINER}@{STORAGE_ACCOUNT}"
            f".dfs.core.windows.net/sensors"
        )
    return os.path.join(_repo_root(), "data", "live_silver", "sensors")


def get_checkpoint_path(stream_name: str, env: str) -> str:
    """Return the deterministic streaming checkpoint path for a stream."""
    if is_databricks_environment(env):
        return (
            f"abfss://{CHECKPOINT_CONTAINER}@{STORAGE_ACCOUNT}"
            f".dfs.core.windows.net/sensors_stream/{stream_name}"
        )
    return os.path.join(
        _repo_root(), "data", "checkpoints", "sensors_stream", stream_name
    )


# ==============================================================================
# 3. SENSOR CSV SCHEMA (from the Phase 1 / Phase 7 generator contract)
# ==============================================================================

# Explicit schema for the live sensor CSV files — must match the header
# contract emitted by data_generators/sensor_stream_to_adls.py and
# data_generators/generate_sensors.py. Auto Loader reads with this schema
# (header row, comma delimiter); no schema inference.
SENSOR_CSV_SCHEMA = StructType(
    [
        StructField("timestamp", StringType(), True),
        StructField("block_id", StringType(), True),
        StructField("sensor_id", StringType(), True),
        StructField("soil_moisture_pct", StringType(), True),
        StructField("soil_temperature_c", StringType(), True),
        StructField("air_temperature_c", StringType(), True),
        StructField("humidity_pct", StringType(), True),
        StructField("soil_ph", StringType(), True),
        StructField("light_intensity_lux", StringType(), True),
        StructField("battery_level_pct", StringType(), True),
        StructField("sensor_status", StringType(), True),
    ]
)

# Live Silver business key: one reading per (sensor_id, timestamp).
SENSOR_KEY_COLUMNS = ["sensor_id", "timestamp"]


# ==============================================================================
# 4. STREAMING READERS / TRANSFORMATIONS
# ==============================================================================


def read_incoming_stream(spark: SparkSession, env: str) -> DataFrame:
    """Build the Auto Loader streaming DataFrame over the Incoming sensor path.

    Auto Loader (cloudFiles) performs incremental file discovery: only newly
    arrived files are picked up on each trigger, and its state is tracked in the
    stream checkpoint. No schema inference is used — the explicit
    ``SENSOR_CSV_SCHEMA`` is applied.
    """
    incoming_path = get_incoming_path(env)
    print(f"Auto Loader source (Incoming): {incoming_path}")
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("delimiter", ",")
        .schema(SENSOR_CSV_SCHEMA)
        .load(incoming_path)
    )


def transform_live_silver(df: DataFrame) -> DataFrame:
    """Clean and standardize the live Bronze stream into live Silver.

    Transformations (mirroring the batch Bronze → Silver conventions):
      * cast ``timestamp`` to TimestampType
      * cast numeric measures to DoubleType (blank strings -> NULL)
      * uppercase/trim IDs and status (standardization)
      * drop rows missing the business key (``sensor_id``, ``timestamp``)
      * deduplicate on the business key
      * add the ``_ingested_at`` audit column
    """
    numeric_cols = [
        "soil_moisture_pct",
        "soil_temperature_c",
        "air_temperature_c",
        "humidity_pct",
        "soil_ph",
        "light_intensity_lux",
        "battery_level_pct",
    ]
    out = df
    for col_name in numeric_cols:
        out = out.withColumn(
            col_name,
            F.when(F.trim(F.col(col_name)) == "", None)
            .otherwise(F.col(col_name))
            .cast(DoubleType()),
        )
    out = (
        out.withColumn("timestamp", F.to_timestamp(F.col("timestamp")))
        .withColumn("block_id", F.upper(F.trim(F.col("block_id"))))
        .withColumn("sensor_id", F.upper(F.trim(F.col("sensor_id"))))
        .withColumn("sensor_status", F.upper(F.trim(F.col("sensor_status"))))
        .filter(F.col("sensor_id").isNotNull() & F.col("timestamp").isNotNull())
        .dropDuplicates(SENSOR_KEY_COLUMNS)
        .withColumn("_ingested_at", F.current_timestamp())
    )
    return out


# ==============================================================================
# 5. STREAM RUNNERS (checkpointed Delta sinks, availableNow trigger)
# ==============================================================================


def run_bronze_stream(spark: SparkSession, env: str) -> int:
    """Run the Incoming -> live Bronze streaming micro-batch.

    Returns the number of rows written to live Bronze in this run.
    """
    source_df = read_incoming_stream(spark, env)
    bronze_path = get_live_bronze_path(env)
    checkpoint = get_checkpoint_path(BRONZE_STREAM_NAME, env)

    print(f"Writing live Bronze Delta: {bronze_path}")
    print(f"Bronze checkpoint: {checkpoint}")

    query = (
        source_df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .start(bronze_path)
    )
    query.awaitTermination()

    # Row count written this run (post-stream verification).
    return spark.read.format("delta").load(bronze_path).count()


def run_silver_stream(spark: SparkSession, env: str) -> int:
    """Run the live Bronze -> live Silver streaming micro-batch.

    Reads the live Bronze Delta as a stream, applies the live Silver
    transformation, and writes to live Silver Delta. Returns the live Silver
    row count after this run.
    """
    bronze_path = get_live_bronze_path(env)
    silver_path = get_live_silver_path(env)
    checkpoint = get_checkpoint_path(SILVER_STREAM_NAME, env)

    print(f"Reading live Bronze Delta (stream): {bronze_path}")
    print(f"Writing live Silver Delta: {silver_path}")
    print(f"Silver checkpoint: {checkpoint}")

    bronze_stream = spark.readStream.format("delta").load(bronze_path)
    silver_df = transform_live_silver(bronze_stream)

    query = (
        silver_df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .start(silver_path)
    )
    query.awaitTermination()

    return spark.read.format("delta").load(silver_path).count()


def run_pipeline(spark: SparkSession, env: str) -> dict[str, int]:
    """Run the full live sensor path: Incoming -> live Bronze -> live Silver.

    Returns row counts for both layers after the run.
    """
    print("\n--- Stage 1: Incoming -> live Bronze ---")
    bronze_count = run_bronze_stream(spark, env)
    print(f"live Bronze rows after run: {bronze_count}")

    print("\n--- Stage 2: live Bronze -> live Silver ---")
    silver_count = run_silver_stream(spark, env)
    print(f"live Silver rows after run: {silver_count}")

    return {"live_bronze": bronze_count, "live_silver": silver_count}


def main() -> int:
    """Main streaming entry point for Databricks job or local execution."""
    env = detect_environment()
    print(f"Execution environment: {env}")
    if is_databricks_environment(env):
        print(
            "Storage auth: Unity Catalog external locations "
            "(credential 'plantation_external_adls'). No storage key/SAS/PAT."
        )

    spark = get_spark_session(app_name="Plantation_Sensors_Stream", env=env)
    try:
        results = run_pipeline(spark, env)
        print("\n========================================================")
        print("Live Sensor Streaming Finished Successfully")
        print("========================================================")
        print(f"  - live_bronze: {results['live_bronze']:>8d} rows")
        print(f"  - live_silver: {results['live_silver']:>8d} rows")
        return 0
    except Exception as exc:  # noqa: BLE001 - top-level entry point: fail loudly
        print("\n========================================================")
        print("Live Sensor Streaming FAILED")
        print("========================================================")
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
