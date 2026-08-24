"""Phase 5: Databricks Spark Silver → Gold Processing Job.

Reads the six DQ-verified Silver Delta datasets from ADLS Gen2 (or local data
when explicitly running in the local development environment), applies
business-level joins, aggregations, and dimension/fact modeling, and writes
analytics-ready Gold Delta datasets to the Gold container.

Execution environments (explicit — same pattern as Phase 3 and Phase 4, no
silent fallback):
  * Databricks (Azure Databricks Serverless, Unity Catalog enabled):
      Silver/Gold ADLS paths are used. Storage authentication is delegated to
      the Unity Catalog storage credential ``plantation_external_adls`` bound
      to the external locations. This script does NOT configure any storage
      account key, SAS token, PAT, or secret (``fs.azure.account.key.*`` is
      never set).
  * Local development (only when explicitly selected via
    ``PIPELINE_ENV=local``):
      reads Silver Delta from ``data/silver`` and writes Gold Delta to
      ``data/gold`` inside the repository.

Gold writes are idempotent: ``mode=overwrite`` with ``overwriteSchema=true``
(deterministic full refresh). Rerunning this job replaces each Gold dataset
rather than appending duplicates.

Storage access logic is NOT duplicated here: environment detection, the Spark
session, and ADLS path resolution are imported from ``bronze_to_silver``.
"""

from __future__ import annotations

# Reuse the Phase 3 environment/path/session helpers (same import pattern as
# Phase 4 dq_checks.py — databricks/batch has no __init__.py).
import importlib
import importlib.util
import os
import sys
from collections.abc import Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


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
is_databricks_environment = bts.is_databricks_environment

# ==============================================================================
# 1. GOLD MODEL REGISTRY
# ==============================================================================
# Each entry defines: source Silver dataset(s), grain (business key), and the
# transformation function. dim_plantation is intentionally excluded: no
# plantation/block master table exists in Silver — fabricating one from bare
# block_id values would invent data.

GOLD_CONTAINER = "gold"

GOLD_MODEL_ORDER = (
    "dim_equipment",
    "dim_employee",
    "fact_harvest",
    "fact_revenue",
    "fact_fertilizer",
    "fact_equipment",
)


def get_gold_path(model_name: str, env: str) -> str:
    """Return the deterministic Gold Delta path for a model."""
    if is_databricks_environment(env):
        return (
            f"abfss://{GOLD_CONTAINER}@{bts.STORAGE_ACCOUNT}"
            f".dfs.core.windows.net/{model_name}"
        )
    return os.path.join(bts._repo_root(), "data", "gold", model_name)


# ==============================================================================
# 2. DIMENSION TRANSFORMATIONS
# ==============================================================================


def build_dim_equipment(equipment_df: DataFrame) -> DataFrame:
    """Build the equipment dimension from the equipment Silver dataset.

    Grain: one row per equipment_id.
    Source: equipment (Silver).
    Columns: equipment_id, equipment_type.
    """
    return (
        equipment_df.select("equipment_id", "equipment_type")
        .filter(F.col("equipment_id").isNotNull())
        .dropDuplicates(["equipment_id"])
        .orderBy("equipment_id")
    )


def build_dim_employee(hr_df: DataFrame) -> DataFrame:
    """Build the employee dimension from the HR Silver dataset.

    Grain: one row per employee_id.
    Source: hr (Silver).
    Columns: employee_id, employee_name, role, department, cost_center_id.
    """
    return (
        hr_df.select(
            "employee_id", "employee_name", "role", "department",
            "cost_center_id",
        )
        .filter(F.col("employee_id").isNotNull())
        .dropDuplicates(["employee_id"])
        .orderBy("employee_id")
    )


# ==============================================================================
# 3. FACT TRANSFORMATIONS
# ==============================================================================


def build_fact_harvest(harvest_df: DataFrame) -> DataFrame:
    """Build the harvest fact table from the harvest Silver dataset.

    Grain: one row per harvest_id (a single harvest transaction).
    Source: harvest (Silver).
    Measures: harvested_weight_kg, moisture_pct, collection_duration_minutes.
    Dimensions: block_id, crop_type, employee_id, equipment_id, quality_grade,
                destination, status.
    """
    return (
        harvest_df.select(
            "harvest_id",
            F.to_date(F.col("timestamp")).alias("harvest_date"),
            F.col("timestamp").alias("harvest_timestamp"),
            "block_id",
            "crop_type",
            "employee_id",
            "equipment_id",
            "harvested_weight_kg",
            "quality_grade",
            "moisture_pct",
            "collection_duration_minutes",
            "destination",
            "status",
        )
        .filter(F.col("harvest_id").isNotNull())
        .dropDuplicates(["harvest_id"])
    )


def build_fact_revenue(finance_df: DataFrame) -> DataFrame:
    """Build the revenue/finance fact table from the finance Silver dataset.

    Grain: one row per (document_id, debit_credit_indicator, gl_account) —
    a single double-entry ledger line.
    Source: finance (Silver).
    Measures: amount (decimal 18,2).
    Dimensions: posting_date, fiscal_year, fiscal_period, company_code,
                cost_center_id, transaction_type, reference_document,
                employee_id, equipment_id, material_id, currency, description.
    """
    return (
        finance_df.select(
            "document_id",
            "posting_date",
            "fiscal_year",
            "fiscal_period",
            "company_code",
            "cost_center_id",
            "gl_account",
            "transaction_type",
            "reference_document",
            "employee_id",
            "equipment_id",
            "material_id",
            "amount",
            "currency",
            "debit_credit_indicator",
            "description",
        )
        .filter(F.col("document_id").isNotNull())
        .dropDuplicates(["document_id", "debit_credit_indicator", "gl_account"])
    )


def build_fact_fertilizer(fertilizer_df: DataFrame) -> DataFrame:
    """Build the fertilizer fact table from the fertilizer Silver dataset.

    Grain: one row per application_id (a single fertilizer application).
    Source: fertilizer (Silver).
    Measures: quantity_kg, rainfall_mm.
    Dimensions: block_id, crop_type, employee_id, material_id, equipment_id,
                weather_station_id, application_method, application_status.
    """
    return (
        fertilizer_df.select(
            "application_id",
            F.to_date(F.col("timestamp")).alias("application_date"),
            F.col("timestamp").alias("application_timestamp"),
            "block_id",
            "crop_type",
            "employee_id",
            "material_id",
            "quantity_kg",
            "application_method",
            "equipment_id",
            "weather_station_id",
            "rainfall_mm",
            "application_status",
        )
        .filter(F.col("application_id").isNotNull())
        .dropDuplicates(["application_id"])
    )


def build_fact_equipment(equipment_df: DataFrame) -> DataFrame:
    """Build the equipment operations fact table from the equipment Silver
    dataset.

    Grain: one row per operation_id (a single equipment operation log).
    Source: equipment (Silver).
    Measures: duration_minutes, engine_hours, fuel_consumption_liters,
              distance_km.
    Dimensions: equipment_id, equipment_type, block_id, operator_id,
                operation_type, maintenance_flag, maintenance_type, status.
    """
    return (
        equipment_df.select(
            "operation_id",
            F.to_date(F.col("timestamp")).alias("operation_date"),
            F.col("timestamp").alias("operation_timestamp"),
            "equipment_id",
            "equipment_type",
            "block_id",
            "operator_id",
            "operation_type",
            "duration_minutes",
            "engine_hours",
            "fuel_consumption_liters",
            "distance_km",
            "maintenance_flag",
            "maintenance_type",
            "status",
        )
        .filter(F.col("operation_id").isNotNull())
        .dropDuplicates(["operation_id"])
    )


# Map Gold model name -> (source Silver name, transformation function).
# Dimensions that need a Silver source read specify it; facts use their own
# name as the source.
GOLD_MODEL_REGISTRY: dict[str, tuple[str, Callable[[DataFrame], DataFrame]]] = {
    "dim_equipment": ("equipment", build_dim_equipment),
    "dim_employee": ("hr", build_dim_employee),
    "fact_harvest": ("harvest", build_fact_harvest),
    "fact_revenue": ("finance", build_fact_revenue),
    "fact_fertilizer": ("fertilizer", build_fact_fertilizer),
    "fact_equipment": ("equipment", build_fact_equipment),
}

# Business key columns per Gold model (for duplicate validation).
GOLD_KEY_COLUMNS: dict[str, list[str]] = {
    "dim_equipment": ["equipment_id"],
    "dim_employee": ["employee_id"],
    "fact_harvest": ["harvest_id"],
    "fact_revenue": ["document_id", "debit_credit_indicator", "gl_account"],
    "fact_fertilizer": ["application_id"],
    "fact_equipment": ["operation_id"],
}


# ==============================================================================
# 4. READERS & WRITERS
# ==============================================================================


def read_silver(spark: SparkSession, source: str, env: str) -> DataFrame:
    """Read a Silver Delta dataset from ADLS (Databricks) or local data."""
    path = get_silver_path(source, env)
    print(f"Reading Silver Delta: {path}")
    try:
        return spark.read.format("delta").load(path)
    except Exception as exc:
        raise RuntimeError(
            f"Failed to read Silver source '{source}' from {path}: {exc}"
        ) from exc


def write_gold_delta(
    df: DataFrame, model_name: str, env: str, mode: str = "overwrite"
) -> str:
    """Write a Gold DataFrame as a Delta Lake table in the Gold container."""
    target_path = get_gold_path(model_name, env)
    print(f"Writing Gold Delta: {target_path} (mode: {mode})")
    try:
        (
            df.write.format("delta")
            .mode(mode)
            .option("overwriteSchema", "true")
            .save(target_path)
        )
    except Exception as exc:
        raise RuntimeError(
            f"Failed to write Gold Delta for '{model_name}' to {target_path}: "
            f"{exc}"
        ) from exc
    return target_path


# ==============================================================================
# 5. BATCH PIPELINE RUNNER
# ==============================================================================


def run_all_gold_transformations(
    spark: SparkSession, env: str
) -> dict[str, int]:
    """Execute Silver → Gold transformation for all Gold models.

    Fails loudly: any read/transform/write error aborts the run with a clear
    message identifying the model that failed.
    """
    results: dict[str, int] = {}

    for model_name in GOLD_MODEL_ORDER:
        print(f"\n--- Processing Gold model: {model_name} ---")
        try:
            source_name, transform_func = GOLD_MODEL_REGISTRY[model_name]
            silver_df = read_silver(spark, source_name, env)
            gold_df = transform_func(silver_df)
            write_gold_delta(gold_df, model_name, env)
            count = gold_df.count()
        except Exception as exc:
            raise RuntimeError(
                f"Silver → Gold processing failed for model '{model_name}': "
                f"{exc}"
            ) from exc

        results[model_name] = count
        print(f"Completed {model_name}: {count} rows in Gold Delta table")

    return results


def main() -> int:
    """Main batch entry point for Databricks job or local execution."""
    env = detect_environment()
    print(f"Execution environment: {env}")
    if is_databricks_environment(env):
        print(
            "Storage auth: Unity Catalog external locations "
            "(credential 'plantation_external_adls'). No storage key/SAS/PAT."
        )

    spark = get_spark_session(app_name="Plantation_Silver_To_Gold", env=env)
    try:
        results = run_all_gold_transformations(spark, env)
        total = sum(results.values())
        print("\n========================================================")
        print("Silver → Gold Processing Finished Successfully")
        print("========================================================")
        for model in GOLD_MODEL_ORDER:
            print(f"  - {model:20s}: {results[model]:>6d} rows")
        print("  ----------------------------------------")
        print(f"  - {'TOTAL':20s}: {total:>6d} rows")
        return 0
    except Exception as exc:  # noqa: BLE001 - top-level batch entry point
        print("\n========================================================")
        print("Silver → Gold Processing FAILED")
        print("========================================================")
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
