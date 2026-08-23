# Single-cell Azure Databricks notebook: Phase 5 Gold verification.
# Paste into ONE Databricks notebook cell and run against the Gold container
# after silver_to_gold.py has completed successfully.
#
# This script is READ-ONLY. It does not modify Gold, Silver, or any other data.
# ruff: noqa: F821 - spark/display are Databricks notebook built-ins

from pyspark.sql import functions as F

BASE = "abfss://gold@plantationsimulatorrg.dfs.core.windows.net"

EXPECTED = {
    "dim_equipment": 30,
    "dim_employee": 24,
    "fact_harvest": 9112,
    "fact_revenue": 12000,
    "fact_fertilizer": 9000,
    "fact_equipment": 10000,
}

EXPECTED_TOTAL = sum(EXPECTED.values())  # 40166

KEY_COLUMNS = {
    "dim_equipment": ["equipment_id"],
    "dim_employee": ["employee_id"],
    "fact_harvest": ["harvest_id"],
    "fact_revenue": ["document_id", "debit_credit_indicator", "gl_account"],
    "fact_fertilizer": ["application_id"],
    "fact_equipment": ["operation_id"],
}

print("=" * 70)
print("PHASE 5 — GOLD VERIFICATION")
print("=" * 70)

grand_total = 0
all_pass = True

for model, expected in EXPECTED.items():
    path = f"{BASE}/{model}"
    keys = KEY_COLUMNS[model]

    try:
        df = spark.read.format("delta").load(path)
    except Exception as exc:  # noqa: BLE001 - read-only verification: report and continue
        print(f"\n{'─' * 70}")
        print(f"MODEL:  {model}")
        print(f"Path:   {path}")
        print(f"ERROR:  Cannot read — {exc}")
        print("RESULT: FAIL (path not readable)")
        all_pass = False
        continue

    actual = df.count()

    # Duplicate business keys
    if keys:
        dup_count = actual - df.select(*keys).distinct().count()
        null_count = df.filter(
            F.col(keys[0]).isNull()
        ).count()
        for k in keys[1:]:
            null_count += df.filter(F.col(k).isNull()).count()
    else:
        dup_count = 0
        null_count = 0

    has_ingested_at = "_ingested_at" in df.columns
    schema_ok = len(df.columns) > 0

    row_pass = actual == expected
    dup_pass = dup_count == 0
    null_pass = null_count == 0

    model_pass = row_pass and dup_pass and null_pass and schema_ok
    all_pass &= model_pass
    grand_total += actual

    print(f"\n{'─' * 70}")
    print(f"MODEL:  {model}")
    print(f"Path:   {path}")
    print(f"Rows:   {actual:,} / expected {expected:,} -> {'PASS' if row_pass else 'FAIL'}")
    print(f"Columns: {len(df.columns)} -> {'PASS' if schema_ok else 'FAIL'}")
    print(f"Duplicate keys: {dup_count:,} -> {'PASS' if dup_pass else 'FAIL'}")
    print(f"Null keys:      {null_count:,} -> {'PASS' if null_pass else 'FAIL'}")
    print(f"_ingested_at:   {'present' if has_ingested_at else 'absent (not required)'}")

    print("\nSchema:")
    df.printSchema()

    print("Sample rows:")
    display(df.limit(5))

print(f"\n{'=' * 70}")
print(f"TOTAL GOLD ROWS: {grand_total:,} / {EXPECTED_TOTAL:,}")
total_pass = grand_total == EXPECTED_TOTAL
print(f"FINAL RESULT: {'PASS — ALL GOLD DATASETS VERIFIED' if all_pass and total_pass else 'FAIL — REVIEW ABOVE'}")
print("=" * 70)
