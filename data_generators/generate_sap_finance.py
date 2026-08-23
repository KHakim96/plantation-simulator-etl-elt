"""
SAP Finance Transaction Data Generator for Smart Plantation Analytics.

Generates SAP-style financial posting documents (Debit/Credit double-entry pairs)
derived directly from upstream harvest, fertilizer, equipment, and HR operational activities (~250k rows).
"""

import csv
import math
import random
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml


def load_config(config_path: str = "data_generators/config.yaml") -> Dict[str, Any]:
    """Load and return master YAML configuration."""
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def validate_upstream_datasets(config: Dict[str, Any]) -> Dict[str, Path]:
    """
    Validate that all required upstream operational CSV datasets exist before processing.

    Raises FileNotFoundError with clear instructions if any required dataset is missing.
    """
    output_cfg = config.get("output_settings", {})
    paths = {
        "harvest": Path(output_cfg.get("output_paths", {}).get("harvest", "data/raw/harvest")) / "harvest_transactions.csv",
        "fertilizer": Path(output_cfg.get("output_paths", {}).get("fertilizer", "data/raw/fertilizer")) / "fertilizer_applications.csv",
        "equipment": Path(output_cfg.get("output_paths", {}).get("equipment", "data/raw/equipment")) / "equipment_logs.csv",
        "hr": Path(output_cfg.get("output_paths", {}).get("hr", "data/raw/hr")) / "hr_attendance.csv",
    }

    missing_files = [str(p) for name, p in paths.items() if not p.exists()]

    if missing_files:
        missing_str = "\n - ".join(missing_files)
        raise FileNotFoundError(
            f"Cannot generate SAP Finance transactions. The following required upstream datasets are missing:\n"
            f" - {missing_str}\n\n"
            "Please execute the upstream data generators first:\n"
            "  1. python3 data_generators/generate_harvest.py\n"
            "  2. python3 data_generators/generate_fertilizer.py\n"
            "  3. python3 data_generators/generate_equipment.py\n"
            "  4. python3 data_generators/generate_hr_attendance.py\n"
        )

    return paths


def generate_sap_finance_transactions(config: Dict[str, Any]) -> str:
    """
    Generate and stream SAP finance posting records directly to CSV matching target row count.

    Returns path to created output CSV file.
    """
    global_cfg = config.get("global_settings", {})
    output_cfg = config.get("output_settings", {})
    sizes_cfg = config.get("dataset_sizes", {})

    seed = global_cfg.get("random_seed", 42)
    random.seed(seed)

    target_rows = sizes_cfg.get("finance", 250000)

    out_dir = Path(output_cfg.get("output_paths", {}).get("finance", "data/raw/finance"))
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "sap_finance_transactions.csv"

    company_code = "MY10"
    currency = "MYR"

    # Reference maps from config
    cost_centers = config.get("cost_centers", [])
    blocks = config.get("blocks", [])
    block_region_map = {b["id"]: b.get("region_id", "REG-NORTH") for b in blocks}
    region_cc_map = {c.get("region_id", "REG-NORTH"): c["id"] for c in cost_centers}
    default_cc_id = cost_centers[0]["id"] if cost_centers else "CC101"

    materials_map = {m["id"]: m.get("standard_cost", 2.5) for m in config.get("materials", [])}

    headers = [
        "document_id",
        "posting_date",
        "posting_timestamp",
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
    ]

    operational_files = validate_upstream_datasets(config)
    rows_written = 0
    doc_counter = 1

    print(f"Generating ~{target_rows:,} SAP Finance documents into {out_file}...")

    with open(out_file, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)

        # Helper to output double-entry SAP document
        def write_doc_pair(
            ts_str: str,
            cc_id: str,
            debit_gl: str,
            credit_gl: str,
            tx_type: str,
            ref_doc: str,
            emp_id: str,
            eq_id: str,
            mat_id: str,
            amount: float,
            desc: str,
        ) -> int:
            nonlocal doc_counter
            if amount <= 0.0:
                return 0

            date_str = ts_str[:10]
            fiscal_year = date_str[:4]
            fiscal_period = f"{int(date_str[5:7]):02d}"
            doc_id = f"DOC-{doc_counter:09d}"
            doc_counter += 1

            amt_val = round(amount, 2)

            # Debit Line Item (S = Soll / Debit)
            debit_row = [
                doc_id,
                date_str,
                ts_str,
                fiscal_year,
                fiscal_period,
                company_code,
                cc_id,
                debit_gl,
                tx_type,
                ref_doc,
                emp_id,
                eq_id,
                mat_id,
                amt_val,
                currency,
                "S",
                f"Debit: {desc}",
            ]
            writer.writerow(debit_row)

            # Credit Line Item (H = Haben / Credit)
            credit_row = [
                doc_id,
                date_str,
                ts_str,
                fiscal_year,
                fiscal_period,
                company_code,
                cc_id,
                credit_gl,
                tx_type,
                ref_doc,
                emp_id,
                eq_id,
                mat_id,
                amt_val,
                currency,
                "H",
                f"Credit: {desc}",
            ]
            writer.writerow(credit_row)
            return 2

        # 1. Process Harvest Transactions -> Labor & Harvesting Costs
        print("Deriving finance postings from Harvest transactions...")
        with open(operational_files["harvest"], mode="r", encoding="utf-8") as hf:
            reader = csv.DictReader(hf)
            for row in reader:
                if rows_written >= target_rows:
                    break
                if row.get("status") == "CANCELLED":
                    continue

                ts_str = row["timestamp"]
                b_id = row.get("block_id", "")
                reg_id = block_region_map.get(b_id, "REG-NORTH")
                cc_id = region_cc_map.get(reg_id, default_cc_id)
                emp_id = row.get("employee_id", "")
                eq_id = row.get("equipment_id", "")
                weight_kg = float(row.get("harvested_weight_kg", 0.0))
                hvt_id = row.get("harvest_id", "")

                labor_amt = weight_kg * 0.08 + random.uniform(5.0, 20.0)
                written = write_doc_pair(
                    ts_str, cc_id, "500100", "200100", "HARVEST_LABOR",
                    hvt_id, emp_id, eq_id, "", labor_amt, f"Harvest Labor Batch {hvt_id}"
                )
                rows_written += written

        # 2. Process Fertilizer Applications -> Material Consumption Costs
        if rows_written < target_rows:
            print("Deriving finance postings from Fertilizer applications...")
            with open(operational_files["fertilizer"], mode="r", encoding="utf-8") as ff:
                reader = csv.DictReader(ff)
                for row in reader:
                    if rows_written >= target_rows:
                        break
                    if row.get("application_status") == "CANCELLED":
                        continue

                    ts_str = row["timestamp"]
                    b_id = row.get("block_id", "")
                    reg_id = block_region_map.get(b_id, "REG-NORTH")
                    cc_id = region_cc_map.get(reg_id, default_cc_id)
                    emp_id = row.get("employee_id", "")
                    eq_id = row.get("equipment_id", "")
                    mat_id = row.get("material_id", "")
                    qty_kg = float(row.get("quantity_kg", 0.0))
                    app_id = row.get("application_id", "")

                    std_cost = materials_map.get(mat_id, 2.50)
                    mat_amt = qty_kg * std_cost
                    written = write_doc_pair(
                        ts_str, cc_id, "501100", "140100", "FERTILIZER_CONSUMPTION",
                        app_id, emp_id, eq_id, mat_id, mat_amt, f"Fertilizer Consumption {app_id}"
                    )
                    rows_written += written

        # 3. Process Equipment Logs -> Fuel, Maintenance & Depreciation Costs
        if rows_written < target_rows:
            print("Deriving finance postings from Equipment logs...")
            with open(operational_files["equipment"], mode="r", encoding="utf-8") as ef:
                reader = csv.DictReader(ef)
                for row in reader:
                    if rows_written >= target_rows:
                        break

                    ts_str = row["timestamp"]
                    b_id = row.get("block_id", "")
                    reg_id = block_region_map.get(b_id, "REG-NORTH") if b_id else "REG-NORTH"
                    cc_id = "CC201"
                    op_id = row.get("operation_id", "")
                    eq_id = row.get("equipment_id", "")
                    emp_id = row.get("operator_id", "")
                    fuel_l = float(row.get("fuel_consumption_liters", 0.0))
                    maint_flag = row.get("maintenance_flag") == "True"

                    if fuel_l > 0.0:
                        fuel_amt = fuel_l * 1.20
                        written = write_doc_pair(
                            ts_str, cc_id, "502100", "140100", "EQUIPMENT_FUEL",
                            op_id, emp_id, eq_id, "MAT04", fuel_amt, f"Equipment Fuel {op_id}"
                        )
                        rows_written += written

                    if maint_flag and rows_written < target_rows:
                        maint_amt = random.uniform(250.0, 1800.0)
                        written = write_doc_pair(
                            ts_str, cc_id, "502200", "200100", "EQUIPMENT_MAINTENANCE",
                            op_id, emp_id, eq_id, "", maint_amt, f"Equipment Repair {op_id}"
                        )
                        rows_written += written

        # 4. Process HR Attendance -> Payroll & Overtime Expenses
        if rows_written < target_rows:
            print("Deriving finance postings from HR Attendance...")
            with open(operational_files["hr"], mode="r", encoding="utf-8") as hrf:
                reader = csv.DictReader(hrf)
                for row in reader:
                    if rows_written >= target_rows:
                        break
                    if row.get("attendance_status") not in ["PRESENT", "LATE"]:
                        continue

                    date_str = row["attendance_date"]
                    ts_str = f"{date_str} 17:00:00"
                    cc_id = row.get("cost_center_id", default_cc_id)
                    att_id = row.get("attendance_id", "")
                    emp_id = row.get("employee_id", "")
                    work_hrs = float(row.get("working_hours", 0.0))
                    ot_hrs = float(row.get("overtime_hours", 0.0))

                    payroll_amt = work_hrs * 12.50
                    written = write_doc_pair(
                        ts_str, cc_id, "503100", "200100", "PAYROLL_WAGES",
                        att_id, emp_id, "", "", payroll_amt, f"Daily Wage {att_id}"
                    )
                    rows_written += written

                    if ot_hrs > 0.0 and rows_written < target_rows:
                        ot_amt = ot_hrs * 18.75
                        written = write_doc_pair(
                            ts_str, cc_id, "503200", "200100", "OVERTIME_WAGES",
                            att_id, emp_id, "", "", ot_amt, f"Overtime Wages {att_id}"
                        )
                        rows_written += written

    print(f"Successfully generated {rows_written:,} SAP Finance documents at {out_file}.")
    return str(out_file)


if __name__ == "__main__":
    cfg = load_config()
    generate_sap_finance_transactions(cfg)
