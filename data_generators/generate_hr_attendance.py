"""
HR Attendance Data Generator for Smart Plantation Analytics.

Generates synthetic daily attendance, shift time clocking, leave, and overtime records
for 200 plantation employees across the 3-year generation period (~200k rows).
Enforces cross-dataset consistency with harvest and equipment operational logs.
"""

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import yaml

from data_generators.fetch_weather_api import generate_weather_records


def load_config(config_path: str = "data_generators/config.yaml") -> Dict[str, Any]:
    """Load and return master YAML configuration."""
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_weather_lookup(
    config: Dict[str, Any]
) -> Tuple[Dict[Tuple[str, str], Dict[str, Any]], Dict[str, str]]:
    """Load weather observations CSV into an hourly (timestamp_hour, region_id) lookup dictionary."""
    output_cfg = config.get("output_settings", {})
    weather_dir = Path(output_cfg.get("output_paths", {}).get("weather", "data/raw/weather"))
    weather_file = weather_dir / "weather_observations.csv"

    if not weather_file.exists():
        print(f"Weather observations not found at {weather_file}. Generating weather dataset...")
        generate_weather_records(config)

    stations = config.get("weather_stations", [])
    region_to_station = {s["region_id"]: s["station_id"] for s in stations}

    lookup: Dict[Tuple[str, str], Dict[str, Any]] = {}
    with open(weather_file, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ts_str = row["timestamp"]
            reg_id = row["region_id"]
            lookup[(ts_str, reg_id)] = {
                "station_id": row["station_id"],
                "rainfall_mm": float(row["rainfall_mm"]) if row["rainfall_mm"] else 0.0,
                "weather_condition": row["weather_condition"],
            }
    return lookup, region_to_station


def load_active_operational_workers(config: Dict[str, Any]) -> Set[Tuple[str, str]]:
    """
    Scan harvest and equipment datasets (if generated) to extract (date_str, employee_id)
    pairs for active operational workers.
    """
    output_cfg = config.get("output_settings", {})
    active_keys: Set[Tuple[str, str]] = set()

    # Scan harvest transactions
    hvt_dir = Path(output_cfg.get("output_paths", {}).get("harvest", "data/raw/harvest"))
    hvt_file = hvt_dir / "harvest_transactions.csv"
    if hvt_file.exists():
        with open(hvt_file, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("status") != "CANCELLED" and row.get("employee_id"):
                    date_str = row["timestamp"][:10]
                    active_keys.add((date_str, row["employee_id"]))

    # Scan equipment logs
    eqp_dir = Path(output_cfg.get("output_paths", {}).get("equipment", "data/raw/equipment"))
    eqp_file = eqp_dir / "equipment_logs.csv"
    if eqp_file.exists():
        with open(eqp_file, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("status") not in ["BREAKDOWN", "IDLE"] and row.get("operator_id"):
                    date_str = row["timestamp"][:10]
                    active_keys.add((date_str, row["operator_id"]))

    return active_keys


def get_malaysian_public_holidays(year: int) -> List[str]:
    """Return fixed Malaysian public holiday dates (YYYY-MM-DD) for a given year."""
    return [
        f"{year}-01-01",  # New Year's Day
        f"{year}-02-01",  # Federal Territory Day
        f"{year}-05-01",  # Labour Day
        f"{year}-08-31",  # National Day (Hari Merdeka)
        f"{year}-09-16",  # Malaysia Day
        f"{year}-12-25",  # Christmas Day
    ]


def generate_hr_attendance(config: Dict[str, Any]) -> str:
    """
    Generate and stream HR attendance records directly to CSV matching target row count.

    Enforces cross-dataset integrity so active operational workers are never marked ABSENT/LEAVE.

    Returns path to created output CSV file.
    """
    global_cfg = config.get("global_settings", {})
    output_cfg = config.get("output_settings", {})
    sizes_cfg = config.get("dataset_sizes", {})

    seed = global_cfg.get("random_seed", 42)
    random.seed(seed)

    start_dt = datetime.strptime(
        global_cfg.get("generation_period", {}).get("start_date", "2023-01-01"),
        "%Y-%m-%d",
    )
    end_dt = datetime.strptime(
        global_cfg.get("generation_period", {}).get("end_date", "2025-12-31"),
        "%Y-%m-%d",
    )

    target_rows = sizes_cfg.get("hr", 200000)

    out_dir = Path(output_cfg.get("output_paths", {}).get("hr", "data/raw/hr"))
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "hr_attendance.csv"

    employees: List[Dict[str, Any]] = config.get("employee_master", [])
    cost_centers: List[Dict[str, Any]] = config.get("cost_centers", [])
    cc_region_map = {c["id"]: c.get("region_id", "REG-NORTH") for c in cost_centers}

    weather_lookup, _ = load_weather_lookup(config)
    active_operational_keys = load_active_operational_workers(config)

    headers = [
        "attendance_id",
        "employee_id",
        "employee_name",
        "role",
        "department",
        "cost_center_id",
        "attendance_date",
        "shift",
        "check_in_time",
        "check_out_time",
        "working_hours",
        "overtime_hours",
        "attendance_status",
        "leave_type",
        "work_location",
        "remarks",
    ]

    total_days = (end_dt - start_dt).days + 1
    holidays_cache = {
        year: get_malaysian_public_holidays(year)
        for year in range(start_dt.year, end_dt.year + 1)
    }

    rows_written = 0
    att_counter = 1

    print(f"Generating ~{target_rows:,} HR attendance records into {out_file}...")

    with open(out_file, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)

        for day_offset in range(total_days):
            if rows_written >= target_rows:
                break

            current_date = start_dt + timedelta(days=day_offset)
            date_str = current_date.strftime("%Y-%m-%d")
            weekday = current_date.weekday()
            is_holiday = date_str in holidays_cache.get(current_date.year, [])

            for emp in employees:
                if rows_written >= target_rows:
                    break

                emp_id = emp["id"]
                emp_name = emp["name"]
                role = emp["role"]
                dept = emp["department"]
                cc_id = emp["cost_center_id"]
                reg_id = cc_region_map.get(cc_id, "REG-NORTH")

                ts_8am_str = f"{date_str} 08:00:00"
                weather_info = weather_lookup.get(
                    (ts_8am_str, reg_id),
                    {"rainfall_mm": 0.0, "weather_condition": "Clear"},
                )
                rainfall = weather_info["rainfall_mm"]
                w_cond = weather_info["weather_condition"]

                att_id = f"ATT-{att_counter:08d}"
                att_counter += 1

                shift = "Morning Shift" if role in ["Harvester", "Tractor Operator"] else "Day Shift"
                work_loc = f"Field - {reg_id.replace('REG-', '')}" if dept == "Field Operations" else "HQ Office"

                # Check if employee was active in harvest or equipment operations on this date
                is_active_operation = (date_str, emp_id) in active_operational_keys

                # Cross-dataset rule: Active workers MUST be PRESENT/LATE and NEVER ABSENT/LEAVE/HOLIDAY
                if is_active_operation:
                    status = "LATE" if (rainfall > 10.0 or w_cond == "Heavy Rain") else "PRESENT"
                elif is_holiday:
                    status = "PUBLIC_HOLIDAY"
                elif weekday == 6 and random.random() > 0.05:
                    continue
                elif weekday == 5 and role not in ["Harvester", "Tractor Operator", "Driver"] and random.random() > 0.15:
                    continue
                else:
                    status_roll = random.random()

                    if (rainfall > 15.0 or w_cond in ["Heavy Rain", "Thunderstorm"]) and dept == "Field Operations":
                        if status_roll < 0.65:
                            status = "PRESENT"
                        elif status_roll < 0.85:
                            status = "LATE"
                        elif status_roll < 0.95:
                            status = "ANNUAL_LEAVE"
                        else:
                            status = "ABSENT"
                    else:
                        if status_roll < 0.88:
                            status = "PRESENT"
                        elif status_roll < 0.94:
                            status = "LATE"
                        elif status_roll < 0.97:
                            status = "ANNUAL_LEAVE"
                        elif status_roll < 0.99:
                            status = "MEDICAL_LEAVE"
                        else:
                            status = "TRAINING"

                if status in ["PRESENT", "LATE"]:
                    leave_type = ""
                    if shift == "Morning Shift":
                        base_in = current_date.replace(hour=7, minute=0)
                        base_out = current_date.replace(hour=16, minute=0)
                    else:
                        base_in = current_date.replace(hour=8, minute=0)
                        base_out = current_date.replace(hour=17, minute=0)

                    if status == "LATE":
                        in_time = base_in + timedelta(minutes=random.randint(15, 75))
                        remarks = f"Late check-in due to weather ({w_cond})" if rainfall > 5.0 else "Late arrival"
                    else:
                        in_time = base_in + timedelta(minutes=random.randint(-15, 10))
                        remarks = "Public Holiday Overtime Shift" if (is_holiday and is_active_operation) else "Regular attendance"

                    ot_hrs = 0.0
                    if role in ["Harvester", "Tractor Operator", "Driver"] and random.random() < 0.35:
                        ot_mins = random.choice([60, 90, 120, 150, 180])
                        ot_hrs = round(ot_mins / 60.0, 1)
                        out_time = base_out + timedelta(minutes=ot_mins + random.randint(-10, 10))
                    else:
                        out_time = base_out + timedelta(minutes=random.randint(-10, 20))

                    check_in = in_time.strftime("%H:%M:%S")
                    check_out = out_time.strftime("%H:%M:%S")
                    duration_hours = (out_time - in_time).total_seconds() / 3600.0 - 1.0
                    work_hrs = round(max(4.0, duration_hours), 1)

                elif status == "PUBLIC_HOLIDAY":
                    leave_type = ""
                    check_in, check_out = "", ""
                    work_hrs, ot_hrs = 0.0, 0.0
                    remarks = "Gazetted Public Holiday"
                elif status in ["ANNUAL_LEAVE", "MEDICAL_LEAVE"]:
                    leave_type = "ANNUAL" if status == "ANNUAL_LEAVE" else "MEDICAL"
                    check_in, check_out = "", ""
                    work_hrs, ot_hrs = 0.0, 0.0
                    remarks = f"Approved {leave_type.lower()} leave"
                elif status == "TRAINING":
                    leave_type = ""
                    check_in = "08:30:00"
                    check_out = "16:30:00"
                    work_hrs, ot_hrs = 8.0, 0.0
                    remarks = "Attending Safety & Agronomy Training"
                else:  # ABSENT
                    leave_type = ""
                    check_in, check_out = "", ""
                    work_hrs, ot_hrs = 0.0, 0.0
                    remarks = "Unexcused absence"

                row = [
                    att_id,
                    emp_id,
                    emp_name,
                    role,
                    dept,
                    cc_id,
                    date_str,
                    shift,
                    check_in,
                    check_out,
                    work_hrs,
                    ot_hrs,
                    status,
                    leave_type,
                    work_loc,
                    remarks,
                ]

                writer.writerow(row)
                rows_written += 1

    print(f"Successfully generated {rows_written:,} HR attendance records at {out_file}.")
    return str(out_file)


if __name__ == "__main__":
    cfg = load_config()
    generate_hr_attendance(cfg)
