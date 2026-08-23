"""
Equipment Utilization & Maintenance Data Generator for Smart Plantation Analytics.

Generates synthetic equipment operation and maintenance logs for 30 fleet assets,
tracking engine hours, fuel consumption, and weather impacts (~400k rows).
"""

import csv
import math
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

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


def map_operators_to_equipment(
    config: Dict[str, Any]
) -> Tuple[List[Dict[str, Any]], Dict[str, List[str]]]:
    """Extract equipment master and map compatible employee operator IDs by equipment type."""
    equipment = config.get("equipment_master", [])
    employees = config.get("employee_master", [])

    role_map = {
        "Tractor": ["Tractor Operator", "Driver", "Field Supervisor"],
        "Harvester": ["Tractor Operator", "Driver", "Senior Harvester"],
        "Sprayer": ["Fertilizer Applicator", "Tractor Operator", "Agronomist"],
        "Truck": ["Driver", "Tractor Operator"],
        "Excavator": ["Tractor Operator", "Driver", "Maintenance Technician"],
        "Drone": ["Agronomist", "Fertilizer Applicator", "Field Supervisor"],
        "Pickup Vehicle": ["Driver", "Field Supervisor", "Quality Inspector", "Agronomist"],
    }

    operator_by_type: Dict[str, List[str]] = {}
    for eq_type, roles in role_map.items():
        op_ids = [e["id"] for e in employees if e.get("role") in roles]
        if not op_ids:
            op_ids = [e["id"] for e in employees]
        operator_by_type[eq_type] = op_ids

    return equipment, operator_by_type


def generate_equipment_logs(config: Dict[str, Any]) -> str:
    """
    Generate and stream equipment operation and maintenance records directly to CSV.

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

    target_rows = sizes_cfg.get("equipment", 400000)

    out_dir = Path(output_cfg.get("output_paths", {}).get("equipment", "data/raw/equipment"))
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "equipment_logs.csv"

    blocks: List[Dict[str, Any]] = config.get("blocks", [])
    block_ids = [b["id"] for b in blocks]
    block_region_map = {b["id"]: b.get("region_id", "REG-NORTH") for b in blocks}

    equipment_list, operators_by_type = map_operators_to_equipment(config)
    weather_lookup, _ = load_weather_lookup(config)

    # Hourly fuel burn rate in liters/hour & average speed in km/h by equipment type
    fuel_burn_rates = {
        "Tractor": (8.0, 14.0),
        "Harvester": (12.0, 22.0),
        "Sprayer": (6.0, 12.0),
        "Truck": (10.0, 18.0),
        "Excavator": (11.0, 20.0),
        "Drone": (0.2, 0.8),
        "Pickup Vehicle": (5.0, 9.0),
    }

    avg_speeds_kmh = {
        "Tractor": 15.0,
        "Harvester": 8.0,
        "Sprayer": 12.0,
        "Truck": 45.0,
        "Excavator": 4.0,
        "Drone": 25.0,
        "Pickup Vehicle": 40.0,
    }

    headers = [
        "operation_id",
        "timestamp",
        "equipment_id",
        "equipment_type",
        "block_id",
        "operator_id",
        "operation_type",
        "start_time",
        "end_time",
        "duration_minutes",
        "engine_hours",
        "fuel_consumption_liters",
        "distance_km",
        "maintenance_flag",
        "maintenance_type",
        "status",
    ]

    total_days = (end_dt - start_dt).days + 1
    total_assets = len(equipment_list)
    total_steps = math.ceil(target_rows / total_assets)

    # Maintain lifetime state per asset (cumulative engine hours & current timestamp)
    asset_states = {}
    for eq in equipment_list:
        asset_states[eq["id"]] = {
            "engine_hours": round(random.uniform(100.0, 1500.0), 1),
            "next_maint_hours": random.uniform(150.0, 250.0),
            "last_dt": start_dt + timedelta(hours=random.randint(0, 5)),
        }

    rows_written = 0
    tx_counter = 1

    print(f"Generating ~{target_rows:,} equipment records into {out_file}...")

    with open(out_file, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)

        for step in range(total_steps):
            if rows_written >= target_rows:
                break

            for eq in equipment_list:
                if rows_written >= target_rows:
                    break

                eq_id = eq["id"]
                eq_type = eq["type"]
                st = asset_states[eq_id]

                # Advance timestamp to daytime window (07:00 to 18:00)
                curr_dt = st["last_dt"] + timedelta(minutes=random.choice([15, 30, 45, 60]))
                if curr_dt.hour < 7:
                    curr_dt = curr_dt.replace(hour=7, minute=0)
                elif curr_dt.hour >= 18:
                    curr_dt = (curr_dt + timedelta(days=1)).replace(hour=7, minute=0)

                if curr_dt > end_dt:
                    curr_dt = start_dt + timedelta(days=random.randint(0, total_days - 1))

                op_id = f"EQP-OP-{tx_counter:08d}"
                tx_counter += 1

                b_id = random.choice(block_ids)
                reg_id = block_region_map[b_id]
                ts_hour_str = curr_dt.strftime("%Y-%m-%d %H:00:00")

                weather_info = weather_lookup.get(
                    (ts_hour_str, reg_id),
                    {"rainfall_mm": 0.0, "weather_condition": "Clear"},
                )
                rainfall = weather_info["rainfall_mm"]
                w_cond = weather_info["weather_condition"]

                op_ids = operators_by_type.get(eq_type, [])
                operator_id = random.choice(op_ids) if op_ids else "EMP001"

                # Check if periodic or unscheduled maintenance is triggered
                is_maintenance = False
                maint_type = ""
                status = "COMPLETED"

                if st["engine_hours"] >= st["next_maint_hours"]:
                    is_maintenance = True
                    maint_type = random.choice(["Preventative Service", "Oil Change", "Hydraulic Inspection"])
                    op_type = "Scheduled Maintenance"
                    b_id = ""  # Workshop location
                    duration_mins = random.randint(120, 360)
                    st["next_maint_hours"] = st["engine_hours"] + random.uniform(180.0, 250.0)
                    status = "COMPLETED"
                elif random.random() < 0.02:  # Breakdown
                    is_maintenance = True
                    maint_type = "Unscheduled Repair"
                    op_type = "Emergency Repair"
                    duration_mins = random.randint(60, 240)
                    status = "BREAKDOWN"
                elif rainfall > 15.0 or w_cond in ["Heavy Rain", "Thunderstorm"]:
                    # Rain forces idling / stand-down
                    op_type = "Idle Stand-down"
                    duration_mins = random.randint(30, 120)
                    status = "IDLE"
                else:
                    op_type = random.choice(
                        ["Field Tillage", "Crop Hauling", "Pesticide Spraying", "Land Preparation", "Transport"]
                    )
                    duration_mins = random.randint(30, 240)
                    if random.random() < 0.05:
                        status = "DELAYED"

                start_ts_str = curr_dt.strftime("%Y-%m-%d %H:%M:%S")
                end_dt_val = curr_dt + timedelta(minutes=duration_mins)
                end_ts_str = end_dt_val.strftime("%Y-%m-%d %H:%M:%S")
                st["last_dt"] = end_dt_val

                # Calculate engine hours delta & fuel consumption
                run_hours = duration_mins / 60.0
                if status in ["COMPLETED", "DELAYED"]:
                    st["engine_hours"] += run_hours
                    burn_min, burn_max = fuel_burn_rates.get(eq_type, (6.0, 12.0))
                    fuel_consumed = round(run_hours * random.uniform(burn_min, burn_max), 2)
                    dist_km = round(run_hours * avg_speeds_kmh.get(eq_type, 15.0) * random.uniform(0.7, 1.1), 1)
                elif status == "IDLE":
                    st["engine_hours"] += run_hours * 0.2
                    fuel_consumed = round(run_hours * 1.5, 2)  # Low idle fuel burn
                    dist_km = 0.0
                else:  # Maintenance / Breakdown
                    fuel_consumed = 0.0
                    dist_km = 0.0

                current_engine_hrs = round(st["engine_hours"], 1)
                dist_str = str(dist_km) if eq_type not in ["Drone"] else ""

                row = [
                    op_id,
                    start_ts_str,
                    eq_id,
                    eq_type,
                    b_id,
                    operator_id,
                    op_type,
                    start_ts_str,
                    end_ts_str,
                    duration_mins,
                    current_engine_hrs,
                    fuel_consumed,
                    dist_str,
                    is_maintenance,
                    maint_type if is_maintenance else "",
                    status,
                ]

                writer.writerow(row)
                rows_written += 1

    print(f"Successfully generated {rows_written:,} equipment records at {out_file}.")
    return str(out_file)


if __name__ == "__main__":
    cfg = load_config()
    generate_equipment_logs(cfg)
