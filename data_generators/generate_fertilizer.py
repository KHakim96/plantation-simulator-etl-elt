"""
Fertilizer Application Data Generator for Smart Plantation Analytics.

Generates synthetic fertilizer application transactions for 20 plantation blocks,
incorporating weather conditions from fetch_weather_api.py (~300k rows).
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
    """
    Load weather observations CSV into an hourly (timestamp_hour, region_id) lookup dictionary.

    Returns (weather_lookup, region_to_station_map).
    """
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


def filter_fertilizer_resources(
    config: Dict[str, Any]
) -> Tuple[List[str], List[str], Dict[str, List[str]]]:
    """Extract fertilizer material IDs, applicator employee IDs, and equipment IDs grouped by type."""
    materials = [
        m["id"]
        for m in config.get("materials", [])
        if m.get("category") == "Fertilizer" or m["id"] in ["MAT01", "MAT02", "MAT03"]
    ]

    employees = [
        e["id"]
        for e in config.get("employee_master", [])
        if e.get("role") in ["Fertilizer Applicator", "Agronomist", "Field Supervisor"]
    ]
    if not employees:
        employees = [e["id"] for e in config.get("employee_master", [])]

    equipment_by_type: Dict[str, List[str]] = {}
    for eq in config.get("equipment_master", []):
        eq_type = eq.get("type", "Other")
        equipment_by_type.setdefault(eq_type, []).append(eq["id"])

    return materials, employees, equipment_by_type


def generate_fertilizer_transactions(config: Dict[str, Any]) -> str:
    """
    Generate and stream fertilizer application records directly to CSV matching target row count.

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

    target_rows = sizes_cfg.get("fertilizer", 300000)

    out_dir = Path(output_cfg.get("output_paths", {}).get("fertilizer", "data/raw/fertilizer"))
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "fertilizer_applications.csv"

    blocks: List[Dict[str, Any]] = config.get("blocks", [])
    block_region_map = {b["id"]: b.get("region_id", "REG-NORTH") for b in blocks}

    fertilizer_mats, applicators, equipment_by_type = filter_fertilizer_resources(config)
    weather_lookup, region_to_station = load_weather_lookup(config)

    methods = [
        "Manual Broadcasting",
        "Tractor Boom Spraying",
        "Drone Aerial Spraying",
        "Soil Injection",
        "Foliar Spraying",
    ]

    method_eq_types = {
        "Drone Aerial Spraying": ["Drone"],
        "Tractor Boom Spraying": ["Tractor", "Sprayer"],
        "Soil Injection": ["Sprayer", "Tractor"],
        "Foliar Spraying": ["Sprayer", "Drone"],
    }

    headers = [
        "application_id",
        "timestamp",
        "block_id",
        "crop_type",
        "employee_id",
        "material_id",
        "quantity_kg",
        "application_method",
        "equipment_id",
        "weather_station_id",
        "weather_condition",
        "rainfall_mm",
        "application_status",
        "notes",
    ]

    total_days = (end_dt - start_dt).days + 1
    daily_batches = math.ceil(target_rows / total_days)

    rows_written = 0
    tx_counter = 1

    print(f"Generating ~{target_rows:,} fertilizer records into {out_file}...")

    with open(out_file, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)

        for day_offset in range(total_days):
            if rows_written >= target_rows:
                break

            current_date = start_dt + timedelta(days=day_offset)

            for _ in range(daily_batches):
                if rows_written >= target_rows:
                    break

                block = random.choice(blocks)
                b_id = block["id"]
                reg_id = block_region_map[b_id]
                crop = block["crop_type"]
                area_ha = block["area_ha"]

                hour = random.randint(7, 16)
                minute = random.choice([0, 15, 30, 45])
                ts = current_date.replace(hour=hour, minute=minute, second=0)
                ts_str = ts.strftime("%Y-%m-%d %H:%M:%S")
                ts_hour_str = current_date.replace(hour=hour, minute=0, second=0).strftime(
                    "%Y-%m-%d %H:00:00"
                )

                stn_id = region_to_station.get(reg_id, "STN-NORTH")
                weather_info = weather_lookup.get(
                    (ts_hour_str, reg_id),
                    {
                        "station_id": stn_id,
                        "rainfall_mm": 0.0,
                        "weather_condition": "Clear",
                    },
                )
                rainfall = weather_info["rainfall_mm"]
                w_cond = weather_info["weather_condition"]

                app_id = f"FERT-{tx_counter:08d}"
                tx_counter += 1

                emp_id = random.choice(applicators)
                mat_id = random.choice(fertilizer_mats)
                method = random.choice(methods)

                # Dynamic equipment selection based on application method and machinery type
                target_types = method_eq_types.get(method, [])
                candidate_eq = [
                    eq_id
                    for t in target_types
                    for eq_id in equipment_by_type.get(t, [])
                ]
                eq_id = random.choice(candidate_eq) if candidate_eq else ""

                # Calculate dosage in kg (scaled by block area: ~15 to 40 kg/ha)
                base_dosage = area_ha * random.uniform(15.0, 40.0)

                status = "COMPLETED"
                notes = "Routine fertilization application."

                if rainfall > 15.0 or w_cond in ["Heavy Rain", "Thunderstorm"]:
                    r_stat = random.random()
                    if r_stat < 0.50:
                        status = "CANCELLED"
                        base_dosage = 0.0
                        notes = f"Cancelled due to heavy rainfall ({rainfall:.1f}mm) and risk of nutrient run-off."
                    elif r_stat < 0.85:
                        status = "DELAYED"
                        base_dosage *= random.uniform(0.4, 0.7)
                        notes = f"Postponed / partial application due to rain ({w_cond})."
                    else:
                        status = "COMPLETED"
                        notes = "Completed despite light drizzle."
                elif rainfall > 3.0 or w_cond == "Light Rain":
                    if random.random() < 0.20:
                        status = "DELAYED"
                        notes = "Delayed due to damp soil conditions."

                dosage_kg = round(max(0.0, base_dosage), 2)

                row = [
                    app_id,
                    ts_str,
                    b_id,
                    crop,
                    emp_id,
                    mat_id,
                    dosage_kg,
                    method,
                    eq_id,
                    stn_id,
                    w_cond,
                    rainfall,
                    status,
                    notes,
                ]

                writer.writerow(row)
                rows_written += 1

    print(f"Successfully generated {rows_written:,} fertilizer records at {out_file}.")
    return str(out_file)


if __name__ == "__main__":
    cfg = load_config()
    generate_fertilizer_transactions(cfg)
