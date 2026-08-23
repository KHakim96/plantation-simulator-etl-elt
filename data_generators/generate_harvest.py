"""
Harvest Transaction Data Generator for Smart Plantation Analytics.

Generates synthetic harvest operation transactions for 20 plantation blocks,
scaling yield by block area and integrating weather observations from fetch_weather_api.py
to model rainfall impacts on harvesting (~500k rows).
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


def load_weather_lookup(config: Dict[str, Any]) -> Dict[Tuple[str, str], Dict[str, Any]]:
    """
    Load weather observations CSV into an hourly (timestamp_hour, region_id) lookup dictionary.

    If weather data does not exist, triggers mock generation via fetch_weather_api.py.
    """
    output_cfg = config.get("output_settings", {})
    weather_dir = Path(output_cfg.get("output_paths", {}).get("weather", "data/raw/weather"))
    weather_file = weather_dir / "weather_observations.csv"

    if not weather_file.exists():
        print(f"Weather observations not found at {weather_file}. Generating weather dataset...")
        generate_weather_records(config)

    lookup: Dict[Tuple[str, str], Dict[str, Any]] = {}
    with open(weather_file, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Key by hourly timestamp and region_id
            ts_str = row["timestamp"]
            reg_id = row["region_id"]
            lookup[(ts_str, reg_id)] = {
                "rainfall_mm": float(row["rainfall_mm"]) if row["rainfall_mm"] else 0.0,
                "weather_condition": row["weather_condition"],
            }
    return lookup


def filter_harvest_resources(
    employees: List[Dict[str, Any]], equipment: List[Dict[str, Any]]
) -> Tuple[List[str], List[str]]:
    """Filter employee harvesters and suitable harvest equipment IDs."""
    harvester_ids = [
        e["id"]
        for e in employees
        if e.get("role") in ["Harvester", "Senior Harvester", "Field Supervisor"]
    ]
    if not harvester_ids:
        harvester_ids = [e["id"] for e in employees]

    harvest_eq_ids = [
        eq["id"]
        for eq in equipment
        if eq.get("type") in ["Harvester", "Tractor", "Truck", "Pickup Vehicle"]
    ]
    return harvester_ids, harvest_eq_ids


def generate_harvest_transactions(config: Dict[str, Any]) -> str:
    """
    Generate and stream harvest records directly to CSV matching target row count.

    Incorporates weather-aware harvest probability, weight reductions, and status shifts.

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

    target_rows = sizes_cfg.get("harvest", 500000)

    out_dir = Path(output_cfg.get("output_paths", {}).get("harvest", "data/raw/harvest"))
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "harvest_transactions.csv"

    blocks: List[Dict[str, Any]] = config.get("blocks", [])
    block_region_map = {b["id"]: b.get("region_id", "REG-NORTH") for b in blocks}

    crop_list: List[Dict[str, Any]] = config.get("crop_types", [])
    crop_yield_map = {c["name"]: c.get("target_yield_per_ha_ton", 10.0) for c in crop_list}

    employees: List[Dict[str, Any]] = config.get("employee_master", [])
    equipment: List[Dict[str, Any]] = config.get("equipment_master", [])
    harvester_ids, harvest_eq_ids = filter_harvest_resources(employees, equipment)

    # Load hourly weather lookup
    weather_lookup = load_weather_lookup(config)

    destinations = [
        "Processing Mill A",
        "Processing Mill B",
        "Central Storage Silo",
        "Refinery Terminal",
        "Local Distribution Hub",
    ]

    headers = [
        "harvest_id",
        "timestamp",
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
    ]

    total_days = (end_dt - start_dt).days + 1
    daily_batches = math.ceil(target_rows / total_days)

    rows_written = 0
    tx_counter = 1

    print(f"Generating ~{target_rows:,} weather-aware harvest records into {out_file}...")

    with open(out_file, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)

        for day_offset in range(total_days):
            if rows_written >= target_rows:
                break

            current_date = start_dt + timedelta(days=day_offset)

            if current_date.weekday() == 6 and random.random() < 0.6:
                continue

            for _ in range(daily_batches):
                if rows_written >= target_rows:
                    break

                block = random.choice(blocks)
                b_id = block["id"]
                reg_id = block_region_map[b_id]
                crop = block["crop_type"]
                area_ha = block["area_ha"]
                target_yield_ton = crop_yield_map.get(crop, 10.0)

                hour = random.randint(7, 16)
                minute = random.choice([0, 15, 30, 45])
                ts = current_date.replace(hour=hour, minute=minute, second=0)
                ts_str = ts.strftime("%Y-%m-%d %H:%M:%S")
                ts_hour_str = current_date.replace(hour=hour, minute=0, second=0).strftime(
                    "%Y-%m-%d %H:00:00"
                )

                # Fetch weather for (hourly timestamp, region)
                weather_info = weather_lookup.get(
                    (ts_hour_str, reg_id),
                    {"rainfall_mm": 0.0, "weather_condition": "Clear"},
                )
                rainfall = weather_info["rainfall_mm"]
                w_cond = weather_info["weather_condition"]

                # Weather Impact Rule 1: Heavy rain suppresses harvest activity
                if rainfall > 15.0 or w_cond in ["Heavy Rain", "Thunderstorm"]:
                    if random.random() < 0.70:
                        continue  # Skip batch generation on heavy rain day

                harvest_id = f"HVT-{tx_counter:08d}"
                tx_counter += 1

                emp_id = random.choice(harvester_ids)

                if crop in ["Tea", "Coffee"] or random.random() < 0.35:
                    eq_id = ""
                else:
                    eq_id = random.choice(harvest_eq_ids)

                base_batch_kg = (area_ha * target_yield_ton * 1000) / 120.0
                weight_kg = base_batch_kg * random.uniform(0.6, 1.4)

                # Weather Impact Rule 2: Rain reduces harvested weight & shifts status
                status = "COMPLETED"
                if rainfall > 15.0 or w_cond in ["Heavy Rain", "Thunderstorm"]:
                    weight_kg *= random.uniform(0.3, 0.6)  # Reduced yield
                    r_status = random.random()
                    if r_status < 0.60:
                        status = "DELAYED"
                    elif r_status < 0.85:
                        status = "CANCELLED"
                        weight_kg = 0.0
                elif rainfall > 3.0 or w_cond == "Light Rain":
                    weight_kg *= random.uniform(0.8, 0.95)  # Slight yield drop
                    if random.random() < 0.15:
                        status = "DELAYED"

                weight_kg = round(max(0.0, weight_kg), 2)
                duration_mins = int(max(15, min(240, weight_kg / 15.0 + random.uniform(10, 30))))

                if status == "CANCELLED":
                    grade = "REJECT"
                    moisture = 0.0
                    dest = "N/A"
                else:
                    dest = random.choice(destinations)
                    # Higher rainfall increases moisture
                    moisture_bump = 5.0 if rainfall > 5.0 else 0.0

                    grade_roll = random.random()
                    if grade_roll < 0.60:
                        grade = "A"
                        moisture = round(random.uniform(12.0, 18.0) + moisture_bump * 0.3, 1)
                    elif grade_roll < 0.88:
                        grade = "B"
                        moisture = round(random.uniform(18.1, 24.0) + moisture_bump * 0.5, 1)
                    elif grade_roll < 0.97:
                        grade = "C"
                        moisture = round(random.uniform(24.1, 30.0) + moisture_bump, 1)
                    else:
                        grade = "REJECT"
                        moisture = round(random.uniform(30.1, 40.0), 1)

                row = [
                    harvest_id,
                    ts_str,
                    b_id,
                    crop,
                    emp_id,
                    eq_id,
                    weight_kg,
                    grade,
                    moisture,
                    duration_mins,
                    dest,
                    status,
                ]

                writer.writerow(row)
                rows_written += 1

    print(f"Successfully generated {rows_written:,} harvest records at {out_file}.")
    return str(out_file)


if __name__ == "__main__":
    cfg = load_config()
    generate_harvest_transactions(cfg)
