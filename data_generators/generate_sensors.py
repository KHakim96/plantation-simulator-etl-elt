"""
IoT Sensor Data Generator for Smart Plantation Analytics.

Generates synthetic 15-minute telemetry for 20 plantation blocks,
streaming output directly to CSV to support high-volume generation (~2.5M rows).
"""

import csv
import math
import os
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Tuple

import yaml


def load_config(config_path: str = "data_generators/config.yaml") -> Dict[str, Any]:
    """Load and return master YAML configuration."""
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def calculate_diurnal_weather(dt: datetime) -> Tuple[float, float, float]:
    """
    Calculate diurnal air temperature (°C), humidity (%), and light intensity (lux)
    based on the time of day using sinusoidal models.
    """
    hour = dt.hour + dt.minute / 60.0

    # Air temperature peaks around 14:00 (~32°C) and drops around 05:00 (~23°C)
    temp_sin = math.sin((hour - 9) * math.pi / 12)
    air_temp = 27.5 + 4.5 * temp_sin + random.uniform(-0.5, 0.5)

    # Humidity is inversely related to temperature (~65% - 95%)
    humidity = 80.0 - 15.0 * temp_sin + random.uniform(-1.5, 1.5)
    humidity = max(20.0, min(100.0, humidity))

    # Light intensity: zero at night (19:00-06:00), peaks at midday (~100,000 lux)
    if 6.0 <= hour <= 19.0:
        sun_sin = math.sin((hour - 6.0) * math.pi / 13.0)
        light_lux = max(0.0, 100000.0 * sun_sin + random.uniform(-2000, 2000))
    else:
        light_lux = 0.0

    return round(air_temp, 2), round(humidity, 2), round(light_lux, 1)


def generate_sensor_readings(config: Dict[str, Any]) -> str:
    """
    Stream IoT sensor records to CSV file matching target row count (~2.5M rows).

    Returns path to created output CSV file.
    """
    # Parse configuration parameters
    global_cfg = config.get("global_settings", {})
    output_cfg = config.get("output_settings", {})
    sizes_cfg = config.get("dataset_sizes", {})
    gen_cfg = config.get("generator_settings", {})

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

    target_rows = sizes_cfg.get("sensors", 2500000)
    interval_mins = gen_cfg.get("sensor_interval_minutes", 15)

    anomaly_rate = gen_cfg.get("randomization", {}).get("anomaly_rate", 0.05)
    missing_rate = gen_cfg.get("randomization", {}).get("missing_data_rate", 0.01)

    blocks: List[Dict[str, Any]] = config.get("blocks", [])
    crop_types_list: List[Dict[str, Any]] = config.get("crop_types", [])
    crop_bounds = {c["name"]: c for c in crop_types_list}

    # Prepare output path
    out_dir = Path(
        output_cfg.get("output_paths", {}).get("sensors", "data/raw/sensors")
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "sensor_readings.csv"

    # Define sensors per block (24 total sensors across 20 blocks)
    sensors_list = []
    for block in blocks:
        b_id = block["id"]
        crop = block["crop_type"]
        # Give larger blocks or palm oil 2 sensors
        sensor_count = 2 if b_id in ["BLK01", "BLK05", "BLK06", "BLK10"] else 1
        for s_idx in range(1, sensor_count + 1):
            s_id = f"SNS-{b_id}-{s_idx:02d}"
            sensors_list.append((b_id, s_id, crop))

    total_sensors = len(sensors_list)
    # Determine steps needed to reach target_rows
    total_steps = math.ceil(target_rows / total_sensors)

    headers = [
        "timestamp",
        "block_id",
        "sensor_id",
        "soil_moisture_pct",
        "soil_temperature_c",
        "air_temperature_c",
        "humidity_pct",
        "soil_ph",
        "light_intensity_lux",
        "battery_level_pct",
        "sensor_status",
    ]

    # Track stateful variables per sensor (battery level & base moisture)
    sensor_states = {}
    for b_id, s_id, crop in sensors_list:
        cb = crop_bounds.get(crop, {})
        m_opt = cb.get("optimal_moisture_pct", [50, 80])
        t_opt = cb.get("optimal_temp_c", [20, 30])
        sensor_states[s_id] = {
            "battery": random.uniform(85.0, 100.0),
            "base_moisture": random.uniform(m_opt[0], m_opt[1]),
            "base_ph": random.uniform(6.0, 6.8),
            "temp_range": t_opt,
        }

    rows_written = 0
    curr_dt = start_dt

    print(f"Generating ~{target_rows:,} sensor records into {out_file}...")

    with open(out_file, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)

        for step in range(total_steps):
            if rows_written >= target_rows:
                break

            air_temp, humidity, light_lux = calculate_diurnal_weather(curr_dt)
            ts_str = curr_dt.strftime("%Y-%m-%d %H:%M:%S")

            for b_id, s_id, crop in sensors_list:
                if rows_written >= target_rows:
                    break

                st = sensor_states[s_id]
                # Battery discharges slowly (approx 0.0005% per interval)
                st["battery"] -= random.uniform(0.0001, 0.0008)
                if st["battery"] < 15.0:
                    st["battery"] = 100.0  # Battery replaced

                # Baseline soil metrics
                soil_moisture = st["base_moisture"] + random.uniform(-1.5, 1.5)
                soil_temp = air_temp - 2.0 + random.uniform(-0.8, 0.8)
                soil_ph = st["base_ph"] + random.uniform(-0.05, 0.05)
                batt_val = round(st["battery"], 1)
                status = "OK"

                # Check for anomaly injection
                if random.random() < anomaly_rate:
                    anomaly_type = random.choice(
                        ["HIGH_MOISTURE", "HIGH_TEMP", "SENSOR_FAULT"]
                    )
                    if anomaly_type == "HIGH_MOISTURE":
                        soil_moisture = random.uniform(91.0, 99.5)
                        status = "ANOMALY"
                    elif anomaly_type == "HIGH_TEMP":
                        soil_temp = random.uniform(46.0, 58.0)
                        status = "ANOMALY"
                    else:
                        status = "FAULT"

                # Format metric values
                moisture_str = str(round(max(0.0, min(100.0, soil_moisture)), 2))
                soil_temp_str = str(round(soil_temp, 2))
                air_temp_str = str(air_temp)
                humidity_str = str(humidity)
                ph_str = str(round(max(4.0, min(9.0, soil_ph)), 2))
                light_str = str(light_lux)

                # Check for missing data injection (1% missing attribute values)
                if random.random() < missing_rate:
                    attr_to_drop = random.choice(
                        [
                            "soil_moisture_pct",
                            "soil_ph",
                            "humidity_pct",
                            "soil_temperature_c",
                        ]
                    )
                    if attr_to_drop == "soil_moisture_pct":
                        moisture_str = ""
                    elif attr_to_drop == "soil_ph":
                        ph_str = ""
                    elif attr_to_drop == "humidity_pct":
                        humidity_str = ""
                    elif attr_to_drop == "soil_temperature_c":
                        soil_temp_str = ""

                row = [
                    ts_str,
                    b_id,
                    s_id,
                    moisture_str,
                    soil_temp_str,
                    air_temp_str,
                    humidity_str,
                    ph_str,
                    light_str,
                    batt_val,
                    status,
                ]

                writer.writerow(row)
                rows_written += 1

            curr_dt += timedelta(minutes=interval_mins)

    print(f"Successfully generated {rows_written:,} sensor records at {out_file}.")
    return str(out_file)


if __name__ == "__main__":
    cfg = load_config()
    generate_sensor_readings(cfg)
