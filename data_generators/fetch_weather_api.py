"""
Weather Data Generator & OpenWeather API Client for Smart Plantation Analytics.

Supports live API retrieval from OpenWeatherMap API or fallback mock generation
creating hourly weather records for all configured plantation weather stations.
"""

import csv
import json
import math
import os
import random
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml


def load_config(config_path: str = "data_generators/config.yaml") -> Dict[str, Any]:
    """Load and return master YAML configuration."""
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def fetch_live_weather(
    station: Dict[str, Any], api_endpoint: str, api_key: str
) -> Optional[Dict[str, Any]]:
    """
    Fetch current live weather data from OpenWeatherMap API.

    Returns dictionary formatted as a weather record or None on failure.
    """
    lat, lon = station.get("latitude"), station.get("longitude")
    url = f"{api_endpoint}?lat={lat}&lon={lon}&appid={api_key}&units=metric"

    try:
        req = urllib.request.Request(url, headers={"User-Agent": "PlantationAnalytics/1.0"})
        with urllib.request.urlopen(req, timeout=5) as response:
            data = json.loads(response.read().decode("utf-8"))

            rain_3h = data.get("rain", {}).get("1h", data.get("rain", {}).get("3h", 0.0))
            return {
                "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                "station_id": station["station_id"],
                "region_id": station["region_id"],
                "temperature_c": round(data["main"]["temp"], 1),
                "humidity_pct": round(data["main"]["humidity"], 1),
                "rainfall_mm": round(rain_3h, 2),
                "wind_speed_kmh": round(data["wind"]["speed"] * 3.6, 1),
                "weather_condition": data["weather"][0]["main"],
                "pressure_hpa": round(data["main"]["pressure"], 1),
            }
    except Exception as err:
        print(f"Warning: Live API fetch failed for {station['station_id']}: {err}")
        return None


def calculate_hourly_weather_state(
    dt: datetime, prev_state: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Compute smooth hourly weather transitions for Malaysian tropical climate.
    """
    month = dt.month
    hour = dt.hour

    # Malaysian monsoon influence (Nov-Mar NE monsoon & May-Sep SW monsoon)
    is_monsoon = month in [11, 12, 1, 2, 3, 5, 6, 7, 8, 9]
    rain_chance = 0.35 if is_monsoon else 0.15

    # Afternoon convective rain spike (14:00 - 18:00)
    if 14 <= hour <= 18:
        rain_chance += 0.25

    # Diurnal baseline temperature (°C)
    diurnal_sin = math.sin((hour - 9) * math.pi / 12)
    target_temp = 27.5 + 4.5 * diurnal_sin

    # Smooth transition from previous state
    curr_temp = prev_state["temp"] * 0.7 + target_temp * 0.3 + random.uniform(-0.4, 0.4)
    curr_temp = max(21.0, min(36.0, curr_temp))

    # Humidity moves inversely to temperature (70% - 98%)
    target_hum = 85.0 - 15.0 * diurnal_sin
    curr_hum = prev_state["hum"] * 0.7 + target_hum * 0.3 + random.uniform(-1.0, 1.0)
    curr_hum = max(60.0, min(99.0, curr_hum))

    # Pressure (1008 - 1014 hPa)
    curr_press = prev_state["press"] * 0.8 + 1011.0 * 0.2 + random.uniform(-0.3, 0.3)
    curr_press = max(1004.0, min(1018.0, curr_press))

    # Wind speed (2 - 25 km/h)
    curr_wind = prev_state["wind"] * 0.7 + random.uniform(3.0, 12.0) * 0.3
    curr_wind = max(1.0, min(35.0, curr_wind))

    # Determine rainfall and condition
    if random.random() < rain_chance:
        if random.random() < 0.20:
            condition = "Thunderstorm"
            rainfall = random.uniform(15.0, 45.0)
            curr_wind += random.uniform(10.0, 20.0)
            curr_hum = min(99.0, curr_hum + 10.0)
        elif random.random() < 0.50:
            condition = "Heavy Rain"
            rainfall = random.uniform(5.0, 15.0)
            curr_hum = min(98.0, curr_hum + 8.0)
        else:
            condition = "Light Rain"
            rainfall = random.uniform(0.5, 5.0)
    else:
        rainfall = 0.0
        if curr_hum > 85.0 and diurnal_sin < 0:
            condition = "Cloudy"
        elif diurnal_sin > 0.5:
            condition = "Clear"
        else:
            condition = "Partly Cloudy"

    # Update state dictionary
    prev_state["temp"] = curr_temp
    prev_state["hum"] = curr_hum
    prev_state["press"] = curr_press
    prev_state["wind"] = curr_wind

    return {
        "temperature_c": round(curr_temp, 1),
        "humidity_pct": round(curr_hum, 1),
        "rainfall_mm": round(rainfall, 2),
        "wind_speed_kmh": round(curr_wind, 1),
        "weather_condition": condition,
        "pressure_hpa": round(curr_press, 1),
    }


def generate_weather_records(config: Dict[str, Any]) -> str:
    """
    Generate hourly weather records for all weather stations in config.

    Supports live API execution if OPENWEATHER_API_KEY is defined or mock fallback.
    """
    global_cfg = config.get("global_settings", {})
    output_cfg = config.get("output_settings", {})
    gen_cfg = config.get("generator_settings", {})
    stations: List[Dict[str, Any]] = config.get("weather_stations", [])

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

    out_dir = Path(output_cfg.get("output_paths", {}).get("weather", "data/raw/weather"))
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "weather_observations.csv"

    headers = [
        "timestamp",
        "station_id",
        "region_id",
        "temperature_c",
        "humidity_pct",
        "rainfall_mm",
        "wind_speed_kmh",
        "weather_condition",
        "pressure_hpa",
    ]

    weather_api_cfg = gen_cfg.get("weather_api", {})
    api_key_var = weather_api_cfg.get("api_key_env_var", "OPENWEATHER_API_KEY")
    api_key = os.getenv(api_key_var)
    api_endpoint = weather_api_cfg.get("endpoint", "")

    # Live Mode Execution (Single snapshot if API key present)
    if api_key and not weather_api_cfg.get("fallback_to_mock", False):
        print("Executing Live Weather API Mode...")
        records = []
        for station in stations:
            rec = fetch_live_weather(station, api_endpoint, api_key)
            if rec:
                records.append(rec)

        if records:
            with open(out_file, mode="w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writeheader()
                writer.writerows(records)
            print(f"Live mode fetch completed: {len(records)} records saved to {out_file}.")
            return str(out_file)

    # Mock Mode Execution (Full historical hourly simulation)
    print(f"Executing Mock Weather Generation ({start_dt.date()} to {end_dt.date()})...")

    # Initialize initial state per station
    station_states = {}
    for stn in stations:
        station_states[stn["station_id"]] = {
            "temp": 26.5,
            "hum": 82.0,
            "press": 1011.0,
            "wind": 6.5,
        }

    total_records = 0
    curr_dt = start_dt

    with open(out_file, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)

        while curr_dt <= end_dt:
            ts_str = curr_dt.strftime("%Y-%m-%d %H:%M:%S")

            for stn in stations:
                stn_id = stn["station_id"]
                reg_id = stn["region_id"]
                st_dict = station_states[stn_id]

                metrics = calculate_hourly_weather_state(curr_dt, st_dict)

                row = [
                    ts_str,
                    stn_id,
                    reg_id,
                    metrics["temperature_c"],
                    metrics["humidity_pct"],
                    metrics["rainfall_mm"],
                    metrics["wind_speed_kmh"],
                    metrics["weather_condition"],
                    metrics["pressure_hpa"],
                ]
                writer.writerow(row)
                total_records += 1

            curr_dt += timedelta(hours=1)

    print(f"Mock weather generation finished: {total_records:,} rows saved at {out_file}.")
    return str(out_file)


if __name__ == "__main__":
    cfg = load_config()
    generate_weather_records(cfg)
