"""Phase 7: Live sensor simulator — ADLS Incoming delivery.

Simulates live IoT sensor telemetry arriving at the plantation platform by
emitting small CSV files into the ADLS Gen2 **Incoming** container
(``incoming/sensors/``). Each file is one micro-batch of sensor readings for a
single 15-minute interval, mimicking how a fleet of field sensors would deliver
telemetry in near-real-time.

This script is intentionally separate from the batch generator
(``generate_sensors.py``) and the batch uploader (``upload_to_adls.py``):
  * ``generate_sensors.py`` produces ONE large historical CSV for local
    simulation scratch.
  * ``upload_to_adls.py`` delivers the six BATCH sources to ADLS **Landing**
    only (ADF owns Landing → Bronze).
  * THIS script delivers LIVE sensor readings to ADLS **Incoming** only
    (Auto Loader owns Incoming → live Bronze). It never writes to Landing,
    Bronze, Silver, Gold, or any other layer.

Architecture rules enforced here (ARCHITECTURE.md §5/§6/§7, AGENTS.md §5):
  * Writes to **Incoming only** — the streaming hand-off zone.
  * Credentials come from environment variables ONLY. No secret is hard-coded
    or written to disk/logs.
  * Uses the same ADLS Gen2 REST Shared Key signing pattern as
    ``upload_to_adls.py`` (standard library only; no extra dependencies).

Authentication (environment variables):
  * ``AZURE_STORAGE_ACCOUNT``         - ADLS Gen2 storage account name (required)
  * ``AZURE_STORAGE_ACCOUNT_KEY``     - account key (optional; enables real upload)
  * ``ADLS_INCOMING_CONTAINER``       - Incoming container name (default: "incoming")

Environment loading: when executed as a CLI entrypoint (``__main__``) it loads
``.env`` from the repository root via ``python-dotenv`` (existing environment
variables take precedence). If ``AZURE_STORAGE_ACCOUNT_KEY`` is not set the
simulator runs in DRY-RUN mode: it generates the files locally and reports
exactly what it *would* upload, without any network calls.

Usage (from the repository root):
    python3 -m data_generators.sensor_stream_to_adls
    python3 -m data_generators.sensor_stream_to_adls --intervals 4 --dry-run
"""

from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import hmac
import math
import os
import random
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib import request as urlrequest
from urllib.error import HTTPError, URLError
from urllib.parse import quote

import yaml

# Allow both invocation styles by ensuring the repo root is importable:
#   python3 -m data_generators.sensor_stream_to_adls
#   python3 data_generators/sensor_stream_to_adls.py
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# The ONLY ADLS layer this module is ever permitted to write to. Auto Loader
# owns Incoming → live Bronze; Landing/Bronze/Silver/Gold are strictly out of
# scope here.
ALLOWED_LAYER = "incoming"

# Blob prefix (folder) inside the Incoming container where live sensor files
# arrive. Auto Loader watches this prefix.
INCOMING_SENSORS_PREFIX = "sensors"

# Sensor CSV header contract — must match generate_sensors.py exactly.
SENSOR_CSV_HEADERS = [
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


class IncomingOnlyViolation(Exception):
    """Raised if any code path attempts to target a non-Incoming destination."""


def load_config(config_path: str = "data_generators/config.yaml") -> dict[str, Any]:
    """Load and return master YAML configuration."""
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def resolve_incoming_container() -> str:
    """Resolve the Incoming container name from the environment (default incoming)."""
    return os.getenv("ADLS_INCOMING_CONTAINER", "incoming")


def _guard_incoming_only(container: str) -> str:
    """Enforce the Incoming-only contract.

    The simulator is architecturally forbidden from writing to Landing, Bronze,
    Silver, Gold, live-Bronze, live-Silver, or checkpoints. This guard rejects
    any container that is explicitly one of those layers.
    """
    forbidden = {
        "landing",
        "bronze",
        "silver",
        "gold",
        "live-bronze",
        "live-silver",
        "checkpoints",
    }
    if container.strip().lower() in forbidden:
        raise IncomingOnlyViolation(
            f"Refusing to upload: container '{container}' is not the Incoming layer. "
            "sensor_stream_to_adls.py writes to ADLS Incoming ONLY "
            "(Auto Loader owns Incoming -> live Bronze)."
        )
    return container


# ==============================================================================
# 1. SENSOR READING GENERATION (reuses the Phase 1 generator's models)
# ==============================================================================


def _calculate_diurnal_weather(dt: datetime) -> tuple[float, float, float]:
    """Diurnal air temperature (°C), humidity (%), and light intensity (lux)."""
    hour = dt.hour + dt.minute / 60.0
    temp_sin = math.sin((hour - 9) * math.pi / 12)
    air_temp = 27.5 + 4.5 * temp_sin + random.uniform(-0.5, 0.5)
    humidity = 80.0 - 15.0 * temp_sin + random.uniform(-1.5, 1.5)
    humidity = max(20.0, min(100.0, humidity))
    if 6.0 <= hour <= 19.0:
        sun_sin = math.sin((hour - 6.0) * math.pi / 13.0)
        light_lux = max(0.0, 100000.0 * sun_sin + random.uniform(-2000, 2000))
    else:
        light_lux = 0.0
    return round(air_temp, 2), round(humidity, 2), round(light_lux, 1)


def _build_sensor_roster(config: dict[str, Any]) -> list[tuple[str, str, str]]:
    """Build the (block_id, sensor_id, crop_type) roster matching generate_sensors.py."""
    blocks: list[dict[str, Any]] = config.get("blocks", [])
    sensors_list: list[tuple[str, str, str]] = []
    for block in blocks:
        b_id = block["id"]
        crop = block["crop_type"]
        sensor_count = 2 if b_id in ["BLK01", "BLK05", "BLK06", "BLK10"] else 1
        for s_idx in range(1, sensor_count + 1):
            s_id = f"SNS-{b_id}-{s_idx:02d}"
            sensors_list.append((b_id, s_id, crop))
    return sensors_list


def _init_sensor_states(
    config: dict[str, Any], sensors_list: list[tuple[str, str, str]]
) -> dict[str, dict[str, Any]]:
    """Initialize per-sensor state (battery, base moisture, pH, temp range)."""
    crop_types_list: list[dict[str, Any]] = config.get("crop_types", [])
    crop_bounds = {c["name"]: c for c in crop_types_list}
    states: dict[str, dict[str, Any]] = {}
    for b_id, s_id, crop in sensors_list:
        cb = crop_bounds.get(crop, {})
        m_opt = cb.get("optimal_moisture_pct", [50, 80])
        t_opt = cb.get("optimal_temp_c", [20, 30])
        states[s_id] = {
            "battery": random.uniform(85.0, 100.0),
            "base_moisture": random.uniform(m_opt[0], m_opt[1]),
            "base_ph": random.uniform(6.0, 6.8),
            "temp_range": t_opt,
        }
    return states


def generate_interval_readings(
    config: dict[str, Any],
    sensor_states: dict[str, dict[str, Any]],
    sensors_list: list[tuple[str, str, str]],
    interval_dt: datetime,
) -> list[list[str]]:
    """Generate one micro-batch of sensor readings for a single 15-minute interval.

    Returns a list of CSV rows (strings), one per sensor, matching
    ``SENSOR_CSV_HEADERS``. Anomaly and missing-data injection follow the same
    rates as the Phase 1 generator (``generator_settings.randomization``).
    """
    gen_cfg = config.get("generator_settings", {})
    anomaly_rate = gen_cfg.get("randomization", {}).get("anomaly_rate", 0.05)
    missing_rate = gen_cfg.get("randomization", {}).get("missing_data_rate", 0.01)

    air_temp, humidity, light_lux = _calculate_diurnal_weather(interval_dt)
    ts_str = interval_dt.strftime("%Y-%m-%d %H:%M:%S")

    rows: list[list[str]] = []
    for b_id, s_id, _crop in sensors_list:
        st = sensor_states[s_id]
        # Battery discharges slowly (approx 0.0005% per interval).
        st["battery"] -= random.uniform(0.0001, 0.0008)
        if st["battery"] < 15.0:
            st["battery"] = 100.0  # Battery replaced

        soil_moisture = st["base_moisture"] + random.uniform(-1.5, 1.5)
        soil_temp = air_temp - 2.0 + random.uniform(-0.8, 0.8)
        soil_ph = st["base_ph"] + random.uniform(-0.05, 0.05)
        batt_val = str(round(st["battery"], 1))
        status = "OK"

        # Anomaly injection.
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

        moisture_str = str(round(max(0.0, min(100.0, soil_moisture)), 2))
        soil_temp_str = str(round(soil_temp, 2))
        air_temp_str = str(air_temp)
        humidity_str = str(humidity)
        ph_str = str(round(max(4.0, min(9.0, soil_ph)), 2))
        light_str = str(light_lux)

        # Missing-data injection (1% of readings lose one attribute).
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

        rows.append(
            [
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
        )
    return rows


# ==============================================================================
# 2. ADLS GEN2 UPLOAD (Shared Key REST signing — same pattern as Phase 1)
# ==============================================================================


def _shared_key_authorization(
    account: str, key: str, method: str, path: str, headers: dict[str, str], length: int = 0
) -> str:
    """Build an Azure Storage Shared Key Authorization header value.

    Query parameters are signed as separate ``name:value`` lines AFTER the
    canonicalized resource, sorted by lowercase parameter name — never embedded
    inside the resource path (that produced HTTP 403 in Phase 1; verified).
    """
    raw_path, _, raw_query = path.partition("?")
    query_pairs = []
    for part in raw_query.split("&"):
        if not part:
            continue
        name, _, value = part.partition("=")
        query_pairs.append((name.lower(), f"{name.lower()}:{value}"))
    query_pairs.sort(key=lambda kv: kv[0])
    query_lines = [line for _, line in query_pairs]

    string_to_sign = "\n".join(
        [
            method,
            "",  # Content-Encoding
            "",  # Content-Language
            str(length) if length else "",  # Content-Length
            "",  # Content-MD5
            headers.get("Content-Type", ""),
            "",  # Date
            "",  # If-Modified-Since
            "",  # If-Match
            "",  # If-None-Match
            "",  # If-Unmodified-Since
            "",  # Range
            "x-ms-date:" + headers["x-ms-date"],
            "x-ms-version:" + headers["x-ms-version"],
            f"/{account}/{raw_path.lstrip('/')}",
        ]
        + query_lines
    )
    signature = base64.b64encode(
        hmac.new(base64.b64decode(key), string_to_sign.encode("utf-8"), hashlib.sha256).digest()
    ).decode("utf-8")
    return f"SharedKey {account}:{signature}"


def _adls_request(
    account: str, key: str, method: str, resource_path: str, data: bytes | None = None
) -> None:
    """Perform a single ADLS Gen2 REST call signed with the account key."""
    url = f"https://{account}.dfs.core.windows.net/{resource_path}"
    headers = {
        "x-ms-date": datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT"),
        "x-ms-version": "2021-08-06",
    }
    length = len(data) if data else 0
    if data is not None:
        headers["Content-Type"] = "application/octet-stream"
    headers["Authorization"] = _shared_key_authorization(
        account, key, method, resource_path, headers, length
    )
    req = urlrequest.Request(url, data=data, headers=headers, method=method)
    try:
        with urlrequest.urlopen(req, timeout=30) as resp:
            resp.read()
    except HTTPError as err:
        raise RuntimeError(
            f"ADLS request failed: {method} {resource_path} -> HTTP {err.code} {err.reason}"
        ) from err
    except URLError as err:
        raise RuntimeError(
            f"ADLS request failed: {method} {resource_path} -> {err.reason}"
        ) from err


def upload_bytes_to_incoming(
    account: str, key: str, container: str, blob_path: str, payload: bytes
) -> None:
    """Upload one payload to the Incoming container using create/append/flush."""
    _guard_incoming_only(container)
    encoded_path = "/".join(quote(part) for part in f"{container}/{blob_path}".split("/"))
    # create (resource=file)
    _adls_request(account, key, "PUT", f"{encoded_path}?resource=file")
    # append
    if payload:
        _adls_request(account, key, "PATCH", f"{encoded_path}?action=append&position=0", payload)
    # flush
    _adls_request(
        account, key, "PATCH", f"{encoded_path}?action=flush&position={len(payload)}"
    )


# ==============================================================================
# 3. STREAMING SIMULATION RUNNER
# ==============================================================================


def _rows_to_csv_bytes(rows: list[list[str]]) -> bytes:
    """Serialize rows to CSV bytes with the sensor header contract."""
    import io

    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(SENSOR_CSV_HEADERS)
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8")


def run_sensor_stream(
    config_path: str = "data_generators/config.yaml",
    num_intervals: int = 4,
    start_dt: datetime | None = None,
    dry_run: bool = False,
    local_out_dir: str | None = None,
) -> dict[str, Any]:
    """Generate and deliver live sensor readings for ``num_intervals`` intervals.

    Each interval produces one CSV file under ``incoming/sensors/`` containing
    one reading per sensor for that 15-minute window.

    Parameters:
      * ``num_intervals``: number of 15-minute intervals to simulate (default 4
        = 1 hour of telemetry). Each interval = one file.
      * ``start_dt``: starting datetime (UTC). Defaults to ``datetime.now``
        truncated to the previous 15-minute boundary.
      * ``dry_run``: if True (or no account key), no network calls; files are
        written to ``local_out_dir`` (or a temp dir) and reported.
      * ``local_out_dir``: optional local directory for dry-run file output.

    Returns a summary dict with upload/dry-run details.
    """
    config = load_config(config_path)
    global_cfg = config.get("global_settings", {})
    seed = global_cfg.get("random_seed", 42)
    random.seed(seed)

    gen_cfg = config.get("generator_settings", {})
    interval_mins = gen_cfg.get("sensor_interval_minutes", 15)

    account = os.getenv("AZURE_STORAGE_ACCOUNT")
    key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
    container = _guard_incoming_only(resolve_incoming_container())

    # Determine dry-run mode: explicit flag wins, otherwise key absence.
    effective_dry_run = dry_run or not bool(key)

    if not effective_dry_run and not account:
        raise RuntimeError(
            "AZURE_STORAGE_ACCOUNT is not set. Both AZURE_STORAGE_ACCOUNT and "
            "AZURE_STORAGE_ACCOUNT_KEY are required for a live upload."
        )

    sensors_list = _build_sensor_roster(config)
    sensor_states = _init_sensor_states(config, sensors_list)

    # Start time: truncate to the previous interval boundary.
    if start_dt is None:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        start_dt = now.replace(second=0, microsecond=0)
        start_dt = start_dt - timedelta(minutes=start_dt.minute % interval_mins)

    # Local output directory for dry-run.
    out_dir: str = local_out_dir or str(Path("data") / "raw" / "sensors_stream_dryrun")
    if effective_dry_run:
        Path(out_dir).mkdir(parents=True, exist_ok=True)

    live_account: str | None = account if not effective_dry_run else None
    live_key: str | None = key if not effective_dry_run else None

    print("Live sensor stream simulator")
    print(f"  destination layer : {ALLOWED_LAYER} (Incoming only)")
    print(f"  container         : {container}")
    print(f"  blob prefix       : {INCOMING_SENSORS_PREFIX}/")
    print(f"  storage account   : {account or '(not set)'}")
    print(f"  mode              : {'DRY-RUN' if effective_dry_run else 'LIVE UPLOAD'}")
    print(f"  sensors           : {len(sensors_list)}")
    print(f"  intervals         : {num_intervals} (every {interval_mins} min)")
    print(f"  start time        : {start_dt}")

    uploaded: list[str] = []
    total_rows = 0
    for i in range(num_intervals):
        interval_dt = start_dt + timedelta(minutes=i * interval_mins)
        rows = generate_interval_readings(config, sensor_states, sensors_list, interval_dt)
        csv_bytes = _rows_to_csv_bytes(rows)
        total_rows += len(rows)

        # Deterministic file name: interval timestamp -> blob path.
        ts_name = interval_dt.strftime("%Y%m%dT%H%M%S")
        blob_path = f"{INCOMING_SENSORS_PREFIX}/sensors_{ts_name}.csv"

        if effective_dry_run:
            local_file = Path(out_dir) / f"sensors_{ts_name}.csv"
            with open(local_file, "wb") as f:
                f.write(csv_bytes)
            print(f"  [dry-run] interval {i + 1}/{num_intervals}: "
                  f"{len(rows)} readings -> {local_file} "
                  f"(would upload to {container}/{blob_path})")
        else:
            upload_bytes_to_incoming(
                str(live_account), str(live_key), container, blob_path, csv_bytes
            )
            print(f"  [upload]  interval {i + 1}/{num_intervals}: "
                  f"{len(rows)} readings -> {container}/{blob_path}")
        uploaded.append(f"{container}/{blob_path}")

    summary = {
        "container": container,
        "layer": ALLOWED_LAYER,
        "prefix": INCOMING_SENSORS_PREFIX,
        "dry_run": effective_dry_run,
        "sensors": len(sensors_list),
        "intervals": num_intervals,
        "total_readings": total_rows,
        "destinations": uploaded,
    }
    print(
        f"\n{'DRY-RUN complete' if effective_dry_run else 'Upload complete'}: "
        f"{num_intervals} file(s), {total_rows} readings -> "
        f"container '{container}/{INCOMING_SENSORS_PREFIX}' (Incoming only)."
    )
    return summary


def _load_dotenv_if_running_as_cli() -> None:
    """Load ``.env`` from the repository root when run as a CLI entrypoint.

    Real environment variables always win over ``.env`` values. This is NOT
    executed on import so library users (and tests) keep full control.
    """
    if __package__ in (None, "") or __name__ == "__main__":
        try:
            from dotenv import load_dotenv  # python-dotenv (in requirements.txt)

            load_dotenv(Path(__file__).resolve().parent.parent / ".env", override=False)
        except ImportError:  # pragma: no cover - dotenv is a declared dependency
            print("  [warn] python-dotenv not installed; relying on the real environment only.")


def main() -> int:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Simulate live sensor telemetry arriving at ADLS Incoming."
    )
    parser.add_argument(
        "--intervals",
        type=int,
        default=4,
        help="Number of 15-minute intervals to simulate (default: 4 = 1 hour).",
    )
    parser.add_argument(
        "--config",
        default="data_generators/config.yaml",
        help="Path to master config.yaml.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Generate files locally without uploading (no network calls).",
    )
    parser.add_argument(
        "--local-out-dir",
        default=None,
        help="Local output directory for dry-run files.",
    )
    args = parser.parse_args()

    _load_dotenv_if_running_as_cli()
    try:
        run_sensor_stream(
            config_path=args.config,
            num_intervals=args.intervals,
            dry_run=args.dry_run,
            local_out_dir=args.local_out_dir,
        )
        return 0
    except Exception as exc:  # noqa: BLE001 - top-level CLI entry point: fail loudly
        print(f"\nERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
