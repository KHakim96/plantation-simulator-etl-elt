"""
Batch source-data generation runner for Smart Plantation Analytics (Phase 1).

Runs the six batch source generators in the required dependency order against
the master configuration in ``data_generators/config.yaml``:

    weather
      -> harvest / fertilizer / equipment   (order-independent among these)
      -> HR                                 (uses harvest+equipment for the
                                             "active operational worker" rule)
      -> SAP finance                        (derives postings from all upstream)

The sensor simulator (``generate_sensors.py``) is intentionally NOT part of this
runner — it belongs to the Phase 7 streaming path, not the batch pipeline.

Output: CSV files under ``data/raw/<source>/`` (local simulation scratch).
Delivery to ADLS Landing is handled separately by ``upload_to_adls.py``.

Usage (from the repository root):
    python3 -m data_generators.run_batch_generators
    # or
    python3 data_generators/run_batch_generators.py
"""

import sys
from pathlib import Path
from typing import Callable, Dict, List, Tuple

# Allow both invocation styles by ensuring the repo root is importable:
#   python3 -m data_generators.run_batch_generators
#   python3 data_generators/run_batch_generators.py
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from data_generators.fetch_weather_api import generate_weather_records, load_config
from data_generators.generate_harvest import generate_harvest_transactions
from data_generators.generate_fertilizer import generate_fertilizer_transactions
from data_generators.generate_equipment import generate_equipment_logs
from data_generators.generate_hr_attendance import generate_hr_attendance
from data_generators.generate_sap_finance import generate_sap_finance_transactions

# Ordered generation plan. Each entry: (step name, callable taking config -> path).
# The order enforces the cross-dataset dependencies documented above.
GENERATION_PLAN: List[Tuple[str, Callable[[Dict], str]]] = [
    ("weather", generate_weather_records),
    ("harvest", generate_harvest_transactions),
    ("fertilizer", generate_fertilizer_transactions),
    ("equipment", generate_equipment_logs),
    ("hr", generate_hr_attendance),
    ("finance", generate_sap_finance_transactions),
]


def run_batch(config_path: str = "data_generators/config.yaml") -> Dict[str, str]:
    """
    Run all batch generators in dependency order.

    Returns a mapping of step name -> output CSV path (as reported by each
    generator). Raises if any step fails, leaving prior steps' outputs on disk.
    """
    config = load_config(config_path)
    results: Dict[str, str] = {}

    total = len(GENERATION_PLAN)
    for idx, (name, generate_fn) in enumerate(GENERATION_PLAN, start=1):
        print(f"\n[{idx}/{total}] Generating '{name}' ...")
        out_path = generate_fn(config)
        results[name] = out_path
        print(f"      -> {out_path}")

    print("\nBatch generation complete. Files written:")
    for name, path in results.items():
        print(f"  {name:11s}: {path}")
    return results


if __name__ == "__main__":
    run_batch()
