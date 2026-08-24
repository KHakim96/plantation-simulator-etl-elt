# Phase 10 Evidence Index

All evidence collected during the Phase 10 live end-to-end verification
(2026-08-24). Every screenshot and run ID below was captured from the real
Azure/Databricks environment — nothing is fabricated.

## Verified Run IDs

| Component | ID | Status |
|---|---|---|
| Databricks batch job | `817981045760739` | — |
| Databricks batch run | `587618142185355` | TERMINATED/SUCCESS |
| — trigger_adf task | `1109103384169245` | TERMINATED/SUCCESS |
| — bronze_to_silver task | `634953965469814` | TERMINATED/SUCCESS |
| — dq_checks task | `721436103247199` | TERMINATED/SUCCESS |
| — silver_to_gold task | `300378927279767` | TERMINATED/SUCCESS |
| ADF pipeline run (workflow-triggered) | `21ae0695-8c8e-455e-9029-996aec96c3ea` | Succeeded |
| ADF pipeline run (manual, pre-verification) | `016dd1f3-c557-473e-a698-0ebdc7f862b5` | Succeeded |
| Databricks streaming job | `649208723548889` | — |
| Databricks streaming run | `231629716446264` | TERMINATED/SUCCESS |
| — sensors_stream task | `118292601590615` | TERMINATED/SUCCESS |
| Git commit used | `db271be49de44887e8b9006b55ace3772d086f80` | — |

## Verified Data Results

| Layer | Rows | Notes |
|---|---|---|
| Source CSVs (local) | 48,595 | 6 files |
| ADLS Landing | 48,595 | 6 blobs, byte-identical to local |
| ADLS Bronze | 48,595 | 6 CSVs, byte-identical to Landing |
| ADLS Silver | 48,595 | 6 Delta tables |
| DQ Gate | 42/42 PASS | 6 sources × 7 checks |
| ADLS Gold | 40,166 | 6 Delta models |
| Synapse Serverless | 40,166 | 6 serving views verified |
| ADLS Incoming | 196 readings | 14 files |
| Live Bronze | 196 | Delta, checkpointed |
| Live Silver | 196 | Delta, checkpointed |
| Databricks SQL (live) | 196 | 14 sensors, 190 OK / 3 ANOMALY / 3 FAULT |

## Screenshots

| File | Component | What it proves |
|---|---|---|
| `22_streamlit_overview.png` | Streamlit — Executive Overview | Dashboard loads with real Gold data: 97.52M kg harvested, 9,112 ops, RM 10.73M cost, 30 equipment, 24 workforce |
| `23_streamlit_live_data.png` | Streamlit — Live Sensors | Live sensor monitoring with real Databricks SQL data: 196 readings, 14 sensors, 190 OK / 3 ANOMALY / 3 FAULT, per-sensor status table, environmental trend charts |
| `24_streamlit_harvest.png` | Streamlit — Harvest | Harvest analytics section with real Gold data via Synapse |
| `25_streamlit_financial.png` | Streamlit — Financial/Costs | Financial analytics section with real Gold data via Synapse |
| `26_streamlit_executive.png` | Streamlit — Executive Overview (light) | Executive overview in alternate theme |

## Browser-based portal screenshots (NOT captured)

The following evidence was **not** captured as browser screenshots because
Azure Portal and Databricks Workspace require interactive sign-in that is not
available in this headless session. CLI/SDK evidence was collected instead:

| Intended screenshot | Alternative evidence |
|---|---|
| Azure Storage / ADLS containers | SDK blob listing with row counts and byte sizes |
| ADF pipeline definition & run | REST API run details with status, duration, parameters |
| Databricks workflow DAG | CLI job definition + run state + task logs |
| Databricks task outputs | CLI get-run-output logs with row counts |
| Synapse query results | pyodbc query results with row counts |
| Databricks SQL query results | databricks-sql-connector query results |
| Streaming checkpoints | SDK blob listing of checkpoint container |

## Local Health Checks

| Check | Result |
|---|---|
| pytest | 173 passed, 21 skipped (2.90s) |
| Ruff | Pre-existing style suggestions only (no errors) |
| git diff --check | PASS |
| JSON validation | All project JSON valid |
| Secret scan | No hardcoded secrets found |
