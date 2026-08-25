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

The 28 screenshots below were captured from the real Azure/Databricks/Streamlit
environment during verification. All files live under
`docs/evidence/phase-10/screenshots/`. Numbering is **not** sequential — the
gaps (e.g. 23, 24) reflect slots that were intentionally not captured as
screenshots; CLI/SDK evidence was recorded for those instead.

| File | Component | What it proves |
|---|---|---|
| `01_source_generation.png` | Source generation | `run_batch_generators.py` completes; six generated CSVs under `data/raw/<source>/` (48,595 rows) |
| `02_adls_landing.png` | ADLS Landing | `landing` container holds the six source folders/files before ADF ingestion |
| `03a_adf_pipeline_overview.png` | ADF | `PL_Ingest_Landing_To_Bronze` pipeline canvas |
| `03b_adf_foreach_copy.png` | ADF | `ForEach_Landing_Source` iterating the six sources |
| `03c_adf_source_configuration.png` | ADF | DelimitedText source reading Landing by wildcard |
| `03d_adf_sink_configuration.png` | ADF | DelimitedText sink writing `bronze/<source>/<file>.csv` with Overwrite |
| `04_adf_successful_run.png` | ADF | ADF Monitor run `21ae0695` Succeeded with 6 Copy activity runs |
| `05_adls_bronze.png` | ADLS Bronze | `bronze` container holds the six CSVs matching Landing (48,595 rows) |
| `06a_databricks_batch_workflow.png` | Databricks batch | `plantation_batch` DAG: `trigger_adf → bronze_to_silver → dq_checks → silver_to_gold` |
| `06b_databricks_batch_run.png` | Databricks batch | A `plantation_batch` run with its tasks |
| `07_bronze_to_silver.png` | Databricks batch | `bronze_to_silver` task succeeded (48,595 rows) |
| `08_adls_silver.png` | ADLS Silver | `silver` container holds the six Delta folders (`_delta_log` present) |
| `09_dq_task.png` | DQ gate | `dq_checks` task in the Databricks workflow run |
| `10_dq_42_42_pass.png` | DQ gate | `dq_checks` task output — all 42 checks pass |
| `11_silver_to_gold.png` | Databricks batch | `silver_to_gold` task succeeded (40,166 rows) |
| `12_adls_gold.png` | ADLS Gold | `gold` container holds the six Delta model folders |
| `13_synapse_objects_views.png` | Synapse Serverless | `plantation_gold` database with `gold.ext_*`/`gold.vw_*` views |
| `14_synapse_query_result.png` | Synapse Serverless | Query over `gold.vw_*` returning 40,166 total |
| `15a_databricks_streaming_workflow.png` | Databricks streaming | `sensor_streaming` workflow with the `sensors_stream` task |
| `15b_databricks_streaming_task_config.png` | Databricks streaming | `sensors_stream` task and its schedule configuration |
| `16_adls_incoming.png` | ADLS Incoming | `incoming/sensors/` per-interval CSV micro-batch files |
| `17_auto_loader_run.png` | Streaming | `sensors_stream` Auto Loader task — files processed, rows written |
| `18_live_bronze.png` | Live Bronze | `live-bronze` container (Delta folder) |
| `19_live_silver.png` | Live Silver | `live-silver` container (Delta folder) |
| `20_streaming_checkpoint.png` | Streaming | `checkpoints/sensors_stream/` with both checkpoint subdirectories |
| `21_databricks_sql_live_data.png` | Databricks SQL | Query over `live_serving.vw_kpi_sensor_status` returning 196 rows |
| `22_streamlit_overview.png` | Streamlit — Executive Overview | Dashboard loads with real Gold data via Synapse: 97.52M kg harvested, 9,112 ops, RM 10.73M cost, 30 equipment, 24 workforce |
| `25_streamlit_live_sensors.png` | Streamlit — Live Sensors | Live sensor monitoring with real Databricks SQL data: 196 readings, 14 sensors, 190 OK / 3 ANOMALY / 3 FAULT, per-sensor status table, environmental trend charts |

## Supplementary CLI/SDK evidence

In addition to the captured screenshots above, programmatic evidence was
collected from the real environment and cross-checked against the images:

| Evidence | Source |
|---|---|
| ADLS container contents (row counts, byte sizes) | SDK blob listing with download/verify |
| ADF pipeline definition & run | REST API run details with status, duration, parameters |
| Databricks workflow DAG & run state | CLI job definition + run state + task logs |
| Databricks task outputs (row counts) | CLI get-run-output logs |
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
