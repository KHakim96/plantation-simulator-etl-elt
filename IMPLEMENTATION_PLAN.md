# IMPLEMENTATION_PLAN.md — plantation-simulator-etl-elt

---

**Architecture:** FINAL / FROZEN (see `ARCHITECTURE.md`)

**Implementation:** IN PROGRESS — Phase 0, Phase 1, Phase 2, Phase 3, Phase 4, Phase 5, Phase 6, and Phase 7 complete (Phase 2 delivered as ADF Copy Landing → Bronze **file** ingestion; Phase 3 delivered as Databricks Spark Bronze CSV → Silver Delta; Phase 4 delivered as the Databricks Serverless Silver **Data Quality gate**; Phase 5 delivered as Databricks Spark Silver → Gold Delta; Phase 6 delivered as Azure Synapse Serverless SQL historical serving over Gold Delta; Phase 7 delivered as the Databricks Serverless **live sensor streaming path** (Auto Loader → live Bronze Delta → live Silver Delta) with checkpoints on ADLS; see Phase 2, Phase 3, Phase 4, Phase 5, Phase 6, and Phase 7 Evidence)

**Current phase:** Phase 8 — Databricks SQL + Streamlit

> **Phase 2 design revision (human-approved).** The original Phase 2 design
> wrote Bronze as Databricks **Delta** tables through a Databricks-linked ADF
> connector. That design is superseded and was never executed: the workspace
> could not provision the required worker capacity, and the revised design
> keeps Phase 2 Bronze **file-based (CSV)**. ADF is ingestion-only
> (Copy Activity); Delta/Spark processing is Phase 3 scope. See the Phase 2
> section below. This deviates from ARCHITECTURE.md §9/§22 ("Bronze = Delta
> written by ADF Copy") and is recorded there as an **approved Phase 2
> implementation deviation** (Bronze still becomes Delta from Phase 3 onward,
> written by Spark).

---

> **How to use this plan**
>
> - Phases are executed **in order**, one at a time.
> - Do not start a phase until its **Dependencies** are satisfied.
> - Do not implement anything listed under **"What NOT to implement yet"**.
> - Mark unverified items as **PENDING**. Never claim something exists or works
>   unless verified in the real environment (see `AGENTS.md` §5).
> - On completing a phase: update its status, record the **Evidence**, and
>   advance "Current phase" / "Next action" at the top and bottom of this file.
> - Schemas, configuration values, and resource names must be derived from
>   **inspected reality** (actual generated data, actual Azure state), not
>   invented.
>
> ---
>
> ## Phase 0 Status: COMPLETE (see Evidence below)
>
> **Verified via direct read-only SDK access to the storage account (2026-08-22).**
> Azure CLI is not authenticated on this machine (no cached subscription);
> resource-manager-level verification of ADF / Databricks / Synapse relies on
> **manual Azure Portal verification recorded by the operator** (see
> "Operator-verified (portal)" items below). These are recorded as reported
> facts, not programmatically re-verified by the agent.
>
> ### Phase 0 Evidence
>
> **Subscription / Resource group**
> - Subscription: `afec86b2-072d-4bdb-83a9-4fe370a3a0fc` (from `.env`; not
>   re-verified via CLI — no authenticated `az` session).
> - Resource group: `plantation-simulator-rg` (from `.env`; name only).
> - Region: **PENDING** — requires ARM/portal access to record (see Phase 0
>   blockers in git history / operator notes).
>
> **ADLS Gen2 storage account — LIVE VERIFIED (SDK, account key)**
> - Account: `plantationsimulatorrg` (`https://plantationsimulatorrg.dfs.core.windows.net`)
> - SKU: `Standard_LRS` · Account kind: `StorageV2` · Hierarchical namespace:
>   **enabled** (ADLS Gen2 confirmed via successful `dfs.core.windows.net`
>   `List File Systems` calls).
> - All 8 required containers exist and are listable (DFS `list_file_systems`):
>   `landing`, `incoming`, `bronze`, `silver`, `gold`, `live-bronze`,
>   `live-silver`, `checkpoints`.
>
> **ADF — operator-verified (portal), not agent-verified**
> - Resource exists per operator confirmation (name not recorded in repo).
>
> **Databricks workspace — operator-verified (portal), not agent-verified**
> - Resource exists per operator confirmation (name/URL not recorded in repo).
> - ONE shared serverless SQL Warehouse exists and was manually verified
>   (start/stop verified manually; name/ID not recorded in repo).
>
> **Synapse workspace — operator-verified (portal), not agent-verified**
> - Resource exists per operator confirmation (name not recorded in repo).
> - Built-in Serverless SQL was manually verified with a trivial `SELECT 1`.
> - **No dedicated Synapse SQL pool was created** (per operator confirmation;
>   not programmatically re-verified).
>
> **Local environment**
> - `.venv` (Python 3.10.13) with `requirements.txt` installed and all
>   packages import successfully (pandas 2.3.3, numpy 2.2.6, pytest 9.1.1,
>   pyyaml, python-dotenv, azure-storage-blob 12.30.0, azure-identity 1.25.3,
>   requests).
> - `.env.example` documents variable names only (secret-free); `.env` is
>   git-ignored and holds real values (never printed/committed).
> - Environment variable naming reconciled across `.env`, `.env.example`, and
>   `upload_to_adls.py` (`AZURE_STORAGE_ACCOUNT`, `AZURE_STORAGE_ACCOUNT_KEY`,
>   `ADLS_LANDING_CONTAINER`).
>
> **Validation results**
> - ADLS containers listable: **verified** (SDK read-only).
> - Storage access works with the account key: **verified** (SDK + fixed
>   Shared Key REST signing, HTTP 200).
> - Databricks serverless SQL `SELECT 1`: **operator-verified (portal)**.
> - Synapse Built-in Serverless SQL `SELECT 1`: **operator-verified (portal)**.
> - No dedicated Synapse pool: **operator-verified (portal)**.
>
> **Known limitations**
> - `az` CLI is not authenticated (no cached subscription; interactive login
>   requires human action). Region/SKU details for ADF/Databricks/Synapse and
>   their resource IDs are not recorded in the repo.
> - The ADF / Databricks / Synapse existence statements are based on operator
>   portal verification, not on programmatically captured evidence.
>
> ---

---

## Phase 0 — Project + Azure Foundation

**Status:** COMPLETE (see Phase 0 Status/Evidence block at the top of this file)

**Objective**
Establish a verified working foundation: local environment, repository hygiene,
and the minimal set of Azure resources required by later phases (resource
group, ADLS Gen2 storage account + containers, ADF, Databricks workspace,
Synapse workspace with serverless SQL, Databricks serverless SQL Warehouse) —
created carefully under trial/cost constraints.

**Files involved**
- `.env.example` (document required variables; no real secrets)
- `.gitignore` (confirm data/checkpoint exclusions)
- `README.md` (setup notes — only if its owning phase permits; keep minimal)
- No pipeline code in this phase.

**Azure services involved**
- Azure Resource Group
- ADLS Gen2 storage account (containers: landing, incoming, bronze, silver,
  gold, live-bronze, live-silver, checkpoints — exact names recorded after
  creation)
- Azure Data Factory
- Azure Databricks workspace
- Azure Synapse workspace (Serverless SQL only — **no dedicated pool**)
- Databricks SQL: ONE shared serverless SQL Warehouse

**Implementation tasks**
1. Verify Azure subscription/trial limits and region availability for each
   service before creating anything.
2. Create the resource group and the minimal resource set above (portal or
   `az` CLI), choosing the cheapest viable SKUs.
3. Create ADLS containers for all planned layers (see list above).
4. Create/verify ONE serverless Databricks SQL Warehouse with auto-stop.
5. Set up local Python environment scaffolding (venv); document env vars in
   `.env.example` (names only, no values).
6. Configure local Azure authentication (e.g., `az login`, service principal
   for later ADF REST calls) and verify it works.
7. Verify access paths: Databricks workspace reachable, Synapse serverless
   endpoint reachable, storage accessible.

**Dependencies**
- None (first phase).

**Validation tasks**
- `az` CLI / portal shows each resource actually created.
- ADLS containers exist and are listable.
- Databricks workspace opens; serverless SQL Warehouse starts and stops.
- Synapse serverless endpoint accepts a trivial `SELECT 1`.

**Completion criteria**
- All foundation resources exist and were **verified** in the portal/CLI.
- No dedicated Synapse pool created.
- No secrets committed; `.env.example` documents variable names only.
- Resource names, regions, and SKUs recorded as evidence.

**Evidence to record**
- Resource names, regions, SKUs; container names; warehouse name/ID.
- CLI/portal verification output snippets.

**What NOT to implement yet**
- No data generators, no uploads, no ADF pipelines, no Spark code, no dbt
  models, no Synapse views, no streaming, no dashboard, no workflows.

---

## Phase 1 — Source Simulation + ADLS Landing

**Status:** COMPLETE (see Evidence below)

**Objective**
Implement the six batch source generators and the ADLS uploader so that
realistic raw batch files are produced and delivered into **ADLS Landing**
(simulating external systems).

**Files involved**
- `data_generators/fetch_weather_api.py`
- `data_generators/generate_sap_finance.py`
- `data_generators/generate_hr_attendance.py`
- `data_generators/generate_equipment.py`
- `data_generators/generate_harvest.py`
- `data_generators/generate_fertilizer.py`
- `data_generators/upload_to_adls.py`
- `data/landing/` (local simulation scratch only — not committed)
- `tests/test_generators.py`, `tests/test_schemas.py` (basic generator sanity)

**Azure services involved**
- ADLS Gen2 (Landing container only)

**Implementation tasks**
1. Implement each generator to emit realistic, internally consistent simulated
   data (e.g., coherent date ranges, referential coherence where relevant).
2. Decide file format(s) **based on what the generators actually produce**;
   document the choice.
3. Implement `upload_to_adls.py` to deliver files into ADLS **Landing only**.
4. Verify the uploader does **not** write to Bronze or any other layer.

**Dependencies**
- Phase 0 complete (storage account + Landing container verified).

**Validation tasks**
- Run each generator; inspect the actual output files.
- Run the uploader; verify files appear in Landing via portal/CLI/SDK.
- Confirm nothing was written to Bronze/Silver/Gold.

**Completion criteria**
- All six sources generate files and land in ADLS Landing (verified).
- Actual output schemas/samples inspected and recorded (basis for later
   Bronze/Silver/Gold schemas — do not invent them now).

**Evidence to record**
- File counts and names in Landing; sample records; chosen file format(s).

**What NOT to implement yet**
- No ADF pipeline, no Bronze writes, no sensor simulator
  (`generate_sensors.py` belongs to Phase 7), no Spark/dbt/streaming/dashboard.

### Phase 1 Evidence (recorded 2026-08-22, verified against real Azure)

**Chosen file format:** CSV (comma-separated, UTF-8, header row), one file per
source, preserving the per-source folder structure `landing/<source>/<file>.csv`.

**Generation run** — `.venv/bin/python -m data_generators.run_batch_generators`
(config window `2024-01-01` … `2024-03-31`, seed 42):

| Source | File (local) | Rows | Bytes | Landing blob |
|---|---|---|---|---|
| weather | `data/raw/weather/weather_observations.csv` | 6,483 | 506,283 | `landing/weather/weather_observations.csv` |
| harvest | `data/raw/harvest/harvest_transactions.csv` | 9,112 | 971,406 | `landing/harvest/harvest_transactions.csv` |
| fertilizer | `data/raw/fertilizer/fertilizer_applications.csv` | 9,000 | 1,523,343 | `landing/fertilizer/fertilizer_applications.csv` |
| equipment | `data/raw/equipment/equipment_logs.csv` | 10,000 | 1,566,167 | `landing/equipment/equipment_logs.csv` |
| hr | `data/raw/hr/hr_attendance.csv` | 2,000 | 326,323 | `landing/hr/hr_attendance.csv` |
| finance | `data/raw/finance/sap_finance_transactions.csv` | 12,000 | 1,974,121 | `landing/finance/sap_finance_transactions.csv` |

Total: 6 files, 48,595 rows, 6,867,643 bytes.

**Actual CSV headers (inspected from generated files — basis for Bronze schemas):**

- weather: `timestamp, station_id, region_id, temperature_c, humidity_pct, rainfall_mm, wind_speed_kmh, weather_condition, pressure_hpa`
- harvest: `harvest_id, timestamp, block_id, crop_type, employee_id, equipment_id, harvested_weight_kg, quality_grade, moisture_pct, collection_duration_minutes, destination, status`
- fertilizer: `application_id, timestamp, block_id, crop_type, employee_id, material_id, quantity_kg, application_method, equipment_id, weather_station_id, weather_condition, rainfall_mm, application_status, notes`
- equipment: `operation_id, timestamp, equipment_id, equipment_type, block_id, operator_id, operation_type, start_time, end_time, duration_minutes, engine_hours, fuel_consumption_liters, distance_km, maintenance_flag, maintenance_type, status`
- hr: `attendance_id, employee_id, employee_name, role, department, cost_center_id, attendance_date, shift, check_in_time, check_out_time, working_hours, overtime_hours, attendance_status, leave_type, work_location, remarks`
- finance: `document_id, posting_date, posting_timestamp, fiscal_year, fiscal_period, company_code, cost_center_id, gl_account, transaction_type, reference_document, employee_id, equipment_id, material_id, amount, currency, debit_credit_indicator, description`

**Realistic patterns / referential consistency (verified on the actual data):**
- Harvest status mix: COMPLETED 8,180 / DELAYED 724 / CANCELLED 208;
  CANCELLED rows carry the sentinel pattern (weight `0.0`, destination `N/A`,
  grade `REJECT`); 4,987 rows have a blank `equipment_id` (manual harvest).
- Equipment status mix: COMPLETED 6,963 / IDLE 2,467 / DELAYED 364 /
  BREAKDOWN 206; 90 maintenance rows have a blank `block_id` (workshop
  sentinel).
- Finance: 12,000 rows = 6,000 documents, each a balanced S/H double-entry
  pair; all `reference_document` values are `HVT-*` IDs that exist in the
  harvest file.
- HR cross-dataset rule holds: employees active in harvest/equipment on a date
  are never ABSENT/LEAVE that date (0 violations).
- Referential scope: harvest `block_id` ⊆ BLK01–BLK10; harvest `employee_id` ⊆
  harvest-capable roles; harvest `equipment_id` ⊆ EQP001–030; fertilizer
  `material_id` = {MAT01, MAT02, MAT03} (Fertilizer category only).
- Weather: hourly records, 3 stations (STN-NORTH/CENTRAL/SOUTH), diurnal and
  monsoon-aware variation.

**Live ADLS Landing upload — VERIFIED:**
- Command: `.venv/bin/python -m data_generators.upload_to_adls` →
  `mode: LIVE UPLOAD`, 6 files uploaded to container `landing` only.
- Independent verification (separate SDK code path,
  `azure-storage-blob` `ContainerClient.list_blobs()` +
  `download_blob().readall()`): 6 CSV blobs present; every downloaded blob's
  MD5 is **byte-identical** to the local source file; row counts re-counted
  from the downloaded Azure bytes: weather 6,483 / harvest 9,112 / fertilizer
  9,000 / equipment 10,000 / hr 2,000 / finance 12,000.
- ADLS Gen2 directory markers (`equipment`, `fertilizer`, `finance`,
  `harvest`, `hr`, `weather` — 0-byte folder entries) are expected HNS
  artifacts, not data.

**Downstream protection — VERIFIED (after upload):**
- `bronze`, `silver`, `gold`, `live-bronze`, `live-silver`, `incoming`,
  `checkpoints`: all contain **0 blobs** (nothing written past Landing).

**Uploader fixes made in this phase:**
- Fixed the Shared Key signature construction (query parameters must be signed
  as separate `name:value` lines, sorted by lowercase name, after the
  canonicalized resource — previously embedded in the path, causing HTTP 403).
- CLI entrypoint now loads `.env` from the repo root via `python-dotenv`
  (existing environment variables take precedence).

**Configuration fix:** `dataset_sizes.hr` corrected from an unreachable 12,000
to 2,000 (the HR generator is structurally capped at 24 employees × 91 days =
2,184 rows for this window, and skips most weekends).

**Tests:** `.venv/bin/python -m pytest tests/ -v` → **23 passed** (11
generator/config tests, 8 schema-contract tests, 4 new Shared Key
signature/dry-run regression tests). No test mocks or fakes a live Azure
upload; live upload was verified with real API calls.

**Known limitation:** `generate_sensors.py` is implemented prematurely
(future-phase leakage from Phase 7). It has **not** been executed and is not
part of Phase 0/1; it is excluded from the batch upload by design.

---

## Phase 2 — ADF Landing → Bronze

**Status:** COMPLETE (see Phase 2 Evidence below)

**Objective**
ADF batch ingestion from ADLS Landing CSV → ADLS Bronze CSV using **Copy
Activity only** (one parameterized pipeline copies the six Phase 1 Landing
CSV files to `bronze/<source>/<file>.csv`; no transformation).

**Revised design (approved) — Bronze is file-based (CSV) in Phase 2**

- ADF Copy Activity copies each file `landing/<source>/<file>.csv` →
  `bronze/<source>/<file>.csv` (CSV → CSV, byte-oriented raw ingestion).
- **Delta is NOT used in Phase 2** (deferred to Phase 3): no Databricks, no
  Spark, no Delta tables, no `_delta_log`, no Mapping Data Flows.
- **Superseded historical design (never executed, kept for the record only):**
  the original Phase 2 plan wrote Bronze as Databricks Delta tables through a
  Databricks linked service (`LS_Databricks_Bronze`, dataset
  `DS_Bronze_Delta`) plus an interactive cluster, a PAT, and one-time Bronze
  DDL (`databricks/sql/01_create_bronze_tables.sql`). It was abandoned because
  the workspace could not provision the required worker capacity. Those
  artifacts were deleted from the repo and the live factory and are **not part
  of the implemented Phase 2**.

**Files involved**
- `adf/` (linked service, 2 datasets, pipeline, README, verify script)
- `tests/test_adf_artifacts.py` (ADF JSON contract tests)

**Azure services involved**
- Azure Data Factory (Copy Activity only)
- ADLS Gen2 (Landing, Bronze)
- Databricks workspace exists but is **NOT USED in Phase 2**

**Implementation tasks**
1. ~~Create ADF linked services/datasets for ADLS Landing (source) and Bronze
   (sink).~~ ✔ Single ADLS Gen2 linked service reused for both datasets.
2. ~~Create pipeline(s) performing Copy Activity from Landing → Bronze~~ ✔ One
   parameterized pipeline `PL_Ingest_Landing_To_Bronze` (ForEach, sequential,
   6 items).
3. ~~Ensure ADF performs ingestion only~~ ✔ CSV file copy, no transformation,
   no staging, no additional columns.

**Design (verified live, documented in `adf/README.md`)**
- Linked service `LS_Adls_PlantationSimulator` (AzureBlobFS, URL only):
  authentication is the **ADF managed identity**; the factory's system-assigned
  identity holds **Storage Blob Data Contributor** on
  `plantationsimulatorrg` (Reader alone cannot write Bronze). No secrets in
  the repo.
- Datasets: `DS_Landing_Source` (DelimitedText, parameters `SourceContainer`
  default `landing` / `SourceFolder`) and `DS_Bronze_Sink` (DelimitedText,
  parameters `SinkContainer` default `bronze` / `SinkFolder` / `SinkFileName`).
- Pipeline `PL_Ingest_Landing_To_Bronze`: ForEach (sequential) over six
  `{sourceFolder, sourceFile}` items; per item, Copy with
  `wildcardFileName: @item().sourceFile`, sink `copyBehavior: Overwrite`,
  `deleteFilesAfterCompletion: false`, `enableStaging: false`.
- Deterministic reruns: `Overwrite` + explicit sink file name ⇒ each run
  leaves `bronze/<source>/<file>.csv` equal to exactly one copy of the Landing
  file (no duplicates, no growth). Verified by two consecutive successful runs
  producing identical Bronze state.
- Real failure encountered and fixed: ADF Copy sink rejected
  `quoteAllText: false` (`DelimitedTextInvalidSettings: QuoteAllText cannot
  set to false for Copy activity currently`) — the setting was removed (ADF's
  default CSV quoting applies; the six data files contain no quoting edge
  cases, and Bronze files came out byte-identical to Landing).

**Dependencies**
- Phase 1 complete (real files exist in Landing to copy). ✔

**Validation tasks**
- ~~Trigger pipeline run manually; observe run status in ADF monitor.~~ ✔
- ~~Verify Bronze on ADLS and row counts match Landing inputs.~~ ✔

**Completion criteria**
- ADF pipeline run succeeded (verified, with run ID). ✔
- Bronze files present with expected row counts. ✔

**What NOT to implement in Phase 2 (respected)**
- No ADF REST trigger script (Phase 9 orchestration), no Spark, no DQ, no dbt,
  no Gold, no streaming, no dashboard, no Databricks usage, no Delta tables,
  no Data Flows.

### Phase 2 Evidence (recorded 2026-08-22/23, all verified live via az CLI + azure-storage-blob SDK)

**ADF objects (live in `plantation-simulator-adf`, Southeast Asia):**

| Object | Type | Detail |
|---|---|---|
| `LS_Adls_PlantationSimulator` | AzureBlobFS linked service | `https://plantationsimulatorrg.dfs.core.windows.net`, URL only (managed identity) |
| `DS_Landing_Source` | DelimitedText dataset | params `SourceContainer` (default `landing`) / `SourceFolder` |
| `DS_Bronze_Sink` | DelimitedText dataset | params `SinkContainer` (default `bronze`) / `SinkFolder` / `SinkFileName` |
| `PL_Ingest_Landing_To_Bronze` | Pipeline | ForEach sequential × 6, Copy per source, `copyBehavior: Overwrite` |

**RBAC (one-time, required for Bronze writes):** ADF managed identity
(principal `0ca751a3-f9e4-427f-8d10-6ec03520e68a`) granted **Storage Blob Data
Contributor** on storage account `plantationsimulatorrg` (it previously had
Storage Blob Data Reader only, which cannot write Bronze).

**ADF pipeline runs (real run IDs, `az datafactory pipeline-run`):**

| Run ID | Status | Detail |
|---|---|---|
| `99a9bbda-9e76-11f1-b171-86283166b020` | Cancelled | First trigger; per-activity failure `DelimitedTextInvalidSettings` (`quoteAllText: false` unsupported in Copy); cancelled after diagnosis, fix applied |
| `2df2fdb4-9e78-11f1-a07b-86283166b020` | **Succeeded** | 2026-08-22T22:24:03Z → 22:26:13Z UTC (130 s); 6 Copy activities, each 1 file read / 1 file written |
| `9d31b9a4-9e78-11f1-8fb5-86283166b020` | **Succeeded** | 2026-08-22T22:27:11Z → 22:29:31Z UTC (idempotency rerun) |

Per-activity copy statistics from the succeeding run (dataRead == dataWritten ==
Landing byte size, per source): weather 506,283 B / harvest 971,406 B /
fertilizer 1,523,343 B / equipment 1,566,167 B / hr 326,323 B /
finance 1,974,121 B.

**Bronze verification (independent SDK, `adf/scripts/verify_bronze.py`, exit 0,
run after both successful runs):**

| Bronze file | Bytes | Data rows |
|---|---|---|
| `bronze/weather/weather_observations.csv` | 506,283 | 6,483 |
| `bronze/harvest/harvest_transactions.csv` | 971,406 | 9,112 |
| `bronze/fertilizer/fertilizer_applications.csv` | 1,523,343 | 9,000 |
| `bronze/equipment/equipment_logs.csv` | 1,566,167 | 10,000 |
| `bronze/hr/hr_attendance.csv` | 326,323 | 2,000 |
| `bronze/finance/sap_finance_transactions.csv` | 1,974,121 | 12,000 |

Total: 6 files, 48,595 data rows — Bronze == Landing for every source, and the
**MD5 of every Bronze file is identical to its Landing source** (verified via a
separate SDK code path). Bronze contains exactly the six expected source
folders and **no `_delta_log` directories** (no Delta created in Phase 2).

**Landing verification (unchanged):** one CSV per source folder with exact
known-good byte sizes, 48,595 data rows total.

**Downstream protection (verified):** `silver`, `gold`, `live-bronze`,
`live-silver`, `checkpoints`, `incoming` containers all contain 0 blobs.

**Tests:** `.venv/bin/python -m pytest tests/ -v` → **41 passed** (23 Phase 0/1
tests unchanged, 18 new ADF artifact contract tests in
`tests/test_adf_artifacts.py` — including guards that the superseded
Databricks/Delta artifacts cannot be reintroduced and that `quoteAllText` stays
out of the Copy sink).

**Superseded-artifact handling:** the deleted Databricks/Delta artifacts
(`LS_Databricks_Bronze.json`, `DS_Bronze_Delta.json`,
`databricks/sql/01_create_bronze_tables.sql`) were removed from the repo and
from the live factory; they had been deployed but were **never run**. The
Databricks workspace `plantation-simulator-dbx` was never used in Phase 2
(no cluster, no job, no PAT, no DDL executed).

**ARCHITECTURE.md deviation (approved and recorded):** ARCHITECTURE.md §9/§22
state Bronze is "Delta on ADLS written by ADF Copy Activity". The approved
Phase 2 implementation writes Bronze as **CSV files** via ADF Copy. Per
ARCHITECTURE.md §25 this deviation is documented in ARCHITECTURE.md itself as
an "Approved deviation — Phase 2 implementation" note in §9 and §22 (Bronze
still becomes Delta from Phase 3 onward, written by Spark).

---

## Phase 3 — Databricks Spark Bronze → Silver

**Status:** COMPLETE (see Phase 3 Evidence below)

**Objective**
Implement the Spark batch job that reads the Bronze CSV files delivered by
Phase 2, applies cleaning /
validation / deduplication / standardization / transformation, and writes
**Silver Delta** on ADLS.

**Files involved**
- `databricks/batch/bronze_to_silver.py`
- `tests/test_transformations.py` (transformation logic sanity)

**Azure services involved**
- Azure Databricks (compute cluster)
- ADLS Gen2 (Bronze, Silver)

**Implementation tasks**
1. Configure Databricks access to ADLS (verified, secure — no hard-coded
   secrets).
2. Implement Bronze → Silver transformations for each source dataset:
   clean, validate, deduplicate, standardize, transform.
3. Write Silver Delta tables to ADLS.
4. Base all column/schema handling on the **actual Bronze data** inspected in
   Phase 2 — do not invent schemas.

**Dependencies**
- Phase 2 complete (Bronze **CSV files** exist with real data:
  `bronze/<source>/<file>.csv`, 6 files, 48,595 rows). ✔

> **Phase 3 note (design input from implemented Phase 2):** Phase 2 delivered
> Bronze as **CSV files**, not Delta tables. `bronze_to_silver.py` must
> therefore read Bronze with a Spark CSV read (header row, comma delimiter)
> instead of reading Bronze Delta tables. Delta is introduced from Phase 3
> onward (Silver as Delta).

**Validation tasks**
- Run the Spark job on Databricks; verify success.
- Inspect Silver tables; confirm cleaning/dedup/standardization took effect
  (e.g., duplicate counts removed, standardized formats).

**Completion criteria**
- Silver Delta tables exist on ADLS (verified).
- Row counts reconcile with Bronze post-dedup expectations.

**Evidence to record**
- Databricks run link/ID; Silver table list + row counts; sample rows.

**What NOT to implement yet**
- No DQ gate, no dbt, no Gold, no Synapse, no streaming, no dashboard, no
  workflows.

### Phase 3 Evidence (recorded 2026-08-24, verified against real Azure)

**Databricks execution — VERIFIED (Azure Databricks Serverless):**
- `databricks/batch/bronze_to_silver.py` ran successfully on Azure Databricks
  Serverless (Spark 4.1.0, Unity Catalog enabled). The job completed and
  reported all six sources processed. (No Databricks run ID/URL is recorded
  here — it was not captured; execution was confirmed via the job's console
  output and the post-run Silver verification below.)
- Storage authentication: **Unity Catalog external locations** backed by the
  storage credential `plantation_external_adls`. **No storage account key, no
  SAS token, no PAT, and no hard-coded secret** is read or configured anywhere
  in the Databricks execution path (`fs.azure.account.key.*` is never set).

**ADLS paths (deterministic):**
- Bronze input: `abfss://bronze@plantationsimulatorrg.dfs.core.windows.net/<source>/<file>.csv`
- Silver output: `abfss://silver@plantationsimulatorrg.dfs.core.windows.net/<source>`

**Silver verification — FINAL RESULT: PASS (ALL SILVER DATASETS VERIFIED):**
All six Silver datasets were read back from ADLS as **Delta** (each contains
`_delta_log` + Parquet), carry the `_ingested_at` audit column, and have
**0 duplicate rows**:

| Source | Rows | Columns | Duplicates |
|---|---|---|---|
| weather | 6,483 | 10 | 0 |
| harvest | 9,112 | 13 | 0 |
| fertilizer | 9,000 | 15 | 0 |
| equipment | 10,000 | 17 | 0 |
| hr | 2,000 | 17 | 0 |
| finance | 12,000 | 18 | 0 |
| **TOTAL** | **48,595** | — | **0** |

Row counts reconcile exactly with Bronze post-dedup expectations (48,595).

**Schema / sample-row verification — VERIFIED:** Silver column types are
correct, including timestamp and date casts, integer and double measures, the
equipment `maintenance_flag` boolean, and finance `amount` as `decimal(18,2)`.
IDs/categoricals are uppercased/trimmed (standardization) and blank strings are
nullified per the transformation rules.

**Idempotency / overwrite behavior:** Silver writes use `mode=overwrite` with
`overwriteSchema=true` (deterministic full refresh), so reruns replace each
Silver table rather than appending duplicates.

**Tests:** `.venv/bin/python -m pytest tests/` → **55 passed** (41 from
Phases 0–2 unchanged, 14 Phase 3 tests in `tests/test_transformations.py` —
environment/path selection, no-silent-local-fallback, schema/registry coverage,
a no-storage-key/SAS/PAT code guard, and four live local-Spark transformation
tests).

**Phase boundary respected:** no DQ gate, dbt, Gold, Synapse, streaming,
dashboard, or workflow logic was added (all Phase 4+ files remain empty
placeholders).

**Resolved real issues encountered during Phase 3:**
- `INVALID_HANDLE.SESSION_CLOSED` — a dead Spark Connect session on Serverless
  (infrastructure, not a pipeline/code failure); resolved by reconnecting
  Serverless, after which the pipeline completed with 48,595 rows.
- A `SystemExit(0)` from the `__main__` block caused the Databricks editor to
  mark a successful run as failed; fixed by calling `main()` directly (commit
  `f2481ab`).

---

## Phase 4 — Data Quality

**Status:** COMPLETE (see Phase 4 Evidence below)

**Objective**
Implement the Data Quality gate that validates Silver data and **stops
downstream processing on critical failures**.

**Files involved**
- `databricks/batch/dq_checks.py`
- `tests/test_data_quality.py`

**Azure services involved**
- Azure Databricks
- ADLS Gen2 (Silver; Bronze for reconciliation)

**Implementation tasks**
1. Implement DQ checks, chosen from (final set based on actual data): schema,
   nulls, duplicates, row counts, freshness, valid ranges, Bronze/Silver
   reconciliation.
2. Classify checks: critical failures halt the pipeline; non-critical issues
   are logged/reported.
3. Produce a clear DQ result (pass/fail + details) usable by orchestration
   later.

**Dependencies**
- Phase 3 complete (Silver exists with real data).

**Validation tasks**
- Run DQ against known-good Silver → passes.
- Deliberately introduce a bad dataset (or simulate one) → critical check
  fails and downstream is blocked.

**Completion criteria**
- DQ gate demonstrably passes good data and blocks bad data (verified).

**Evidence to record**
- DQ run output: checks executed, pass/fail per check, block behavior proof.

**What NOT to implement yet**
- No dbt/Gold, no Synapse, no streaming, no dashboard, no workflows.

### Phase 4 Evidence (recorded 2026-08-24, verified against real Azure)

**Implementation:** `databricks/batch/dq_checks.py` (PySpark + Delta DQ gate)
+ `tests/test_data_quality.py`. The gate reuses the Phase 3 helpers
(`detect_environment`, `get_spark_session`, `get_silver_path`,
`get_bronze_path`, `SOURCE_ORDER`) — no ADLS auth logic duplicated. Storage
authentication is **Unity Catalog external locations** backed by the storage
credential `plantation_external_adls`. **No storage account key, no SAS token,
no PAT, and no hard-coded secret** is read or configured
(`fs.azure.account.key.*` is never set).

**Checks implemented (all 7 from the plan), per source across all six Silver
datasets (weather, harvest, fertilizer, equipment, hr, finance):**

| # | Check | Type |
|---|---|---|
| 1 | schema (required columns incl. `_ingested_at`) | CRITICAL |
| 2 | nulls (key columns) | CRITICAL |
| 3 | duplicates (business-key uniqueness) | CRITICAL |
| 4 | row counts (expected per-source counts; total 48,595) | CRITICAL |
| 5 | freshness (newest `_ingested_at` within window) | NON-CRITICAL |
| 6 | valid ranges (plausible measure bounds) | NON-CRITICAL |
| 7 | Bronze/Silver reconciliation (Silver == Bronze distinct-key count) | CRITICAL |

Critical checks (`schema`, `nulls`, `duplicates`, `row_count`,
`reconciliation`) halt the pipeline on failure; non-critical checks
(`valid_ranges`, `freshness`) are logged/reported but do not block. A critical
failure returns exit code **1**; PASS returns **0**. The gate prints a
human-readable per-check report (source | check | PASS/FAIL | detail) plus an
overall result.

**Live Databricks run — PASS (Azure Databricks Serverless, Spark Connect,
Unity Catalog enabled):** `dq_checks.py` ran against the live Silver Delta on
ADLS. **42/42 checks passed** (6 sources × 7 checks). **OVERALL RESULT:
PASS**; process exit code **0**. All six Silver datasets passed all 7 checks;
**Bronze/Silver reconciliation passed**; total Silver rows reconciled to
**48,595**. (No Databricks run ID/URL is recorded here — it was not captured;
execution was confirmed via the job's console output.)

**Critical-failure blocking proof (no production data modified):** verified
via the real `main()` with an injected critical `row_count` failure (no Spark /
ADLS / Silver touched) and via automated tests
(`test_evaluate_overall_blocks_only_on_critical`,
`test_critical_noncritical_classification`). A critical failure produced
**OVERALL RESULT: FAIL (downstream processing BLOCKED)** and **exit code 1**;
a non-critical-only failure still produced PASS. Good data and bad data paths
are both demonstrated.

**Serverless compatibility fix:** the per-source `cache()`/`unpersist()` was
removed because Spark Connect translates DataFrame caching into a `PERSIST
TABLE` operation that Serverless rejects
(`[NOT_SUPPORTED_WITH_SERVERLESS] PERSIST TABLE`). Each check is a plain
DataFrame aggregation, so caching was unnecessary; correctness is unchanged. A
guard test (`test_no_serverless_incompatible_persistence_calls`) locks this
out. A separate fix made module loading `__file__`-independent so the script
runs from the Git-backed Databricks workspace.

**Tests:** `.venv/bin/python -m pytest tests/ -q` → **59 passed, 11 skipped**
(55 from Phases 0–3 unchanged; 4 net new Phase 4 always-on tests — the DQ
Spark-behavior tests skip locally because this workstation has no Java, and
run on any Java-enabled runner). Ruff: clean on both changed files.

**Phase boundary respected:** no dbt, Gold, Synapse, streaming, dashboard, or
workflow logic was added; no DQ results Delta table and no Great Expectations
were introduced (out of scope for Phase 4).

---

## Phase 5 — Databricks Spark Silver → Gold

**Status:** COMPLETE (see Phase 5 Evidence below)

**Objective**
Implement the Databricks Spark batch transformation that takes DQ-verified
Silver Delta datasets and produces **analytics-ready Gold Delta** datasets on
ADLS.

**Files involved**
- `databricks/batch/silver_to_gold.py`
- `tests/test_gold_transformations.py`

**Azure services involved**
- Azure Databricks (compute)
- ADLS Gen2 (Silver input, Gold output)

**Implementation design (conceptual)**
- Read DQ-verified Silver Delta datasets from ADLS.
- Perform business-level joins/aggregations to create analytics-ready Gold
  datasets.
- Write Gold as Delta on ADLS.
- Use the **same Unity Catalog / external-location authentication pattern**
  already established in Phase 3 and Phase 4 (storage credential
  `plantation_external_adls`). No storage keys, SAS tokens, PATs, or secrets.
- Gold writes must be **idempotent**: prefer deterministic full-refresh
  behavior (`mode=overwrite`, `overwriteSchema=true`), consistent with Phase 3.
  Rerunning Phase 5 must **not** append duplicate Gold records.
- Must be compatible with **Azure Databricks Serverless**.
- Must **not** invent Gold schemas — the eventual Gold datasets must be derived
  from the **actual** Silver schemas inspected during implementation.

**Candidate Gold concepts** (candidates only — not final until Phase 5
implementation inspects the actual Silver data and confirms they remain
supported):
- `dim_plantation`
- `dim_equipment`
- `dim_employee`
- `fact_harvest`
- `fact_revenue`
- `fact_fertilizer`
- `fact_equipment`

**Implementation tasks**
1. Implement `silver_to_gold.py` to read DQ-verified Silver Delta.
2. Perform business-level joins/aggregations to build analytics-ready Gold
   datasets from the **actual** Silver schemas.
3. Write Gold as Delta on ADLS (idempotent overwrite).
4. Verify Gold datasets are queryable.

**Dependencies**
- Phase 4 complete (DQ-verified Silver data). The DQ gate must remain the
  quality gate before Gold processing: PASS → Gold; FAIL → STOP.

**Validation tasks**
- Run the Spark job on Databricks; verify success.
- Inspect Gold Delta datasets on ADLS; confirm they are analytics-ready and
  idempotent (rerun produces no duplicates).

**Completion criteria**
- Gold Delta datasets exist on ADLS (verified) and are analytics-ready.

**Evidence to record**
- Databricks run link/ID; list of Gold datasets; row counts/sample queries.

**What NOT to implement yet**
- No Synapse views, no streaming, no dashboard, no workflows.

### Phase 5 Evidence (recorded 2026-08-24, verified against real Azure)

**Implementation:** `databricks/batch/silver_to_gold.py` (PySpark Silver →
Gold transformation) + `tests/test_gold_transformations.py`. The job reuses
the Phase 3 helpers (`detect_environment`, `get_spark_session`,
`get_silver_path`, `is_databricks_environment`) — no ADLS auth logic
duplicated. Storage authentication is **Unity Catalog external locations**
backed by the storage credential `plantation_external_adls`. **No storage
account key, no SAS token, no PAT, and no hard-coded secret** is read or
configured (`fs.azure.account.key.*` is never set).

**Gold models implemented (6):** `dim_plantation` was intentionally excluded —
no plantation/block master table exists in Silver to build it from without
fabricating data.

| Gold Model | Type | Source Silver | Grain (Business Key) |
|---|---|---|---|
| `dim_equipment` | Dimension | equipment | `equipment_id` |
| `dim_employee` | Dimension | hr | `employee_id` |
| `fact_harvest` | Fact | harvest | `harvest_id` |
| `fact_revenue` | Fact | finance | `(document_id, debit_credit_indicator, gl_account)` |
| `fact_fertilizer` | Fact | fertilizer | `application_id` |
| `fact_equipment` | Fact | equipment | `operation_id` |

**Live Databricks run — PASS (Azure Databricks Serverless, Spark Connect,
Unity Catalog enabled):** `silver_to_gold.py` ran against the live Silver
Delta on ADLS and wrote six Gold Delta datasets to
`abfss://gold@plantationsimulatorrg.dfs.core.windows.net/<model>`. (No
Databricks run ID/URL is recorded here — it was not captured; execution was
confirmed via the job's console output and the post-run Gold verification
below.)

**Gold verification — FINAL RESULT: PASS (ALL GOLD DATASETS VERIFIED):**
All six Gold datasets were read back from ADLS as **Delta** (each contains
`_delta_log` + Parquet), have **0 duplicate business keys** and **0 null
business keys**, and match expected row counts exactly:

| Gold Model | Rows | Expected | Duplicates | Null Keys |
|---|---|---|---|---|
| dim_equipment | 30 | 30 | 0 | 0 |
| dim_employee | 24 | 24 | 0 | 0 |
| fact_harvest | 9,112 | 9,112 | 0 | 0 |
| fact_revenue | 12,000 | 12,000 | 0 | 0 |
| fact_fertilizer | 9,000 | 9,000 | 0 | 0 |
| fact_equipment | 10,000 | 10,000 | 0 | 0 |
| **TOTAL** | **40,166** | **40,166** | **0** | **0** |

**Idempotency — VERIFIED:** the complete `silver_to_gold.py` transformation
was executed twice. Both runs produced identical row counts (40,166 total).
Deterministic overwrite/full-refresh behavior (`mode=overwrite`,
`overwriteSchema=true`) was verified — no row-count growth on rerun.

**Tests:** `.venv/bin/python -m pytest tests/ -q` → **71 passed, 19 skipped**
(59 from Phases 0–4 unchanged; 12 net new Phase 5 tests — 11 pure tests
always run, 8 Spark-behavior tests skip locally because this workstation has
no Java, and run on any Java-enabled runner). Ruff: clean on both changed
files.

**Phase boundary respected:** no Synapse, streaming, dashboard, or workflow
logic was added. No dbt was reintroduced.

---

## Phase 6 — Synapse Historical Serving

**Status:** COMPLETE (see Phase 6 Evidence below)

**Objective**
Expose Gold Delta through **Azure Synapse Serverless SQL** as the historical
analytical serving layer for the dashboard.

**Files involved**
- `synapse/sql/external_tables.sql`
- `synapse/sql/plantation_views.sql`

**Azure services involved**
- Azure Synapse Serverless SQL (no dedicated pool)
- ADLS Gen2 (Gold)

**Implementation tasks**
1. Verify the serverless endpoint can read Delta on ADLS in this subscription
   (do not assume — test it).
2. Create external tables/views over Gold Delta.
3. Define serving views matching dashboard needs (historical sections).

**Dependencies**
- Phase 5 complete (Gold marts exist).

**Validation tasks**
- Run representative analytical queries from the Synapse serverless endpoint;
  verify results match Gold data.

**Completion criteria**
- Synapse serverless queries over Gold return correct, verified results.

**Evidence to record**
- Created objects list; sample query results; confirmation serverless (not
  dedicated) pool used.

**What NOT to implement yet**
- No dashboard, no streaming, no workflows.

### Phase 6 Evidence (recorded 2026-08-24, verified against real Azure)

**Environment (verified):**
- Synapse workspace: `plantation-simulator-synapse`
- Serverless SQL endpoint: `plantation-simulator-synapse.sql.azuresynapse.net`
- SQL pool used: **Built-in / Serverless** (no dedicated pool)
- Database: `plantation_gold` (collation `Latin1_General_100_BIN2_UTF8`)
- Schema: `gold`
- ADLS Gold container: `abfss://gold@plantationsimulatorrg.dfs.core.windows.net/`
- Access: Synapse workspace **managed identity** — the workspace identity has
  **Storage Blob Data Contributor** on storage account `plantationsimulatorrg`.
  No storage account key, SAS token, PAT, or secret was used.
- A database master key was required in `plantation_gold` and was successfully
  created.

**Deployment:** `synapse/sql/external_tables.sql` executed successfully, then
`synapse/sql/plantation_views.sql` executed successfully, on the serverless
endpoint.

**Objects verified (12):**
- 6 base external objects over `OPENROWSET(... FORMAT='DELTA')` on the Gold
  container via the `GoldAdls` external data source (managed-identity
  credential `SynapseIdentity`):
  `gold.ext_dim_equipment`, `gold.ext_dim_employee`, `gold.ext_fact_harvest`,
  `gold.ext_fact_revenue`, `gold.ext_fact_fertilizer`, `gold.ext_fact_equipment`.
- 6 serving views:
  `gold.vw_dim_equipment`, `gold.vw_dim_employee`, `gold.vw_fact_harvest`,
  `gold.vw_fact_revenue`, `gold.vw_fact_fertilizer`, `gold.vw_fact_equipment`.

**Gold row-count verification (all matched the Phase 5 verified Gold output):**

| Gold dataset | Rows |
|---|---|
| dim_equipment | 30 |
| dim_employee | 24 |
| fact_harvest | 9,112 |
| fact_revenue | 12,000 |
| fact_fertilizer | 9,000 |
| fact_equipment | 10,000 |
| **TOTAL** | **40,166** |

**Business-key verification:**
- `fact_harvest`: 0 duplicate business keys, 0 null business keys.
- `fact_revenue`: business grain is the composite key
  `(document_id, debit_credit_indicator, gl_account)` — 12,000 total rows,
  12,000 distinct composite business keys, 0 duplicate business keys, 0 null
  business keys. (An initial generic check counting `document_id` alone showed
  6,000 apparent duplicates; this was **not** a data-quality failure. The
  Phase 5 grain is the composite key, and the corrected composite-key
  verification returned 0 duplicates / 0 nulls.)
- `fact_fertilizer`: 0 duplicate business keys, 0 null business keys.
- `fact_equipment`: 0 duplicate business keys, 0 null business keys.

**Analytical serving verification:** a live Synapse Serverless analytical query
against `gold.vw_fact_harvest` executed successfully (`GROUP BY crop_type`,
`COUNT_BIG(*)`, `SUM(harvested_weight_kg)`, `ORDER BY total_kg DESC`) and
returned results for OIL PALM, RUBBER, TEA, and COFFEE.

**Phase boundary respected:** no dashboard, streaming, or workflow logic was
added. No code outside `synapse/sql/` was introduced; Phase 0–5 implementation
and evidence are unchanged.

---

## Phase 7 — Live Sensor Streaming

**Status:** COMPLETE (see Phase 7 Evidence below)

**Objective**
Implement the near-real-time sensor path: sensor simulator → ADLS Incoming →
Auto Loader → Structured Streaming → live Bronze Delta → live Silver Delta,
with checkpoints on ADLS.

**Files involved**
- `data_generators/generate_sensors.py`
- `databricks/streaming/sensors_stream.py`
- `data/` scratch (local simulation only — not committed)

**Azure services involved**
- Azure Databricks (Auto Loader + Structured Streaming)
- ADLS Gen2 (Incoming, live Bronze, live Silver, checkpoints)

**Implementation tasks**
1. Implement the sensor simulator writing sensor reading files into ADLS
   Incoming.
2. Implement `sensors_stream.py`: Auto Loader incremental discovery →
   Structured Streaming processing → write live Bronze Delta → live Silver
   Delta; maintain current sensor state where appropriate.
3. Configure checkpoint locations on ADLS Gen2 (never in Git).

**Dependencies**
- Phase 0 (storage/containers) and Phase 3 patterns (Databricks ↔ ADLS
  access) — streaming path itself is independent of the batch Gold chain.

**Validation tasks**
- Start the stream; generate sensor files; verify rows appear incrementally
  in live Bronze and live Silver Delta.
- Restart the stream; verify checkpoint-based recovery (no duplicates /
  reprocessing storms).

**Completion criteria**
- Incremental live processing verified end-to-end with checkpoints on ADLS.

**Evidence to record**
- Checkpoint container paths; live table row growth observations; stream run
  ID.

**What NOT to implement yet**
- No Databricks SQL serving views, no dashboard, no streaming workflow JSON
  (Phase 9), no coupling of streaming to ADF/Gold/Synapse (intentionally
  excluded).

### Phase 7 Evidence (recorded 2026-08-24, verified against real Azure)

**Implementation:**
- `data_generators/sensor_stream_to_adls.py` — live sensor simulator. Generates
  15-minute-interval telemetry for 14 sensors (SNS-BLKxx-01/02 across 10
  blocks; 2 sensors for BLK01/05/06/10) and uploads one CSV micro-batch per
  interval to ADLS **Incoming** (`incoming/sensors/sensors_<UTC>.csv`). Uses the
  same ADLS Gen2 REST Shared Key signing pattern as Phase 1 `upload_to_adls.py`
  (env-var credentials only; an **Incoming-only** guard rejects any other
  layer). Supports DRY-RUN (no network) and LIVE UPLOAD modes. CSV schema
  matches `generate_sensors.py` exactly.
- `databricks/streaming/sensors_stream.py` — Databricks Structured Streaming
  job. Stage 1: **Auto Loader** (`cloudFiles`, explicit schema, no inference)
  reads `incoming/sensors/` → appends raw-fidelity rows to **live Bronze**
  Delta. Stage 2: reads live Bronze Delta as a stream →
  `transform_live_silver` (cast timestamp/numerics, uppercase/trim IDs, drop
  missing keys, `dropDuplicates(["sensor_id", "timestamp"])`, `_ingested_at`
  audit) → appends to **live Silver** Delta. Both streams use
  `trigger(availableNow=True)` (micro-batch drain-and-stop) and reuses the
  Phase 3 helpers (`detect_environment`, `get_spark_session`,
  `is_databricks_environment`) — no ADLS auth logic duplicated, no
  `__file__`-dependent loading on Databricks.
- `tests/test_sensor_streaming.py` — Phase 7 tests (pure local + Spark).

**Authentication (Databricks):** **Unity Catalog external locations** backed by
the storage credential `plantation_external_adls`. **No storage account key, no
SAS token, no PAT, and no hard-coded secret** is read or configured by the
Databricks streaming path (`fs.azure.account.key.*` is never set). External
locations verified for `incoming`, `live-bronze`, `live-silver`, and
`checkpoints`.

**Deterministic ADLS paths:**
- Input (Auto Loader): `abfss://incoming@plantationsimulatorrg.dfs.core.windows.net/sensors`
- Live Bronze Delta: `abfss://live-bronze@plantationsimulatorrg.dfs.core.windows.net/sensors`
- Live Silver Delta: `abfss://live-silver@plantationsimulatorrg.dfs.core.windows.net/sensors`
- Bronze checkpoint: `abfss://checkpoints@plantationsimulatorrg.dfs.core.windows.net/sensors_stream/sensors_live_bronze`
- Silver checkpoint: `abfss://checkpoints@plantationsimulatorrg.dfs.core.windows.net/sensors_stream/sensors_live_silver`

**Databricks execution — VERIFIED (Azure Databricks Serverless):**
`sensors_stream.py` ran on Azure Databricks Serverless (Unity Catalog enabled).
(No Databricks run ID/URL is recorded here — it was not captured; execution was
confirmed via the job's console output and the post-run live-layer row counts
below.)

**Live verification (all on Azure):**

1. **Initial streaming run** — Incoming → live Bronze: **56 rows**; live Bronze
   → live Silver: **56 rows**.
2. **Restart / re-upload verification** — checkpoint state correctly prevented
   reprocessing of already-processed files. Bronze increased **56 → 84**
   because only **28 genuinely new readings** existed in that upload burst; the
   2 overlapping files were **correctly ignored by Auto Loader** (exactly-once
   checkpoint behavior). Silver also reached **84**. Investigation confirmed
   **no data loss** and **0 duplicate** `sensor_id + timestamp` keys.
3. **Clean incremental verification** — 4 explicitly non-overlapping files were
   generated (`sensors_20260823T213000.csv`, `sensors_20260823T214500.csv`,
   `sensors_20260823T220000.csv`, `sensors_20260823T221500.csv`; 56 new
   readings) and uploaded to `incoming/sensors/`. `sensors_stream.py` was rerun
   on Azure Databricks Serverless. Final counts: live Bronze **140**, live
   Silver **140** (expected 84 + 56 = 140). ✔

**Idempotency / restart-safety:** Auto Loader incremental file discovery +
deterministic per-stream checkpoints on ADLS give exactly-once/incremental
recovery; the rerun processed only newly arrived files (no duplicate
reprocessing), and live Silver `dropDuplicates` on the business key guards
against intra-batch duplicates.

**Tests:** `.venv/bin/python -m pytest tests/ -q` → **91 passed, 21 skipped**
(71 from Phases 0–6 unchanged; 20 net new Phase 7 always-on tests — the 2
Spark-behavior tests skip locally because this workstation has no Java and run
on any Java-enabled runner). Ruff: clean on all changed files.

**Phase boundary respected:** no Databricks SQL serving views, no dashboard, no
streaming workflow JSON, and no coupling of streaming to ADF/Gold/Synapse was
added. Phase 8+ placeholders (`dashboard/app.py`,
`databricks/sql/live_sensor_kpis.sql`, workflow JSON) remain untouched.

---

## Phase 8 — Databricks SQL + Streamlit

**Status:** NOT STARTED

**Objective**
Build the Streamlit dashboard with two data paths: **historical via Synapse
Serverless SQL** (Gold) and **live via Databricks SQL** (live Silver), served
by the one shared serverless SQL Warehouse.

**Files involved**
- `dashboard/app.py`
- `databricks/sql/live_sensor_kpis.sql`

**Azure services involved**
- Streamlit (local runtime)
- Azure Synapse Serverless SQL (historical)
- Databricks SQL serverless Warehouse (live)

**Implementation tasks**
1. Define live sensor KPI queries over live Silver (Databricks SQL).
2. Implement Streamlit sections (final set refined during implementation):
   Plantation overview, Harvest, Revenue/Costs, Fertilizer, Equipment,
   Live Sensors (Temperature, Humidity, Soil Moisture, Sensor Status).
3. Wire historical sections → Synapse Serverless; live sections → Databricks
   SQL.
4. Use secure credential handling (env vars; no committed secrets).

**Dependencies**
- Phase 6 complete (Synapse serving) and Phase 7 complete (live Silver data).

**Validation tasks**
- Dashboard loads; historical sections show Gold-derived data via Synapse.
- Live sections reflect newly simulated sensor data within a short delay.

**Completion criteria**
- Both paths verified working in the running dashboard.

**Evidence to record**
- Screenshots/recordings of historical and live sections; queries used.

**What NOT to implement yet**
- No Databricks Workflows orchestration (Phase 9).

---

## Phase 9 — Databricks Workflows

**Status:** NOT STARTED

**Objective**
Orchestrate the platform with Databricks Workflows: a batch workflow that
triggers ADF via REST, waits/polls, then runs Spark → DQ → Spark → Gold; and a
separate continuous streaming workflow.

**Files involved**
- `databricks/workflows/plantation_batch.json`
- `databricks/workflows/sensor_streaming.json`
- `databricks/orchestrator/trigger_adf.py`

**Azure services involved**
- Databricks Workflows
- Azure Data Factory (triggered via REST API)

**Implementation tasks**
1. Implement `trigger_adf.py` using the ADF REST API (trigger + poll to
   terminal state), with secure credentials (service principal / env / Key
   Vault — never hard-coded).
2. Define the batch workflow: trigger ADF → wait/poll → Spark Bronze→Silver →
   DQ gate → Spark Silver→Gold.
3. Define the separate streaming workflow running `sensors_stream.py`
   continuously.
4. Ensure critical DQ failure stops the batch workflow before Gold.

**Dependencies**
- Phases 2–5 complete (batch components exist) and Phase 7 complete
  (streaming job exists).

**Validation tasks**
- Run the batch workflow end-to-end; verify task ordering, ADF trigger +
  polling, and DQ gating behavior.
- Verify the streaming workflow runs continuously and independently of batch.

**Completion criteria**
- Verified end-to-end orchestrated batch run; separate verified streaming
  workflow run.

**Evidence to record**
- Workflow run IDs, task timelines, ADF run triggered via REST (run ID),
  streaming job run ID.

**What NOT to implement yet**
- Phase 10 polish items (final docs/demo assets).

---

## Phase 10 — Testing + Documentation + Demo

**Status:** NOT STARTED

**Objective**
Harden and present the project: complete tests, finalize documentation, and
prepare the portfolio demo narrative.

**Files involved**
- `tests/` (all test files)
- `docs/pipeline_design.md`, `docs/data_dictionary.md`,
  `docs/deployment.md`, `docs/troubleshooting.md`
- `README.md` (final project overview — its owning phase)
- `AGENTS.md`, `ARCHITECTURE.md`, `IMPLEMENTATION_PLAN.md` (final consistency
  pass)

**Azure services involved**
- All previously used services (verification/demo runs only — no new
  services).

**Implementation tasks**
1. Complete and run the test suite (generators, schemas, transformations,
   data quality).
2. Finalize docs: pipeline design, data dictionary (from **actual** final
   schemas), deployment steps, troubleshooting.
3. Run a full verified end-to-end demo (batch + streaming + dashboard) and
   capture evidence.
4. Final consistency check across the three control documents.

**Dependencies**
- Phases 0–9 complete.

**Validation tasks**
- All tests pass (verified output).
- Docs match the implemented reality (no planned-vs-built contradictions).

**Completion criteria**
- Green test suite; docs accurate; demo evidence captured; control documents
  consistent with the built system.

**Evidence to record**
- Test output; doc links; demo screenshots/recordings; final verified run
  IDs.

**What NOT to implement yet**
- No new features, no new Azure services, no excluded technologies. This phase
  is hardening and presentation only.

---

**Next action:**
Phase 8 — Databricks SQL + Streamlit. Phase 7 is complete: the live sensor
streaming path (sensor simulator → ADLS Incoming → Auto Loader → Structured
Streaming → live Bronze Delta → live Silver Delta) is implemented and verified
on Azure Databricks Serverless with checkpoints on ADLS — final live counts
live Bronze **140** / live Silver **140** (initial 56-row run, checkpoint-based
restart/re-upload protection with 0 duplicate keys, then a clean +56
incremental run to 140; see Phase 7 Evidence). Phase 8 builds the Streamlit
dashboard with two data paths: **historical via Synapse Serverless SQL**
(Gold) and **live via Databricks SQL** (live Silver), served by the one shared
serverless SQL Warehouse (`dashboard/app.py`,
`databricks/sql/live_sensor_kpis.sql`).
