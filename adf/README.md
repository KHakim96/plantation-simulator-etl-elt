# ADF — Phase 2: Landing → Bronze (file copy, CSV → CSV)

Deployment and operations guide for the parameterized Copy pipeline
`PL_Ingest_Landing_To_Bronze`, which copies the six Phase 1 Landing CSV files
into the ADLS Gen2 `bronze` container **as files**
(`bronze/<source>/<file>.csv`).

> **Revised design (approved).** Phase 2 was originally authored with a
> Databricks Delta sink (`AzureDatabricksDeltaLake` connector + interactive
> cluster + PAT + one-time Delta DDL). That approach is **superseded**: the
> workspace could not provision the required worker capacity, and Bronze in
> Phase 2 is now the **raw file-based ingestion layer**. ADF is
> **ingestion-only**; Delta/Spark processing is Phase 3 scope. The superseded
> artifacts (`LS_Databricks_Bronze`, `DS_Bronze_Delta`,
> `databricks/sql/01_create_bronze_tables.sql`) were removed from this repo and
> deleted from the live factory.

## Design facts (verified live)

- **One pipeline, one Copy activity per source.** `PL_Ingest_Landing_To_Bronze`
  iterates a ForEach (sequential) over the six known
  `{sourceFolder, sourceFile}` items. Both datasets are parameterized
  `DelimitedText` on the single ADLS Gen2 linked service.
- **Authentication is the ADF managed identity.** `LS_Adls_PlantationSimulator`
  carries only the account URL (no key, no SAS, no secret in the repo). The
  factory's system-assigned identity is granted **Storage Blob Data
  Contributor** on the storage account (Reader alone cannot write Bronze).
- **Deterministic reruns.** Sink `copyBehavior: Overwrite` with an explicit
  sink file name means every run rewrites `bronze/<source>/<file>.csv` to be
  exactly one copy of the Landing file — no duplicates, no growth. Proven:
  two consecutive successful runs produced identical Bronze state.
- **Landing is never touched.** `deleteFilesAfterCompletion: false` on the
  source; the pipeline only reads Landing.
- **No `quoteAllText` in the Copy sink.** Real ADF error
  `DelimitedTextInvalidSettings: QuoteAllText cannot set to false for Copy
  activity currently` (run `99a9bbda-9e76-11f1-b171-86283166b020`, cancelled).
  The fix was simply to remove the setting; ADF's default CSV quoting applies.
- **No `_delta_log` anywhere.** Phase 2 writes plain CSV files; Delta is
  deferred to Phase 3 by design.

## Artifacts in this folder

| File | ADF object | Notes |
|---|---|---|
| `linkedService/LS_Adls_PlantationSimulator.json` | Linked service (AzureBlobFS) | URL only; auth = ADF managed identity via RBAC |
| `dataset/DS_Landing_Source.json` | Dataset (DelimitedText) | Parameterized `SourceContainer` (default `landing`) / `SourceFolder`; wildcard file name is set in the pipeline (`@item().sourceFile`) |
| `dataset/DS_Bronze_Sink.json` | Dataset (DelimitedText) | Parameterized `SinkContainer` (default `bronze`) / `SinkFolder` / `SinkFileName` |
| `pipeline/PL_Ingest_Landing_To_Bronze.json` | Pipeline | ForEach (sequential) over 6 items; Copy per source, `copyBehavior: Overwrite` |
| `scripts/verify_bronze.py` | — | Read-only post-run verification (see below) |

## Azure resources used (existing — nothing created in Phase 2)

- Subscription `afec86b2-072d-4bdb-83a9-4fe370a3a0fc`, resource group
  `plantation-simulator-rg`, tenant `28c1b3c5-e2c0-4c43-9eef-8b0b7c09cf16`.
- Storage account `plantationsimulatorrg` (ADLS Gen2, HNS enabled).
- Data Factory `plantation-simulator-adf` (Southeast Asia).
- Databricks workspace `plantation-simulator-dbx` — **NOT used in Phase 2**.

## Step-by-step deployment (what was actually done)

Prerequisites: `az` CLI authenticated
(`az login`, tenant `28c1b3c5-e2c0-4c43-9eef-8b0b7c09cf16`), with the
`datafactory` extension (`az extension add --name datafactory`).

1. **RBAC (one-time):** grant the ADF managed identity write access to the
   storage account (it only had Storage Blob Data *Reader*):

   ```bash
   PRINCIPAL_ID=$(az datafactory show -g plantation-simulator-rg \
     --name plantation-simulator-adf --query identity.principalId -o tsv)
   az role assignment create --assignee-object-id "$PRINCIPAL_ID" \
     --assignee-principal-type ServicePrincipal \
     --role "Storage Blob Data Contributor" \
     --scope "/subscriptions/afec86b2-072d-4bdb-83a9-4fe370a3a0fc/resourceGroups/plantation-simulator-rg/providers/Microsoft.Storage/storageAccounts/plantationsimulatorrg"
   ```

2. **Deploy the objects** (create-or-update; reuses the existing factory and
   the existing `LS_Adls_PlantationSimulator`):

   ```bash
   RG=plantation-simulator-rg FACTORY=plantation-simulator-adf

   az datafactory dataset create -g $RG --factory-name $FACTORY --name DS_Landing_Source \
     --properties "$(jq '.properties' adf/dataset/DS_Landing_Source.json)"

   az datafactory dataset create -g $RG --factory-name $FACTORY --name DS_Bronze_Sink \
     --properties "$(jq '.properties' adf/dataset/DS_Bronze_Sink.json)"

   az datafactory pipeline create -g $RG --factory-name $FACTORY --name PL_Ingest_Landing_To_Bronze \
     --pipeline "$(jq '.properties' adf/pipeline/PL_Ingest_Landing_To_Bronze.json)"
   ```

   (`az datafactory linked-service create ...` for
   `LS_Adls_PlantationSimulator` only if it does not already exist.)

3. **Trigger and capture the run ID:**

   ```bash
   RUN_ID=$(az datafactory pipeline create-run -g $RG --factory-name $FACTORY \
     --name PL_Ingest_Landing_To_Bronze --query runId -o tsv)

   az datafactory pipeline-run show -g $RG --factory-name $FACTORY --run-id $RUN_ID \
     --query "{status:status,start:runStart,end:runEnd}"
   ```

   Typical duration ≈ 2–2.5 min (six sequential copies, ~6.9 MB total).

4. **Verify Bronze independently of ADF:**

   ```bash
   .venv/bin/python adf/scripts/verify_bronze.py
   ```

   Read-only; checks that:
   1. Landing is still intact (one CSV per source, exact known-good byte
      sizes, 48,595 data rows total);
   2. each `bronze/<source>/<file>.csv` exists with the expected data-row
      count, equal to the live Landing file;
   3. Bronze contains exactly the six expected source folders and **no
      `_delta_log` directories**;
   4. silver / gold / live-bronze / live-silver / checkpoints / incoming are
      all empty (Phase 2 writes nothing downstream).

   Exit code 0 = all pass.

## Real run history (evidence)

| Run ID | Status | Notes |
|---|---|---|
| `99a9bbda-9e76-11f1-b171-86283166b020` | Cancelled | First trigger; failed per-activity with `DelimitedTextInvalidSettings` (`quoteAllText: false` not supported in Copy), cancelled after diagnosis |
| `2df2fdb4-9e78-11f1-a07b-86283166b020` | **Succeeded** | 2026-08-22T22:24:03Z → 22:26:13Z (130s); 6 files copied, byte counts match Landing exactly |
| `9d31b9a4-9e78-11f1-8fb5-86283166b020` | **Succeeded** | Idempotency rerun; Bronze re-verified identical afterwards |

Post-run verification: `verify_bronze.py` exit 0 — 6 Bronze files,
48,595 rows, and MD5 of every Bronze file identical to its Landing source.

## Re-run semantics

- Safe to re-run any time: each run overwrites the six Bronze CSVs to exactly
  mirror Landing (verified — two consecutive runs, identical Bronze state).
- Landing files are never modified or deleted by the pipeline.
- The pipeline is manual-trigger only (no schedule/tumbling-window trigger in
  Phase 2).
