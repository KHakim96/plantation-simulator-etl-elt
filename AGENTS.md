# AGENTS.md — Agent Operating Instructions

Project: **plantation-simulator-etl-elt**
Type: Azure Data Engineering portfolio project (ETL/ELT, batch + streaming)

---

## 1. Project Context

This is a **new project built from scratch**. The repository started as an
empty scaffold; Phase 0 (Azure foundation) and Phase 1 (source simulation +
ADLS Landing delivery) are now **implemented and verified**. Later-phase
artifacts (ADF pipelines, Spark jobs, dbt models, Synapse views, streaming,
dashboard, workflows) remain placeholder/empty files and must still be treated
as **planned, not implemented**.

The project demonstrates an Azure-based plantation analytics platform using:

- Simulated external data sources (Python generators)
- Azure Data Lake Storage Gen2 (ADLS Gen2)
- Azure Data Factory (ADF)
- Azure Databricks + Apache Spark
- Delta Lake (Bronze / Silver / Gold)
- Data Quality gates
- dbt-databricks
- Databricks SQL (one shared serverless SQL Warehouse)
- Azure Synapse Serverless SQL
- Auto Loader + Structured Streaming (near-real-time sensors)
- Databricks Workflows (batch orchestration)
- Streamlit (dashboard)

---

## 2. Control Documents

The following three files are the **project control documents**. They govern all
work on this repository:

| Document | Role |
|---|---|
| `AGENTS.md` | Operating rules for any agent (human or AI) working on this repo |
| `ARCHITECTURE.md` | The **frozen** target architecture. Single source of truth for design |
| `IMPLEMENTATION_PLAN.md` | The phased roadmap and the current implementation status |

Rules:

1. `ARCHITECTURE.md` defines the **frozen** architecture. It is the authority on
   what gets built.
2. `IMPLEMENTATION_PLAN.md` defines the **current phase** and what is / is not
   allowed to be built yet.
3. If a control document and your instinct disagree, the control document wins —
   unless the human explicitly approves a change.
4. Keep these documents accurate. When a phase is completed, update
   `IMPLEMENTATION_PLAN.md` (status + evidence) in the same change set.

---

## 3. Phased Execution

1. Work **one phase at a time**, in the order defined in
   `IMPLEMENTATION_PLAN.md` (Phase 0 → Phase 10).
2. Do **not** start a phase before its dependencies are complete.
3. Do **not** implement work belonging to a later phase ("no future-phase
   leakage"). Each phase lists "What NOT to implement yet" — respect it.
4. Do not mark a phase complete until its **completion criteria** are met and
   **evidence** has been recorded.
5. After completing a phase, update `IMPLEMENTATION_PLAN.md`:
   - mark the phase complete,
   - record the evidence gathered,
   - advance "Current phase" and "Next action".

---

## 4. Architecture Discipline

1. **Do not redesign the architecture** without explicit human approval. The
   architecture is FINAL / FROZEN (see `ARCHITECTURE.md`).
2. **Do not introduce excluded technologies.** The following are intentionally
   excluded and must never appear as implementation requirements:
   - Kafka
   - Azure Event Hubs
   - Azure IoT Hub
   - Airflow
   - Kubernetes
   - Microsoft Fabric
   - Power BI
   - Dedicated Synapse SQL pool (only Serverless SQL is used)
3. Respect service responsibilities exactly as defined in `ARCHITECTURE.md`. In
   particular:
   - **ADF = batch ingestion only** (Landing → Bronze, Copy Activity). ADF is
     **not** a "CSV → Delta transformation" engine; Databricks/Spark owns
     transformation.
   - **dbt owns Silver → Gold.** Do not duplicate Spark transformation logic in
     dbt unnecessarily.
   - **Streaming does NOT go through ADF, dbt, Gold, or Synapse.** This
     separation is intentional.
   - Use **ONE shared serverless Databricks SQL Warehouse** for both dbt
     execution and live sensor serving. Do not create separate warehouses.
4. Keep the implementation practical and achievable as a **one-day portfolio
   project**. Prefer simple, working solutions over elaborate ones.

---

## 5. Anti-Hallucination Rules (MANDATORY)

These rules exist to prevent fabricated progress. Follow them without exception:

1. **Inspect the actual project files before changing them.** Never assume file
   contents — read them first.
2. **Inspect actual Azure state before claiming resources exist.** Use the Azure
   portal, `az` CLI, or SDKs to verify. A resource listed in a document is a
   *plan*, not proof of existence.
3. **Verify actual pipeline/job execution.** Do not claim an ADF pipeline ran, a
   Databricks job succeeded, or a stream is live without observing the run and
   its output.
4. **Do not invent configuration values** (paths, URLs, endpoint names, schema
   names, warehouse sizes, etc.). Derive them from the real environment or mark
   them PENDING.
5. **Do not invent credentials** and never commit secrets. Use `.env.example` /
   environment variables / Azure Key Vault patterns. No secrets in code,
   notebooks, JSON, or Git history.
6. **Do not invent schemas.** Inspect the actual generated source data first,
   then define schemas/models from what is really there.
7. **Do not assume an Azure feature works without verification.** Check behavior
   in the actual subscription (e.g., serverless SQL access to Delta on ADLS,
   Auto Loader directory listing, REST API responses).
8. **Distinguish planned architecture from implemented architecture.** The
   diagram is the target; only what is verified running may be described as
   "implemented".
9. **Mark unverified items as PENDING** in plans and status updates.
10. **Never claim something works unless verified.** "It should work" is not a
    status.
11. **Never fabricate** Azure resources, pipeline runs, job runs, tables, rows,
    credentials, or results — in code, docs, commit messages, or chat.
12. **Record evidence** when a phase is completed (see §8).

---

## 6. File & Change Discipline

1. Do **not** modify unrelated files. Touch only what the current phase requires.
2. The scaffold files (e.g., `README.md`, `requirements.txt`, `.gitignore`,
   `.env.example`, placeholder generators, placeholder notebooks/scripts) must
   not be modified outside the phase that owns them.
3. Streaming **checkpoints live in ADLS Gen2** and must **never be committed to
   Git**. Local `data/` landing/bronze/silver/gold folders are simulation
   artifacts — do not commit real data or checkpoints.
4. Commit in small, reviewable units tied to a phase task. Never commit secrets.
5. Before committing: review `git status` and `git diff` to ensure only intended
   files are staged.

---

## 7. Azure Cost & Trial Constraints

1. This project runs under **Azure trial / cost-sensitive constraints**. Treat
   every resource as billable.
2. Prefer **serverless / pay-per-use** options:
   - Databricks SQL: **one shared serverless** SQL Warehouse (auto-stop on).
   - Synapse: **Serverless SQL only** (pay per query). No dedicated pool.
3. Pause/stop clusters and warehouses when not in use. Delete or deallocate
   resources that are no longer needed.
4. Keep data volumes small (simulated data). Do not generate or process large
   datasets.
5. Verify cost-impacting assumptions (e.g., per-query cost of Synapse queries)
   rather than assuming they are free.

---

## 8. Verification & Evidence

When a phase finishes, record evidence in `IMPLEMENTATION_PLAN.md`, such as:

- Azure resource confirmations (portal/CLI output, resource names, regions).
- ADF pipeline run IDs and status.
- Databricks job/cluster run IDs and status.
- Counts of rows/files written per layer (Landing, Bronze, Silver, Gold).
- Data Quality check results (pass/fail, checks executed).
- dbt run/test results.
- Synapse query results proving Gold is servable.
- Streaming proof (checkpoint location on ADLS, live rows visible via
  Databricks SQL).
- Screenshots or exported logs where useful.

Evidence must come from the **real environment**, not from memory or
expectation.

---

## 9. Definition of Done (per task)

A task is only "done" when:

1. The change is implemented in the real files/environment.
2. It was **verified** against the real environment (not assumed).
3. It does not violate the frozen architecture or the excluded-services list.
4. No secrets were introduced; no unrelated files were modified.
5. Evidence is recorded and `IMPLEMENTATION_PLAN.md` is updated.

---

## 10. Quick Reference — Flow Ownership

**Batch:**
Simulated sources → Python generators → `upload_to_adls.py` → ADLS **LANDING**
→ **ADF** (batch ingestion) → ADLS **BRONZE** (Delta) → **Databricks Spark** →
ADLS **SILVER** (Delta) → **DQ checks** (gate) → **dbt-databricks** →
**Databricks SQL Warehouse** → ADLS **GOLD** (Delta) → **Synapse Serverless SQL**
→ **Streamlit**

**Streaming:**
Live sensor simulator → ADLS **INCOMING** → **Auto Loader** →
**Structured Streaming** → **Live Bronze Delta** → **Live Silver Delta** →
**Databricks SQL** → **Streamlit**

(Streaming bypasses ADF, dbt, Gold, and Synapse — intentionally.)

**Orchestration:** Databricks Workflows → trigger ADF (via ADF REST API) →
poll/wait → Spark → DQ → dbt → Gold.
