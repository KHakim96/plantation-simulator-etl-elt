-- ============================================================================
-- Phase 8 — Live Sensor Serving
-- File: databricks/sql/live_sensor_kpis.sql
--
-- Exposes the Phase 7 live Silver Delta sensor stream to the ONE shared
-- serverless Databricks SQL Warehouse and defines the live sensor KPI layer
-- consumed by the Streamlit dashboard (dashboard/app.py).
--
-- Live Silver source (Phase 7 output, verified 140 rows):
--   abfss://live-silver@plantationsimulatorrg.dfs.core.windows.net/sensors
--
-- Authentication: Unity Catalog. The table below is an EXTERNAL (unmanaged)
-- Delta table whose LOCATION is the existing live Silver ADLS path. Access to
-- that path is governed by the existing Unity Catalog external location over
-- the live-silver container (storage credential `plantation_external_adls`).
-- This script configures NO storage account key, NO SAS token, NO PAT, and
-- never sets fs.azure.account.key.*.
--
-- Serverless-compatible: no PERSIST/CACHE TABLE, no temp views.
-- Idempotent: CREATE SCHEMA IF NOT EXISTS, CREATE TABLE IF NOT EXISTS, and
-- CREATE OR REPLACE VIEW — safe to rerun.
--
-- Run against the existing shared serverless SQL Warehouse:
--   "Serverless Starter Warehouse" (id 7d27a516598723a3).
-- Do NOT create another warehouse.
--
-- All objects are fully qualified with the verified Unity Catalog
-- `plantation_simulator_dbx` (schema `live_serving`), so this script does not
-- depend on any implicit or default catalog context.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. Schema (Unity Catalog). Fully qualified with the verified workspace
--    catalog `plantation_simulator_dbx` so nothing depends on an implicit or
--    default catalog context.
-- ----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS plantation_simulator_dbx.live_serving
COMMENT 'Phase 8 live sensor serving objects over the Phase 7 live Silver Delta';

-- ----------------------------------------------------------------------------
-- 2. External table over the live Silver Delta path.
--
--    This is an UNMANAGED/external UC table: it registers the existing Delta
--    files at the live-silver ADLS path WITHOUT copying or rewriting them.
--    The data stays owned by the Phase 7 streaming job; this table only makes
--    it queryable from the SQL Warehouse.
--
--    Live Silver columns (from Phase 7 transform_live_silver):
--      timestamp, block_id, sensor_id,
--      soil_moisture_pct, soil_temperature_c, air_temperature_c,
--      humidity_pct, soil_ph, light_intensity_lux, battery_level_pct,
--      sensor_status, _ingested_at
--    Business key: (sensor_id, timestamp)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS plantation_simulator_dbx.live_serving.live_silver_sensors
USING DELTA
LOCATION 'abfss://live-silver@plantationsimulatorrg.dfs.core.windows.net/sensors';

-- ----------------------------------------------------------------------------
-- 3. KPI view — Temperature (air + soil).
--    One row per sensor reading with the temperature measures projected.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW plantation_simulator_dbx.live_serving.vw_kpi_temperature
AS
SELECT
    block_id,
    sensor_id,
    timestamp            AS reading_ts,
    air_temperature_c,
    soil_temperature_c
FROM plantation_simulator_dbx.live_serving.live_silver_sensors;

-- ----------------------------------------------------------------------------
-- 4. KPI view — Humidity.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW plantation_simulator_dbx.live_serving.vw_kpi_humidity
AS
SELECT
    block_id,
    sensor_id,
    timestamp            AS reading_ts,
    humidity_pct
FROM plantation_simulator_dbx.live_serving.live_silver_sensors;

-- ----------------------------------------------------------------------------
-- 5. KPI view — Soil Moisture.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW plantation_simulator_dbx.live_serving.vw_kpi_soil_moisture
AS
SELECT
    block_id,
    sensor_id,
    timestamp            AS reading_ts,
    soil_moisture_pct,
    soil_ph
FROM plantation_simulator_dbx.live_serving.live_silver_sensors;

-- ----------------------------------------------------------------------------
-- 6. KPI view — Sensor Status.
--    Per-sensor latest-status summary plus supporting measures, useful for the
--    dashboard's Sensor Status section (OK / ANOMALY / FAULT).
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW plantation_simulator_dbx.live_serving.vw_kpi_sensor_status
AS
SELECT
    block_id,
    sensor_id,
    MAX(timestamp)                                  AS last_reading_ts,
    COUNT(*)                                        AS reading_count,
    SUM(CASE WHEN sensor_status = 'OK'      THEN 1 ELSE 0 END) AS ok_count,
    SUM(CASE WHEN sensor_status = 'ANOMALY' THEN 1 ELSE 0 END) AS anomaly_count,
    SUM(CASE WHEN sensor_status = 'FAULT'   THEN 1 ELSE 0 END) AS fault_count,
    AVG(battery_level_pct)                          AS avg_battery_pct
FROM plantation_simulator_dbx.live_serving.live_silver_sensors
GROUP BY block_id, sensor_id;
