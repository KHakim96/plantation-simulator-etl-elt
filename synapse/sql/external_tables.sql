-- ============================================================================
-- Phase 6 — Synapse Historical Serving
-- File: synapse/sql/external_tables.sql
--
-- Setup script for the Synapse Serverless SQL historical serving layer over
-- the Phase 5 Gold Delta datasets on ADLS Gen2.
--
-- Creates, in a dedicated custom database on the BUILT-IN SERVERLESS SQL
-- endpoint (no dedicated pool):
--   * database            plantation_gold  (UTF-8 collation required for Delta)
--   * database scoped credential  SynapseIdentity  (workspace system-assigned
--                         managed identity — NO storage key, NO SAS, NO PAT,
--                         NO secret)
--   * external data source GoldAdls          (ADLS Gen2 gold container)
--   * schema              gold
--   * six base views      gold.ext_<model>   over OPENROWSET(... FORMAT='DELTA')
--
-- Run this script FIRST, then synapse/sql/plantation_views.sql.
-- Execute against: plantation-simulator-synapse.sql.azuresynapse.net
-- (built-in serverless SQL endpoint). Statements are split across the master
-- and plantation_gold databases where required by Synapse serverless.
--
-- Verified prerequisite (real Azure): the workspace managed identity
-- (object id 17a2ec83-6a05-423a-9d46-45dac4f2a26d) holds Storage Blob Data
-- Contributor on storage account plantationsimulatorrg, and
-- OPENROWSET(... FORMAT='DELTA') over the gold container returned real rows.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. Database (serverless external objects/data sources cannot be created in
--    master). Delta strings are UTF-8, so the database must use a UTF-8
--    collation or string reads fail with conversion errors.
-- ----------------------------------------------------------------------------
USE master;
GO

IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = 'plantation_gold')
BEGIN
    CREATE DATABASE plantation_gold COLLATE Latin1_General_100_BIN2_UTF8;
END;
GO

USE plantation_gold;
GO

-- ----------------------------------------------------------------------------
-- 2. Database scoped credential — workspace system-assigned managed identity.
--    This introduces NO key, SAS token, PAT, or secret into the repository.
-- ----------------------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.database_scoped_credentials WHERE name = 'SynapseIdentity')
BEGIN
    CREATE DATABASE SCOPED CREDENTIAL SynapseIdentity
    WITH IDENTITY = 'Managed Identity';
END;
GO

-- ----------------------------------------------------------------------------
-- 3. External data source — ADLS Gen2 gold container root, using the managed
--    identity credential. Base views use relative locations from this root.
-- ----------------------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.external_data_sources WHERE name = 'GoldAdls')
BEGIN
    CREATE EXTERNAL DATA SOURCE GoldAdls
    WITH (
        LOCATION   = 'https://plantationsimulatorrg.dfs.core.windows.net/gold',
        CREDENTIAL = SynapseIdentity
    );
END;
GO

-- ----------------------------------------------------------------------------
-- 4. Schema for the serving objects.
-- ----------------------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold')
BEGIN
    EXEC('CREATE SCHEMA gold');
END;
GO

-- ----------------------------------------------------------------------------
-- 5. Base external objects over the six Phase 5 Gold Delta datasets.
--
--    Each base view reads one Gold Delta folder via OPENROWSET FORMAT='DELTA'
--    relative to the GoldAdls data source. Column lists and types are derived
--    exactly from the verified Phase 5 Gold model definitions
--    (databricks/batch/silver_to_gold.py + Silver Delta schemas):
--      Spark string            -> varchar(...) COLLATE Latin1_General_100_BIN2_UTF8
--      Spark timestamp         -> datetime2
--      Spark date              -> date
--      Spark double            -> float
--      Spark integer           -> int
--      Spark decimal(18,2)     -> decimal(18,2)
--      Spark boolean           -> bit
-- ----------------------------------------------------------------------------

CREATE OR ALTER VIEW gold.ext_dim_equipment
AS
SELECT
    equipment_id,
    equipment_type
FROM OPENROWSET(
        BULK 'dim_equipment/',
        DATA_SOURCE = 'GoldAdls',
        FORMAT      = 'DELTA'
     )
     WITH (
        equipment_id   varchar(50)  COLLATE Latin1_General_100_BIN2_UTF8,
        equipment_type varchar(100) COLLATE Latin1_General_100_BIN2_UTF8
     ) AS r;
GO

CREATE OR ALTER VIEW gold.ext_dim_employee
AS
SELECT
    employee_id,
    employee_name,
    role,
    department,
    cost_center_id
FROM OPENROWSET(
        BULK 'dim_employee/',
        DATA_SOURCE = 'GoldAdls',
        FORMAT      = 'DELTA'
     )
     WITH (
        employee_id    varchar(50)  COLLATE Latin1_General_100_BIN2_UTF8,
        employee_name  varchar(200) COLLATE Latin1_General_100_BIN2_UTF8,
        role           varchar(100) COLLATE Latin1_General_100_BIN2_UTF8,
        department     varchar(100) COLLATE Latin1_General_100_BIN2_UTF8,
        cost_center_id varchar(50)  COLLATE Latin1_General_100_BIN2_UTF8
     ) AS r;
GO

CREATE OR ALTER VIEW gold.ext_fact_harvest
AS
SELECT
    harvest_id,
    harvest_date,
    harvest_timestamp,
    block_id,
    crop_type,
    employee_id,
    equipment_id,
    harvested_weight_kg,
    quality_grade,
    moisture_pct,
    collection_duration_minutes,
    destination,
    status
FROM OPENROWSET(
        BULK 'fact_harvest/',
        DATA_SOURCE = 'GoldAdls',
        FORMAT      = 'DELTA'
     )
     WITH (
        harvest_id                 varchar(50)  COLLATE Latin1_General_100_BIN2_UTF8,
        harvest_date               date,
        harvest_timestamp          datetime2,
        block_id                   varchar(50)  COLLATE Latin1_General_100_BIN2_UTF8,
        crop_type                  varchar(100) COLLATE Latin1_General_100_BIN2_UTF8,
        employee_id                varchar(50)  COLLATE Latin1_General_100_BIN2_UTF8,
        equipment_id               varchar(50)  COLLATE Latin1_General_100_BIN2_UTF8,
        harvested_weight_kg        float,
        quality_grade              varchar(20)  COLLATE Latin1_General_100_BIN2_UTF8,
        moisture_pct               float,
        collection_duration_minutes int,
        destination                varchar(100) COLLATE Latin1_General_100_BIN2_UTF8,
        status                     varchar(50)  COLLATE Latin1_General_100_BIN2_UTF8
     ) AS r;
GO

CREATE OR ALTER VIEW gold.ext_fact_revenue
AS
SELECT
    document_id,
    posting_date,
    fiscal_year,
    fiscal_period,
    company_code,
    cost_center_id,
    gl_account,
    transaction_type,
    reference_document,
    employee_id,
    equipment_id,
    material_id,
    amount,
    currency,
    debit_credit_indicator,
    description
FROM OPENROWSET(
        BULK 'fact_revenue/',
        DATA_SOURCE = 'GoldAdls',
        FORMAT      = 'DELTA'
     )
     WITH (
        document_id            varchar(50)   COLLATE Latin1_General_100_BIN2_UTF8,
        posting_date           date,
        fiscal_year            int,
        fiscal_period          int,
        company_code           varchar(20)   COLLATE Latin1_General_100_BIN2_UTF8,
        cost_center_id         varchar(50)   COLLATE Latin1_General_100_BIN2_UTF8,
        gl_account             varchar(20)   COLLATE Latin1_General_100_BIN2_UTF8,
        transaction_type       varchar(100)  COLLATE Latin1_General_100_BIN2_UTF8,
        reference_document     varchar(100)  COLLATE Latin1_General_100_BIN2_UTF8,
        employee_id            varchar(50)   COLLATE Latin1_General_100_BIN2_UTF8,
        equipment_id           varchar(50)   COLLATE Latin1_General_100_BIN2_UTF8,
        material_id            varchar(50)   COLLATE Latin1_General_100_BIN2_UTF8,
        amount                 decimal(18,2),
        currency               varchar(10)   COLLATE Latin1_General_100_BIN2_UTF8,
        debit_credit_indicator varchar(5)    COLLATE Latin1_General_100_BIN2_UTF8,
        description            varchar(500)  COLLATE Latin1_General_100_BIN2_UTF8
     ) AS r;
GO

CREATE OR ALTER VIEW gold.ext_fact_fertilizer
AS
SELECT
    application_id,
    application_date,
    application_timestamp,
    block_id,
    crop_type,
    employee_id,
    material_id,
    quantity_kg,
    application_method,
    equipment_id,
    weather_station_id,
    rainfall_mm,
    application_status
FROM OPENROWSET(
        BULK 'fact_fertilizer/',
        DATA_SOURCE = 'GoldAdls',
        FORMAT      = 'DELTA'
     )
     WITH (
        application_id        varchar(50)  COLLATE Latin1_General_100_BIN2_UTF8,
        application_date      date,
        application_timestamp datetime2,
        block_id              varchar(50)  COLLATE Latin1_General_100_BIN2_UTF8,
        crop_type             varchar(100) COLLATE Latin1_General_100_BIN2_UTF8,
        employee_id           varchar(50)  COLLATE Latin1_General_100_BIN2_UTF8,
        material_id           varchar(50)  COLLATE Latin1_General_100_BIN2_UTF8,
        quantity_kg           float,
        application_method    varchar(100) COLLATE Latin1_General_100_BIN2_UTF8,
        equipment_id          varchar(50)  COLLATE Latin1_General_100_BIN2_UTF8,
        weather_station_id    varchar(50)  COLLATE Latin1_General_100_BIN2_UTF8,
        rainfall_mm           float,
        application_status    varchar(50)  COLLATE Latin1_General_100_BIN2_UTF8
     ) AS r;
GO

CREATE OR ALTER VIEW gold.ext_fact_equipment
AS
SELECT
    operation_id,
    operation_date,
    operation_timestamp,
    equipment_id,
    equipment_type,
    block_id,
    operator_id,
    operation_type,
    duration_minutes,
    engine_hours,
    fuel_consumption_liters,
    distance_km,
    maintenance_flag,
    maintenance_type,
    status
FROM OPENROWSET(
        BULK 'fact_equipment/',
        DATA_SOURCE = 'GoldAdls',
        FORMAT      = 'DELTA'
     )
     WITH (
        operation_id            varchar(50)  COLLATE Latin1_General_100_BIN2_UTF8,
        operation_date          date,
        operation_timestamp     datetime2,
        equipment_id            varchar(50)  COLLATE Latin1_General_100_BIN2_UTF8,
        equipment_type          varchar(100) COLLATE Latin1_General_100_BIN2_UTF8,
        block_id                varchar(50)  COLLATE Latin1_General_100_BIN2_UTF8,
        operator_id             varchar(50)  COLLATE Latin1_General_100_BIN2_UTF8,
        operation_type          varchar(100) COLLATE Latin1_General_100_BIN2_UTF8,
        duration_minutes        int,
        engine_hours            float,
        fuel_consumption_liters float,
        distance_km             float,
        maintenance_flag        bit,
        maintenance_type        varchar(100) COLLATE Latin1_General_100_BIN2_UTF8,
        status                  varchar(50)  COLLATE Latin1_General_100_BIN2_UTF8
     ) AS r;
GO
