-- ============================================================================
-- Phase 6 — Synapse Historical Serving
-- File: synapse/sql/plantation_views.sql
--
-- Serving views for the dashboard's historical sections. One view per Phase 5
-- Gold dataset, projecting the full Gold column set from the base external
-- objects (gold.ext_<model>) with no transformation and no change to Gold
-- data semantics.
--
-- Run this script SECOND, after synapse/sql/external_tables.sql has created
-- the plantation_gold database, the GoldAdls data source, the gold schema,
-- and the six gold.ext_<model> base views.
-- Execute against: plantation-simulator-synapse.sql.azuresynapse.net
-- (built-in serverless SQL endpoint), database plantation_gold.
-- ============================================================================

USE plantation_gold;
GO

-- ----------------------------------------------------------------------------
-- Dimensions
-- ----------------------------------------------------------------------------

CREATE OR ALTER VIEW gold.vw_dim_equipment
AS
SELECT
    equipment_id,
    equipment_type
FROM gold.ext_dim_equipment;
GO

CREATE OR ALTER VIEW gold.vw_dim_employee
AS
SELECT
    employee_id,
    employee_name,
    role,
    department,
    cost_center_id
FROM gold.ext_dim_employee;
GO

-- ----------------------------------------------------------------------------
-- Facts
-- ----------------------------------------------------------------------------

CREATE OR ALTER VIEW gold.vw_fact_harvest
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
FROM gold.ext_fact_harvest;
GO

CREATE OR ALTER VIEW gold.vw_fact_revenue
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
FROM gold.ext_fact_revenue;
GO

CREATE OR ALTER VIEW gold.vw_fact_fertilizer
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
FROM gold.ext_fact_fertilizer;
GO

CREATE OR ALTER VIEW gold.vw_fact_equipment
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
FROM gold.ext_fact_equipment;
GO
