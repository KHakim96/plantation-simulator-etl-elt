"""Phase 8: Plantation Analytics Dashboard (Streamlit).

Two clearly separated serving paths over the verified Phase 0–7 platform:

  * HISTORICAL — Azure Synapse Serverless SQL (built-in endpoint, no dedicated
    pool) reading the Phase 6 Gold serving views (``gold.vw_*``) over the
    Phase 5 Gold Delta models.
  * LIVE — Databricks SQL on the ONE shared serverless SQL Warehouse reading
    the Phase 8 live sensor KPI layer (``plantation_simulator_dbx.live_serving.*``)
    over the Phase 7 live Silver Delta.

Credentials come from environment variables ONLY (loaded from the repo-root
``.env`` via python-dotenv; real environment variables take precedence). No
secret is hard-coded, logged, or committed. If a connection cannot be
configured, the dashboard renders an honest "not configured" notice instead of
fabricating data.

Run locally with:
    streamlit run dashboard/app.py
"""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import streamlit as st

# ---------------------------------------------------------------------------
# Configuration (environment variables only — no secrets in code)
# ---------------------------------------------------------------------------

# Historical (Synapse Serverless) source: the Phase 6 Gold serving views.
SYNAPSE_VIEWS = {
    "dim_equipment": "gold.vw_dim_equipment",
    "dim_employee": "gold.vw_dim_employee",
    "fact_harvest": "gold.vw_fact_harvest",
    "fact_revenue": "gold.vw_fact_revenue",
    "fact_fertilizer": "gold.vw_fact_fertilizer",
    "fact_equipment": "gold.vw_fact_equipment",
}

# Live (Databricks SQL) source: the Phase 8 KPI views over live Silver.
# Fully qualified with the verified Unity Catalog so the dashboard matches the
# corrected Phase 8 SQL and never depends on a default/implicit catalog.
LIVE_SCHEMA = "plantation_simulator_dbx.live_serving"
LIVE_TABLE = f"{LIVE_SCHEMA}.live_silver_sensors"
LIVE_VIEWS = {
    "temperature": f"{LIVE_SCHEMA}.vw_kpi_temperature",
    "humidity": f"{LIVE_SCHEMA}.vw_kpi_humidity",
    "soil_moisture": f"{LIVE_SCHEMA}.vw_kpi_soil_moisture",
    "sensor_status": f"{LIVE_SCHEMA}.vw_kpi_sensor_status",
}


def _load_env() -> None:
    """Load repo-root .env (python-dotenv); real env vars take precedence."""
    try:
        from dotenv import load_dotenv

        load_dotenv(Path(__file__).resolve().parent.parent / ".env", override=False)
    except Exception as exc:  # noqa: BLE001 - dotenv optional at runtime
        # Honest, secret-free note; the app still runs on real env vars.
        print(f"  [info] .env not loaded ({exc}); using real environment only.")


def _get(name: str) -> str:
    return os.getenv(name, "").strip()


def synapse_config() -> dict:
    """Historical connection config from env (empty strings if unset)."""
    return {
        "server": _get("SYNAPSE_SQL_SERVER"),
        "database": _get("SYNAPSE_SQL_DATABASE"),
        "username": _get("SYNAPSE_SQL_USERNAME"),
        "password": _get("SYNAPSE_SQL_PASSWORD"),
    }


def databricks_config() -> dict:
    """Live connection config from env (empty strings if unset)."""
    return {
        "server_hostname": _get("DATABRICKS_SQL_SERVER_HOSTNAME"),
        "http_path": _get("DATABRICKS_SQL_HTTP_PATH"),
        "access_token": _get("DATABRICKS_SQL_ACCESS_TOKEN"),
    }


def synapse_configured(cfg: dict) -> bool:
    return bool(cfg["server"] and cfg["database"] and cfg["username"] and cfg["password"])


def databricks_configured(cfg: dict) -> bool:
    return bool(cfg["server_hostname"] and cfg["http_path"] and cfg["access_token"])


# ---------------------------------------------------------------------------
# Query runners (lazy imports so the app imports cleanly without drivers)
# ---------------------------------------------------------------------------


def run_synapse_query(sql: str) -> pd.DataFrame:
    """Run a read-only query on Synapse Serverless via pyodbc."""
    import pyodbc  # lazy: only needed when a live connection is used

    cfg = synapse_config()
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={cfg['server']};DATABASE={cfg['database']};"
        f"UID={cfg['username']};PWD={cfg['password']};"
        "Encrypt=yes;TrustServerCertificate=no;"
    )
    with pyodbc.connect(conn_str, timeout=30) as conn:
        return pd.read_sql(sql, conn)


def run_databricks_query(sql: str) -> pd.DataFrame:
    """Run a read-only query on the Databricks SQL Warehouse."""
    from databricks import sql as dbsql  # lazy: databricks-sql-connector

    cfg = databricks_config()
    with dbsql.connect(
        server_hostname=cfg["server_hostname"],
        http_path=cfg["http_path"],
        access_token=cfg["access_token"],
    ) as conn, conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
        cols: list[str] = [d[0] for d in cur.description] if cur.description else []
    # pandas stub quirk: columns accepts a plain list at runtime.
    return pd.DataFrame(list(rows), columns=list(cols))  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# UI helpers
# ---------------------------------------------------------------------------


def _not_configured(path_label: str, missing_vars: list[str]) -> None:
    st.warning(
        f"{path_label} is not configured. Set these environment variables in "
        f"your .env to enable it: {', '.join(missing_vars)}. "
        "No data is shown because no live connection was made."
    )


def _query_or_error(df_fn, sql: str):
    try:
        return df_fn(sql), None
    except Exception as exc:  # noqa: BLE001 - surface honest connection errors
        return None, str(exc)


# ---------------------------------------------------------------------------
# HISTORICAL sections (Synapse Serverless -> Gold views)
# ---------------------------------------------------------------------------


def section_historical_overview() -> None:
    st.subheader("Plantation Overview")
    queries = {
        "Harvested weight by crop type": (
            f"SELECT crop_type, COUNT(*) AS harvest_rows, "
            f"SUM(harvested_weight_kg) AS total_weight_kg "
            f"FROM {SYNAPSE_VIEWS['fact_harvest']} "
            f"GROUP BY crop_type ORDER BY total_weight_kg DESC"
        ),
        "Equipment fleet": (
            f"SELECT equipment_type, COUNT(*) AS equipment_count "
            f"FROM {SYNAPSE_VIEWS['dim_equipment']} "
            f"GROUP BY equipment_type ORDER BY equipment_count DESC"
        ),
        "Employees by role": (
            f"SELECT role, COUNT(*) AS employee_count "
            f"FROM {SYNAPSE_VIEWS['dim_employee']} "
            f"GROUP BY role ORDER BY employee_count DESC"
        ),
    }
    for title, sql in queries.items():
        st.markdown(f"**{title}**")
        df, err = _query_or_error(run_synapse_query, sql)
        if err:
            st.error(err)
        else:
            st.dataframe(df, use_container_width=True)


def section_historical_harvest() -> None:
    st.subheader("Harvest")
    sql = (
        f"SELECT crop_type, quality_grade, status, "
        f"COUNT(*) AS harvest_rows, "
        f"SUM(harvested_weight_kg) AS total_weight_kg, "
        f"AVG(moisture_pct) AS avg_moisture_pct "
        f"FROM {SYNAPSE_VIEWS['fact_harvest']} "
        f"GROUP BY crop_type, quality_grade, status "
        f"ORDER BY total_weight_kg DESC"
    )
    df, err = _query_or_error(run_synapse_query, sql)
    if err:
        st.error(err)
    else:
        st.dataframe(df, use_container_width=True)


def section_historical_revenue() -> None:
    st.subheader("Revenue / Costs")
    sql = (
        f"SELECT transaction_type, gl_account, cost_center_id, "
        f"COUNT(*) AS line_count, SUM(amount) AS total_amount "
        f"FROM {SYNAPSE_VIEWS['fact_revenue']} "
        f"GROUP BY transaction_type, gl_account, cost_center_id "
        f"ORDER BY total_amount DESC"
    )
    df, err = _query_or_error(run_synapse_query, sql)
    if err:
        st.error(err)
    else:
        st.dataframe(df, use_container_width=True)


def section_historical_fertilizer() -> None:
    st.subheader("Fertilizer")
    sql = (
        f"SELECT material_id, crop_type, "
        f"COUNT(*) AS applications, SUM(quantity_kg) AS total_quantity_kg "
        f"FROM {SYNAPSE_VIEWS['fact_fertilizer']} "
        f"GROUP BY material_id, crop_type "
        f"ORDER BY total_quantity_kg DESC"
    )
    df, err = _query_or_error(run_synapse_query, sql)
    if err:
        st.error(err)
    else:
        st.dataframe(df, use_container_width=True)


def section_historical_equipment() -> None:
    st.subheader("Equipment")
    sql = (
        f"SELECT equipment_type, status, "
        f"COUNT(*) AS operations, "
        f"SUM(duration_minutes) AS total_duration_min, "
        f"SUM(fuel_consumption_liters) AS total_fuel_liters, "
        f"SUM(distance_km) AS total_distance_km "
        f"FROM {SYNAPSE_VIEWS['fact_equipment']} "
        f"GROUP BY equipment_type, status "
        f"ORDER BY total_duration_min DESC"
    )
    df, err = _query_or_error(run_synapse_query, sql)
    if err:
        st.error(err)
    else:
        st.dataframe(df, use_container_width=True)


# ---------------------------------------------------------------------------
# LIVE sections (Databricks SQL -> live sensor KPI views)
# ---------------------------------------------------------------------------


def _live_metric_view(view_key: str, value_cols: list[str], title: str) -> None:
    st.markdown(f"**{title}**")
    sql = (
        f"SELECT block_id, sensor_id, reading_ts, {', '.join(value_cols)} "
        f"FROM {LIVE_VIEWS[view_key]} "
        f"ORDER BY reading_ts DESC LIMIT 200"
    )
    df, err = _query_or_error(run_databricks_query, sql)
    if err:
        st.error(err)
        return
    if df is None:
        return
    st.dataframe(df, use_container_width=True)
    for col in value_cols:
        if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
            st.line_chart(df.set_index("reading_ts")[col].sort_index())


def section_live() -> None:
    st.subheader("Live Sensors")

    # Sensor status summary (OK / ANOMALY / FAULT per sensor).
    st.markdown("**Sensor Status (per sensor)**")
    status_sql = (
        f"SELECT block_id, sensor_id, last_reading_ts, reading_count, "
        f"ok_count, anomaly_count, fault_count, avg_battery_pct "
        f"FROM {LIVE_VIEWS['sensor_status']} "
        f"ORDER BY block_id, sensor_id"
    )
    df, err = _query_or_error(run_databricks_query, status_sql)
    if err:
        st.error(err)
    else:
        st.dataframe(df, use_container_width=True)

    _live_metric_view("temperature", ["air_temperature_c", "soil_temperature_c"], "Temperature")
    _live_metric_view("humidity", ["humidity_pct"], "Humidity")
    _live_metric_view("soil_moisture", ["soil_moisture_pct"], "Soil Moisture")


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------


def main() -> None:
    _load_env()
    st.set_page_config(page_title="Plantation Analytics", layout="wide")
    st.title("Plantation Analytics Dashboard")
    st.caption(
        "Historical path: Synapse Serverless SQL over Gold. "
        "Live path: Databricks SQL over live Silver. "
        "Credentials are read from environment variables only."
    )

    syn_cfg = synapse_config()
    dbx_cfg = databricks_config()

    tab_hist, tab_live = st.tabs(["Historical (Synapse / Gold)", "Live Sensors (Databricks SQL)"])

    with tab_hist:
        if not synapse_configured(syn_cfg):
            _not_configured(
                "Historical path (Synapse)",
                ["SYNAPSE_SQL_SERVER", "SYNAPSE_SQL_DATABASE",
                 "SYNAPSE_SQL_USERNAME", "SYNAPSE_SQL_PASSWORD"],
            )
        else:
            section = st.selectbox(
                "Section",
                ["Plantation Overview", "Harvest", "Revenue / Costs",
                 "Fertilizer", "Equipment"],
                key="hist_section",
            )
            if section == "Plantation Overview":
                section_historical_overview()
            elif section == "Harvest":
                section_historical_harvest()
            elif section == "Revenue / Costs":
                section_historical_revenue()
            elif section == "Fertilizer":
                section_historical_fertilizer()
            elif section == "Equipment":
                section_historical_equipment()

    with tab_live:
        if not databricks_configured(dbx_cfg):
            _not_configured(
                "Live path (Databricks SQL)",
                ["DATABRICKS_SQL_SERVER_HOSTNAME", "DATABRICKS_SQL_HTTP_PATH",
                 "DATABRICKS_SQL_ACCESS_TOKEN"],
            )
        else:
            section_live()


if __name__ == "__main__":
    main()
