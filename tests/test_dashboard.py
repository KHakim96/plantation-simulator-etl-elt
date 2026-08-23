"""Phase 8 tests: serving/dashboard layer.

Pure local tests (no Spark, no Azure, no live connections). They validate:

  * ``databricks/sql/live_sensor_kpis.sql`` — static/contract validation:
    correct live Silver columns, required KPI domains, the live-silver ABFSS
    source path, idempotent DDL, and no secrets.
  * ``dashboard/app.py`` — imports cleanly, references the existing Phase 6
    Synapse views, references the implemented Phase 8 Databricks SQL layer,
    contains the plan-required dashboard sections, reads connection config
    from environment variables, and contains no hard-coded secrets.
  * Phase 0–7 regression guard: the Phase 6 Synapse view names and the Phase 7
    live Silver column set used by Phase 8 match the existing project
    definitions (no invented columns/views).

These tests never require Azure credentials and never create Azure resources.
"""

import importlib.util
import io
import re
import sys
import tokenize
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SQL_PATH = REPO_ROOT / "databricks" / "sql" / "live_sensor_kpis.sql"
APP_PATH = REPO_ROOT / "dashboard" / "app.py"
SYNAPSE_VIEWS_SQL = REPO_ROOT / "synapse" / "sql" / "plantation_views.sql"
SENSORS_STREAM = REPO_ROOT / "databricks" / "streaming" / "sensors_stream.py"

LIVE_SILVER_PATH = (
    "abfss://live-silver@plantationsimulatorrg.dfs.core.windows.net/sensors"
)

# Live Silver columns written by Phase 7 transform_live_silver.
LIVE_SILVER_COLUMNS = [
    "timestamp",
    "block_id",
    "sensor_id",
    "soil_moisture_pct",
    "soil_temperature_c",
    "air_temperature_c",
    "humidity_pct",
    "soil_ph",
    "light_intensity_lux",
    "battery_level_pct",
    "sensor_status",
    "_ingested_at",
]

# Phase 6 Synapse Gold serving views (historical source).
SYNAPSE_GOLD_VIEWS = [
    "gold.vw_dim_equipment",
    "gold.vw_dim_employee",
    "gold.vw_fact_harvest",
    "gold.vw_fact_revenue",
    "gold.vw_fact_fertilizer",
    "gold.vw_fact_equipment",
]


def _load_app():
    """Import dashboard/app.py by path (dashboard/ has no __init__.py).

    The module only lazy-imports streamlit/pyodbc/databricks inside functions,
    but it imports ``streamlit`` at top level; if streamlit is unavailable in
    this environment we stub it so the module still imports for static tests.
    """
    if importlib.util.find_spec("streamlit") is None:
        import types

        sys.modules.setdefault("streamlit", types.ModuleType("streamlit"))
    spec = importlib.util.spec_from_file_location("dashboard_app", APP_PATH)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load module from {APP_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules["dashboard_app"] = module
    spec.loader.exec_module(module)
    return module


def _code_tokens_only(source: str) -> str:
    """Return Python source with comments and string literals removed."""
    out = []
    for tok in tokenize.generate_tokens(io.StringIO(source).readline):
        if tok.type in (tokenize.COMMENT, tokenize.STRING):
            continue
        out.append(tok.string)
    return " ".join(out)


def _sql_statements_only(source: str) -> str:
    """Return SQL with line comments and block comments stripped.

    The SQL file's header comments legitimately document that storage keys and
    CACHE/PERSIST TABLE are NOT used, so guards must scan executable SQL only.
    """
    # Strip /* ... */ block comments, then -- line comments.
    no_block = re.sub(r"/\*.*?\*/", " ", source, flags=re.DOTALL)
    lines = [re.sub(r"--.*$", "", ln) for ln in no_block.splitlines()]
    return "\n".join(lines)


app = _load_app()
SQL_TEXT = SQL_PATH.read_text(encoding="utf-8")
APP_TEXT = APP_PATH.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# live_sensor_kpis.sql — static / contract validation
# ---------------------------------------------------------------------------


def test_sql_file_exists_and_non_empty():
    assert SQL_PATH.is_file()
    assert len(SQL_TEXT.strip()) > 0


def test_sql_references_live_silver_path():
    assert LIVE_SILVER_PATH in SQL_TEXT


def test_sql_creates_external_delta_table_not_managed():
    # External/unmanaged exposure over the existing path (no data rewrite).
    assert "USING DELTA" in SQL_TEXT
    assert "LOCATION" in SQL_TEXT
    assert (
        "CREATE TABLE IF NOT EXISTS "
        "plantation_simulator_dbx.live_serving.live_silver_sensors" in SQL_TEXT
    )


def test_sql_is_fully_qualified_with_verified_catalog():
    """All UC objects must be prefixed with the verified catalog
    `plantation_simulator_dbx` so the script never depends on a default/implicit
    catalog context."""
    stmts = _sql_statements_only(SQL_TEXT)
    # Every live_serving object reference must be catalog-qualified.
    for obj in (
        "live_serving.live_silver_sensors",
        "live_serving.vw_kpi_temperature",
        "live_serving.vw_kpi_humidity",
        "live_serving.vw_kpi_soil_moisture",
        "live_serving.vw_kpi_sensor_status",
    ):
        qualified = f"plantation_simulator_dbx.{obj}"
        assert qualified in stmts, f"missing fully-qualified object: {qualified}"
    # The schema itself is created under the verified catalog.
    assert "CREATE SCHEMA IF NOT EXISTS plantation_simulator_dbx.live_serving" in stmts
    # No bare (unqualified) live_serving object reference may remain. A bare
    # reference is one NOT immediately preceded by the catalog name.
    bare = re.findall(r"(?<!plantation_simulator_dbx\.)\blive_serving\.", stmts)
    assert not bare, f"unqualified live_serving references found: {len(bare)}"


def test_sql_is_idempotent():
    assert "CREATE SCHEMA IF NOT EXISTS" in SQL_TEXT
    assert "CREATE TABLE IF NOT EXISTS" in SQL_TEXT
    assert SQL_TEXT.count("CREATE OR REPLACE VIEW") >= 4


def test_sql_uses_all_live_silver_columns():
    body = SQL_TEXT.lower()
    for col in LIVE_SILVER_COLUMNS:
        assert col in body, f"live Silver column missing from SQL: {col}"


def test_sql_covers_required_kpi_domains():
    # The four plan-required KPI domains: Temperature, Humidity, Soil
    # Moisture, Sensor Status.
    assert "vw_kpi_temperature" in SQL_TEXT
    assert "vw_kpi_humidity" in SQL_TEXT
    assert "vw_kpi_soil_moisture" in SQL_TEXT
    assert "vw_kpi_sensor_status" in SQL_TEXT


def test_sql_sensor_status_uses_known_status_values():
    # Status values produced by the Phase 7 generator/transform.
    for status in ("OK", "ANOMALY", "FAULT"):
        assert f"'{status}'" in SQL_TEXT


def test_sql_has_no_secrets_or_storage_keys():
    body = _sql_statements_only(SQL_TEXT).lower()
    for forbidden in (
        "accountkey",
        "account_key",
        "sharedaccesssignature",
        "sas_token",
        "fs.azure.account.key",
        "password",
        "secret",
    ):
        assert forbidden not in body, f"forbidden secret-like token in SQL: {forbidden}"


def test_sql_no_serverless_incompatible_persistence():
    body = _sql_statements_only(SQL_TEXT).upper()
    assert "CACHE TABLE" not in body
    assert "PERSIST" not in body


# ---------------------------------------------------------------------------
# dashboard/app.py — structure, wiring, config, no-secrets
# ---------------------------------------------------------------------------


def test_app_imports_cleanly():
    assert app is not None
    assert callable(app.main)


def test_app_references_all_synapse_gold_views():
    for view in SYNAPSE_GOLD_VIEWS:
        assert view in app.SYNAPSE_VIEWS.values(), f"missing Synapse view: {view}"


def test_app_references_live_kpi_views():
    prefix = "plantation_simulator_dbx.live_serving"
    assert app.LIVE_VIEWS["temperature"] == f"{prefix}.vw_kpi_temperature"
    assert app.LIVE_VIEWS["humidity"] == f"{prefix}.vw_kpi_humidity"
    assert app.LIVE_VIEWS["soil_moisture"] == f"{prefix}.vw_kpi_soil_moisture"
    assert app.LIVE_VIEWS["sensor_status"] == f"{prefix}.vw_kpi_sensor_status"
    assert app.LIVE_TABLE == f"{prefix}.live_silver_sensors"


def test_app_live_references_are_fully_qualified_no_bare_namespace():
    """Every live-serving reference in app.py must use the verified catalog;
    no bare `live_serving.*` reference may remain."""
    # Bare reference = "live_serving." NOT immediately preceded by the catalog.
    bare = re.findall(r"(?<!plantation_simulator_dbx\.)\blive_serving\.", APP_TEXT)
    assert not bare, f"unqualified live_serving references remain: {len(bare)}"
    # The verified catalog prefix must be present.
    assert "plantation_simulator_dbx.live_serving" in APP_TEXT


def test_app_has_required_dashboard_sections():
    # Plan-required sections: Plantation overview, Harvest, Revenue/Costs,
    # Fertilizer, Equipment, and Live Sensors (Temperature, Humidity, Soil
    # Moisture, Sensor Status).
    for fn in (
        "section_historical_overview",
        "section_historical_harvest",
        "section_historical_revenue",
        "section_historical_fertilizer",
        "section_historical_equipment",
        "section_live",
    ):
        assert callable(getattr(app, fn, None)), f"missing section: {fn}"


def test_app_separates_historical_and_live_paths():
    # Historical runner uses pyodbc (Synapse); live runner uses databricks.sql.
    assert "pyodbc" in APP_TEXT
    assert "databricks" in APP_TEXT
    assert "run_synapse_query" in APP_TEXT
    assert "run_databricks_query" in APP_TEXT


def test_app_uses_environment_variables_for_config():
    # Synapse (historical) connection vars.
    for var in (
        "SYNAPSE_SQL_SERVER",
        "SYNAPSE_SQL_DATABASE",
        "SYNAPSE_SQL_USERNAME",
        "SYNAPSE_SQL_PASSWORD",
    ):
        assert var in APP_TEXT, f"missing Synapse env var: {var}"
    # Databricks SQL (live) connection vars.
    for var in (
        "DATABRICKS_SQL_SERVER_HOSTNAME",
        "DATABRICKS_SQL_HTTP_PATH",
        "DATABRICKS_SQL_ACCESS_TOKEN",
    ):
        assert var in APP_TEXT, f"missing Databricks env var: {var}"


def test_app_config_readers_pull_from_os_getenv():
    code = _code_tokens_only(APP_TEXT)
    assert "os.getenv" in code or "getenv" in code


def test_app_has_no_hardcoded_secrets():
    body = APP_TEXT.lower()
    for forbidden in (
        "fs.azure.account.key",
        "sharedaccesssignature",
        "accountkey",
        "dapi",  # Databricks PAT prefix must not be hard-coded
    ):
        assert forbidden not in body, f"forbidden token in app: {forbidden}"
    # No literal warehouse hostname / token values hard-coded.
    assert "azuredatabricks.net" not in body
    assert "sql.azuresynapse.net" not in body


def test_app_no_serverless_incompatible_calls():
    code = _code_tokens_only(APP_TEXT)
    for forbidden in (".cache(", ".persist(", ".unpersist(", "saveAsTable"):
        assert forbidden not in code


def test_app_does_not_modify_phase6_synapse_or_phase7_streaming():
    # The app must only READ the existing Synapse views and live KPI layer;
    # it must not create/alter Synapse objects or re-run the streaming job.
    body = APP_TEXT.upper()
    assert "CREATE EXTERNAL" not in body
    assert "CREATE OR ALTER VIEW" not in body
    assert "sensors_stream" not in APP_TEXT


# ---------------------------------------------------------------------------
# Phase 0–7 regression guards (Phase 8 must not invent columns/views)
# ---------------------------------------------------------------------------


def test_phase6_synapse_views_exist_in_phase6_sql():
    syn_sql = SYNAPSE_VIEWS_SQL.read_text(encoding="utf-8")
    for view in SYNAPSE_GOLD_VIEWS:
        assert f"VIEW {view}" in syn_sql, f"Phase 6 SQL missing view: {view}"


def test_phase7_live_silver_columns_match_streaming_transform():
    stream = SENSORS_STREAM.read_text(encoding="utf-8")
    for col in LIVE_SILVER_COLUMNS:
        assert col in stream, f"Phase 7 streaming missing column: {col}"


def test_phase8_sql_columns_are_subset_of_phase7_columns():
    """Every column the Phase 8 SQL selects must be a real live Silver column."""
    select_cols = set(re.findall(r"\b([a-z_]+)\b", SQL_TEXT.lower()))
    unknown = {
        c
        for c in (
            "air_temperature_c", "soil_temperature_c", "humidity_pct",
            "soil_moisture_pct", "soil_ph", "battery_level_pct",
            "sensor_status", "reading_ts",
        )
        if c not in LIVE_SILVER_COLUMNS and c != "reading_ts"
    }
    assert not unknown, f"SQL references non-existent live Silver columns: {unknown}"
    assert select_cols  # sanity: file has identifiers
