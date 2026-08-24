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
    # Fertilizer, Equipment, and Live Sensors. The redesigned dashboard maps
    # these to renderer functions plus a HISTORICAL_SECTIONS registry.
    for fn in (
        "hist_overview",
        "hist_harvest",
        "hist_revenue",
        "hist_fertilizer",
        "hist_equipment",
        "hist_workforce",
        "live_dashboard",
    ):
        assert callable(getattr(app, fn, None)), f"missing section: {fn}"


# ---------------------------------------------------------------------------
# Theme system (light/dark) — presentation behavior only
# ---------------------------------------------------------------------------


def test_theme_state_default_is_dark():
    """Theme must default to 'dark' (Olist default) in session state."""
    assert app.current_theme() == "dark"
    assert app.is_dark() is True


def test_both_themes_defined():
    """Both light and dark design-token sets must exist (Olist values)."""
    assert app._DARK and app._LIGHT
    for key in ("bg", "card", "text", "subtext", "border", "grid", "header_bg"):
        assert app._DARK[key]
        assert app._LIGHT[key]
    # Themes must actually differ.
    assert app._DARK["bg"] != app._LIGHT["bg"]
    assert app._DARK["card"] != app._LIGHT["card"]
    # Olist-derived values.
    assert app._DARK["bg"] == "#0E1117"
    assert app._LIGHT["bg"] == "#F8FAFC"


def test_plotly_light_theme_exists():
    layout = app.get_plotly_layout(False)
    assert layout["paper_bgcolor"] == app._LIGHT["card"]
    assert layout["plot_bgcolor"] == app._LIGHT["card"]
    assert layout["font"]["color"] == app._LIGHT["text"]
    assert "colorway" in layout


def test_plotly_dark_theme_exists():
    layout = app.get_plotly_layout(True)
    assert layout["paper_bgcolor"] == app._DARK["card"]
    assert layout["plot_bgcolor"] == app._DARK["card"]
    assert layout["font"]["color"] == app._DARK["text"]
    assert "colorway" in layout


def test_hoverlabel_light_theme_readable():
    """FIX 1: light-mode hover tooltips = white bg, dark text, subtle border."""
    import plotly.graph_objects as go
    app.st.session_state["theme"] = "light"
    fig = app.apply_chart_theme(go.Figure())
    hl = fig.layout.hoverlabel
    assert hl.bgcolor == app._LIGHT["hover_label_bg"] == "#FFFFFF"
    assert hl.bordercolor == app._LIGHT["hover_label_border"]
    assert hl.font.color == app._LIGHT["hover_label_text"] == "#0F172A"


def test_hoverlabel_dark_theme_readable():
    """FIX 1: dark-mode hover tooltips = dark card bg, light text, border."""
    import plotly.graph_objects as go
    app.st.session_state["theme"] = "dark"
    fig = app.apply_chart_theme(go.Figure())
    hl = fig.layout.hoverlabel
    assert hl.bgcolor == app._DARK["hover_label_bg"]
    assert hl.bordercolor == app._DARK["hover_label_border"]
    assert hl.font.color == app._DARK["hover_label_text"] == "#F8FAFC"


def test_bar_chart_adds_compact_value_labels(monkeypatch):
    """FIX 3: bar charts show compact value labels on every bar."""
    import pandas as pd
    captured = {}

    def fake_show(fig):
        captured["fig"] = fig

    monkeypatch.setattr(app, "show_plotly", fake_show)
    series = pd.Series(
        [97520000.0, 2000.0, 947.0],
        index=["OIL PALM", "RUBBER", "TEA"],
    )
    app.bar_chart(series, "Test")
    fig = captured["fig"]
    bar = fig.data[0]
    # Labels present, compact, no raw 15-digit decimals.
    assert list(bar.text) == ["97.52M", "2K", "947"]
    assert bar.textposition == "outside"


def test_bar_chart_angles_long_category_labels(monkeypatch):
    """FIX 2: long categorical X labels are angled to avoid overlap."""
    import pandas as pd
    captured = {}

    def fake_show(fig):
        captured["fig"] = fig

    monkeypatch.setattr(app, "show_plotly", fake_show)
    series = pd.Series(
        [1.0, 2.0, 3.0],
        index=["MANUAL BROADCASTING", "FOLIAR SPRAYING", "SOIL INJECTION"],
    )
    app.bar_chart(series, "Test")
    fig = captured["fig"]
    assert fig.layout.xaxis.tickangle == -35


def test_compact_num_formatting():
    """Bar-label number formatting is compact and never 15-digit raw."""
    assert app._compact_num(97520000) == "97.52M"
    assert app._compact_num(17920000) == "17.92M"
    assert app._compact_num(2000) == "2K"
    assert app._compact_num(947) == "947"
    assert app._compact_num(5363096.99) == "5.36M"


def test_no_decorative_emojis_in_ui():
    """FIX 4: no decorative emojis remain in the dashboard source."""
    for emoji in ("🌱", "📊", "🌾", "💰", "🧪", "🚜", "👥", "📡", "⭐", "🟢", "⚡", "🎛️"):
        assert emoji not in APP_TEXT, f"decorative emoji still present: {emoji}"


def test_theme_css_is_theme_scoped():
    """The injected CSS must be generated per-theme (no single hardcoded bg)."""
    light_css = app._theme_css(app._LIGHT)
    dark_css = app._theme_css(app._DARK)
    assert app._LIGHT["bg"] in light_css
    assert app._DARK["bg"] in dark_css
    assert light_css != dark_css


def test_theme_switch_uses_session_state():
    """The theme must be driven by st.session_state['theme']."""
    assert "theme" in APP_TEXT
    assert "session_state" in APP_TEXT
    # Sidebar toggle follows the Olist pattern.
    assert "Dark Mode" in APP_TEXT


def test_app_historical_sections_registry_covers_plan_sections():
    """HISTORICAL_SECTIONS must cover the plan-defined historical sections and
    map each to a callable renderer."""
    expected = {
        "Executive Overview",
        "Harvest",
        "Financial / Costs",
        "Fertilizer",
        "Equipment",
        "Workforce",
    }
    assert set(app.HISTORICAL_SECTIONS) == expected
    for name, fn in app.HISTORICAL_SECTIONS.items():
        assert callable(fn), f"{name} not callable"


def test_app_revenue_section_does_not_fabricate_profit_or_sales():
    """fact_revenue is a cost ledger. The revenue section must NOT present
    profit/margin as metrics — only cost analytics. (A disclaimer sentence that
    says it is "not sales revenue" is allowed.)"""
    idx = APP_TEXT.find("def hist_revenue")
    assert idx != -1
    region = APP_TEXT[idx: idx + 3000]
    # Strip the explicit disclaimer phrase before scanning for fabricated KPIs.
    scan = region.lower().replace("not sales revenue", "").replace(
        "never as sales revenue or profit", ""
    )
    for forbidden in ("profit", "margin", "net income"):
        assert forbidden not in scan, f"revenue section fabricates metric: {forbidden}"
    # It must reference the cost ledger view.
    assert "fact_revenue" in region


def test_app_kpi_formatters_exist_and_handle_none():
    for fn in ("_fmt_int", "_fmt_kg", "_fmt_myr", "_fmt_pct"):
        f = getattr(app, fn, None)
        assert callable(f), f"missing formatter: {fn}"
        assert f(None) == "—", f"{fn} must render None as em-dash"


def test_app_kpi_and_section_title_exist():
    assert callable(app.kpi)
    assert callable(app.section_title)
    assert callable(app._kpi_ribbon)


def test_app_uses_cached_query_wrappers():
    """The dashboard must route queries through cached wrappers so widgets do
    not repeatedly open connections."""
    code = _code_tokens_only(APP_TEXT)
    assert "cache_data" in code
    assert callable(app.synapse_df)
    assert callable(app.databricks_df)


def test_app_separates_historical_and_live_paths():
    # Historical runner uses pyodbc (Synapse); live runner uses databricks.sql.
    assert "pyodbc" in APP_TEXT
    assert "databricks" in APP_TEXT
    assert "run_synapse_query" in APP_TEXT
    assert "run_databricks_query" in APP_TEXT


# ---------------------------------------------------------------------------
# Synapse AAD-token auth (Phase 8 Option B) — no real credentials required
# ---------------------------------------------------------------------------


def test_synapse_config_defaults_to_aad_auth(monkeypatch):
    """With SYNAPSE_SQL_AUTH unset, the auth mode defaults to 'aad'."""
    monkeypatch.delenv("SYNAPSE_SQL_AUTH", raising=False)
    monkeypatch.setenv("SYNAPSE_SQL_SERVER", "s.example.net")
    monkeypatch.setenv("SYNAPSE_SQL_DATABASE", "db")
    cfg = app.synapse_config()
    assert cfg["auth"] == "aad"


def test_synapse_config_sql_auth_override(monkeypatch):
    """SYNAPSE_SQL_AUTH=sql selects the username/password fallback."""
    monkeypatch.setenv("SYNAPSE_SQL_AUTH", "sql")
    monkeypatch.setenv("SYNAPSE_SQL_SERVER", "s.example.net")
    monkeypatch.setenv("SYNAPSE_SQL_DATABASE", "db")
    monkeypatch.setenv("SYNAPSE_SQL_USERNAME", "u")
    monkeypatch.setenv("SYNAPSE_SQL_PASSWORD", "p")
    cfg = app.synapse_config()
    assert cfg["auth"] == "sql"


def test_synapse_configured_aad_needs_only_server_and_db(monkeypatch):
    """AAD mode is configured with just server+database (token from identity)."""
    monkeypatch.setenv("SYNAPSE_SQL_AUTH", "aad")
    monkeypatch.setenv("SYNAPSE_SQL_SERVER", "s.example.net")
    monkeypatch.setenv("SYNAPSE_SQL_DATABASE", "db")
    monkeypatch.delenv("SYNAPSE_SQL_USERNAME", raising=False)
    monkeypatch.delenv("SYNAPSE_SQL_PASSWORD", raising=False)
    assert app.synapse_configured(app.synapse_config()) is True


def test_synapse_configured_sql_needs_username_and_password(monkeypatch):
    """SQL mode requires username+password in addition to server+database."""
    monkeypatch.setenv("SYNAPSE_SQL_AUTH", "sql")
    monkeypatch.setenv("SYNAPSE_SQL_SERVER", "s.example.net")
    monkeypatch.setenv("SYNAPSE_SQL_DATABASE", "db")
    monkeypatch.delenv("SYNAPSE_SQL_USERNAME", raising=False)
    monkeypatch.delenv("SYNAPSE_SQL_PASSWORD", raising=False)
    assert app.synapse_configured(app.synapse_config()) is False
    monkeypatch.setenv("SYNAPSE_SQL_USERNAME", "u")
    monkeypatch.setenv("SYNAPSE_SQL_PASSWORD", "p")
    assert app.synapse_configured(app.synapse_config()) is True


def test_synapse_query_uses_aad_token_path_when_aad(monkeypatch):
    """In AAD mode, run_synapse_query acquires an Azure identity token and uses
    attrs_before (SQL_COPT_SS_ACCESS_TOKEN=1256) — never username/password."""
    import types

    captured = {}

    class _FakeCred:
        def get_token(self, scope):
            captured["scope"] = scope
            return types.SimpleNamespace(token="FAKE_TOKEN")

    class _FakeConn:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    def fake_connect(conn_str, attrs_before=None, timeout=None):
        captured["conn_str"] = conn_str
        captured["attrs_before"] = attrs_before
        return _FakeConn()

    monkeypatch.setenv("SYNAPSE_SQL_AUTH", "aad")
    monkeypatch.setenv("SYNAPSE_SQL_SERVER", "s.example.net")
    monkeypatch.setenv("SYNAPSE_SQL_DATABASE", "db")

    fake_pyodbc = types.SimpleNamespace(connect=fake_connect)
    monkeypatch.setitem(sys.modules, "pyodbc", fake_pyodbc)
    import azure.identity as ai

    monkeypatch.setattr(ai, "AzureCliCredential", _FakeCred)

    import pandas as pd

    monkeypatch.setattr(pd, "read_sql", lambda sql, conn: sql)
    out = app.run_synapse_query("SELECT 1")
    assert out == "SELECT 1"
    # Token passed via attrs_before[1256] as UTF-16-LE bytes; scope is SQL.
    assert captured["scope"] == "https://database.windows.net/.default"
    assert 1256 in captured["attrs_before"]
    assert captured["attrs_before"][1256] == "FAKE_TOKEN".encode("utf-16-le")
    # No username/password embedded in the connection string for AAD.
    assert "UID=" not in captured["conn_str"]
    assert "PWD=" not in captured["conn_str"]


def test_synapse_query_uses_uid_pwd_when_sql(monkeypatch):
    """In SQL mode, run_synapse_query falls back to UID=/PWD= and no AAD token."""
    import types

    captured = {}

    class _FakeConn:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    def fake_connect(conn_str, timeout=None, **kw):
        captured["conn_str"] = conn_str
        captured["kw"] = kw
        return _FakeConn()

    monkeypatch.setenv("SYNAPSE_SQL_AUTH", "sql")
    monkeypatch.setenv("SYNAPSE_SQL_SERVER", "s.example.net")
    monkeypatch.setenv("SYNAPSE_SQL_DATABASE", "db")
    monkeypatch.setenv("SYNAPSE_SQL_USERNAME", "sqluser")
    monkeypatch.setenv("SYNAPSE_SQL_PASSWORD", "sqlpass")

    fake_pyodbc = types.SimpleNamespace(connect=fake_connect)
    monkeypatch.setitem(sys.modules, "pyodbc", fake_pyodbc)

    import pandas as pd

    monkeypatch.setattr(pd, "read_sql", lambda sql, conn: sql)
    out = app.run_synapse_query("SELECT 2")
    assert out == "SELECT 2"
    assert "UID=sqluser" in captured["conn_str"]
    assert "PWD=sqlpass" in captured["conn_str"]
    # No AAD attrs_before token used in SQL mode.
    assert "attrs_before" not in captured["kw"]


def test_synapse_query_does_not_log_token(monkeypatch, capsys):
    """The AAD path must never print the access token."""
    import types

    class _FakeCred:
        def get_token(self, scope):
            return types.SimpleNamespace(token="FAKE_SECRET_TOKEN_123")

    class _FakeConn:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    monkeypatch.setenv("SYNAPSE_SQL_AUTH", "aad")
    monkeypatch.setenv("SYNAPSE_SQL_SERVER", "s.example.net")
    monkeypatch.setenv("SYNAPSE_SQL_DATABASE", "db")
    monkeypatch.setitem(
        sys.modules,
        "pyodbc",
        types.SimpleNamespace(connect=lambda *a, **k: _FakeConn()),
    )
    import azure.identity as ai

    monkeypatch.setattr(ai, "AzureCliCredential", _FakeCred)
    import pandas as pd

    monkeypatch.setattr(pd, "read_sql", lambda sql, conn: sql)
    app.run_synapse_query("SELECT 3")
    out = capsys.readouterr().out
    assert "FAKE_SECRET_TOKEN_123" not in out


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
