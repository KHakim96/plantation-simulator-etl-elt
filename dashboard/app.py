"""Phase 8: Plantation Operations & Analytics (Streamlit).

Two clearly separated serving paths over the verified Phase 0–7 platform:

  * HISTORICAL — Azure Synapse Serverless SQL (built-in endpoint, no dedicated
    pool) reading the Phase 6 Gold serving views (``gold.vw_*``) over the
    Phase 5 Gold Delta models.
  * LIVE — Databricks SQL on the ONE shared serverless SQL Warehouse reading
    the Phase 8 live sensor KPI layer (``plantation_simulator_dbx.live_serving.*``)
    over the Phase 7 live Silver Delta.

The presentation layer follows the ACTUAL Olist Executive Intelligence
Streamlit implementation (sidebar theme toggle + controls, header banner,
st.metric KPI ribbon with colored left accents, tab navigation, and a single
``apply_chart_theme`` Plotly helper). Visual design only — no data changes.

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
import plotly.graph_objects as go
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
        # Auth mode: "aad" (Azure AD access token via local Azure identity) or
        # "sql" (username/password). Defaults to "aad"; set SYNAPSE_SQL_AUTH=sql
        # to force the username/password fallback.
        "auth": (_get("SYNAPSE_SQL_AUTH") or "aad").lower(),
    }


def databricks_config() -> dict:
    """Live connection config from env (empty strings if unset)."""
    return {
        "server_hostname": _get("DATABRICKS_SQL_SERVER_HOSTNAME"),
        "http_path": _get("DATABRICKS_SQL_HTTP_PATH"),
        "access_token": _get("DATABRICKS_SQL_ACCESS_TOKEN"),
    }


def synapse_configured(cfg: dict) -> bool:
    """True when the historical connection can be attempted.

    AAD mode needs only server+database (token comes from the local Azure
    identity); SQL mode additionally needs username+password.
    """
    if not (cfg["server"] and cfg["database"]):
        return False
    if cfg.get("auth", "aad") == "aad":
        return True
    return bool(cfg["username"] and cfg["password"])


def databricks_configured(cfg: dict) -> bool:
    return bool(cfg["server_hostname"] and cfg["http_path"] and cfg["access_token"])


# ---------------------------------------------------------------------------
# Query runners (lazy imports so the app imports cleanly without drivers)
# ---------------------------------------------------------------------------


def run_synapse_query(sql: str) -> pd.DataFrame:
    """Run a read-only query on Synapse Serverless via pyodbc.

    Authentication (username/password preserved as fallback):
      * AAD (default): an Azure AD access token is obtained from the EXISTING
        local Azure identity (Azure CLI) via azure-identity and passed to
        pyodbc through ``attrs_before``. The token is never printed or logged.
      * SQL (``SYNAPSE_SQL_AUTH=sql``): username/password from env vars.
    """
    import pyodbc  # lazy: only needed when a live connection is used

    cfg = synapse_config()
    base = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={cfg['server']};DATABASE={cfg['database']};"
        "Encrypt=yes;TrustServerCertificate=no;"
    )

    if cfg.get("auth", "aad") == "aad":
        # azure-identity (already a project dependency). AzureCliCredential
        # uses the already-authenticated `az` session — no new user/SP.
        from azure.identity import AzureCliCredential

        # 1256 = SQL_COPT_SS_ACCESS_TOKEN (pyodbc attr for AAD token auth).
        credential = AzureCliCredential()
        token = credential.get_token("https://database.windows.net/.default").token
        token_bytes = token.encode("utf-16-le")  # never printed/logged
        attrs_before = {1256: token_bytes}
        with pyodbc.connect(base, attrs_before=attrs_before, timeout=30) as conn:
            return pd.read_sql(sql, conn)

    conn_str = base + f"UID={cfg['username']};PWD={cfg['password']};"
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
# Cached query wrappers (avoid re-opening a connection for every widget)
# ---------------------------------------------------------------------------


@st.cache_data(ttl=300, show_spinner=False)
def synapse_df(sql: str) -> pd.DataFrame:
    """Cached Synapse query (5 min TTL)."""
    return run_synapse_query(sql)


@st.cache_data(ttl=60, show_spinner=False)
def databricks_df(sql: str) -> pd.DataFrame:
    """Cached Databricks SQL query (1 min TTL — live data stays fresh)."""
    return run_databricks_query(sql)


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


def show_error(err) -> None:
    if err:
        st.error(str(err))


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------


def _fmt_int(n) -> str:
    try:
        return f"{round(float(n)):,}"
    except (TypeError, ValueError):
        return "—"


def _fmt_kg(n) -> str:
    try:
        v = float(n)
    except (TypeError, ValueError):
        return "—"
    if abs(v) >= 1_000_000:
        return f"{v / 1_000_000:.2f}M kg"
    if abs(v) >= 1_000:
        return f"{v / 1_000:.1f}K kg"
    return f"{v:.0f} kg"


def _fmt_myr(n) -> str:
    try:
        v = float(n)
    except (TypeError, ValueError):
        return "—"
    if abs(v) >= 1_000_000:
        return f"RM {v / 1_000_000:.2f}M"
    if abs(v) >= 1_000:
        return f"RM {v / 1_000:.1f}K"
    return f"RM {v:,.2f}"


def _fmt_pct(n) -> str:
    try:
        return f"{float(n):.1f}%"
    except (TypeError, ValueError):
        return "—"


# ---------------------------------------------------------------------------
# Presentation layer (ACTUAL Olist implementation pattern)
# ---------------------------------------------------------------------------

# Theme tokens (exact Olist values).
_DARK = {
    "bg": "#0E1117", "card": "#161B22", "text": "#F8FAFC", "subtext": "#94A3B8",
    "border": "#30363D", "grid": "#262C36",
    "header_bg": "linear-gradient(135deg, #1E1B4B 0%, #0F172A 100%)",
    "header_border": "#312E81", "header_text": "#F8FAFC", "hover_bg": "#1E293B",
    # Hover tooltip (dark card + light text + subtle border).
    "hover_label_bg": "#1E293B", "hover_label_border": "#334155", "hover_label_text": "#F8FAFC",
}
_LIGHT = {
    "bg": "#F8FAFC", "card": "#FFFFFF", "text": "#0F172A", "subtext": "#334155",
    "border": "#CBD5E1", "grid": "#CBD5E1",
    "header_bg": "linear-gradient(135deg, #EEF2FF 0%, #E0E7FF 100%)",
    "header_border": "#C7D2FE", "header_text": "#1E1B4B", "hover_bg": "#F1F5F9",
    # Hover tooltip (white bg + dark text + subtle border).
    "hover_label_bg": "#FFFFFF", "hover_label_border": "#CBD5E1", "hover_label_text": "#0F172A",
}


def current_theme() -> str:
    """Active theme ('dark' or 'light'); default 'dark' (Olist default)."""
    return st.session_state.get("theme", "dark")


def is_dark() -> bool:
    return current_theme() == "dark"


def _tokens() -> dict:
    return _DARK if is_dark() else _LIGHT


def _theme_css(t: dict) -> str:
    return f"""
<style>
    /* Olist: hide default menu/footer, transparent header */
    #MainMenu {{visibility: hidden;}}
    footer {{visibility: hidden;}}
    header[data-testid="stHeader"] {{background: transparent;}}

    .stApp {{background-color: {t['bg']} !important; color: {t['text']} !important;}}
    h1, h2, h3, h4, h5, h6, p, span, label, div {{color: {t['text']};}}

    /* Sidebar */
    [data-testid="stSidebar"] {{border-right: none !important; background-color: {t['card']} !important;}}
    [data-testid="stSidebarContent"] {{background-color: {t['card']} !important; color: {t['text']} !important;}}
    [data-testid="stSidebarContent"] label, [data-testid="stSidebarContent"] span,
    [data-testid="stSidebarContent"] p {{color: {t['text']} !important;}}

    /* Theme toggle track */
    [data-testid="stSidebar"] div[role="switch"] {{
        border: 2px solid #334155 !important; background-color: #E2E8F0 !important; padding: 2px !important;
    }}
    [data-testid="stSidebar"] div[role="switch"][aria-checked="true"] {{
        background-color: #1E293B !important; border-color: #6366F1 !important;
    }}
    [data-testid="stSidebar"] div[role="switch"] > div {{
        background-color: #0F172A !important; border: 1px solid #334155 !important;
    }}
    [data-testid="stSidebar"] div[role="switch"][aria-checked="true"] > div {{
        background-color: #FFFFFF !important;
    }}

    /* KPI metric cards (Olist: card + subtle shadow + hover) */
    div[data-testid="stMetric"] {{
        background-color: {t['card']}; border: 1px solid {t['border']}; border-radius: 14px;
        padding: 1.1rem 1.4rem !important; min-height: 120px !important;
        box-shadow: 0 4px 20px rgba(0, 0, 0, 0.08);
        transition: transform 0.2s ease, box-shadow 0.2s ease;
    }}
    div[data-testid="stMetric"]:hover {{
        transform: translateY(-2px); box-shadow: 0 6px 24px rgba(99, 102, 241, 0.2);
    }}
    div[data-testid="stMetricLabel"] {{
        font-size: 0.82rem !important; color: {t['subtext']} !important;
        font-weight: 600 !important; text-transform: uppercase; letter-spacing: 0.05em;
    }}
    div[data-testid="stMetricLabel"] * {{color: {t['subtext']} !important;}}
    div[data-testid="stMetricValue"] {{
        font-size: 1.65rem !important; font-weight: 700 !important; color: {t['text']} !important;
    }}
    div[data-testid="stMetricValue"] * {{color: {t['text']} !important;}}
    div[data-testid="stMetricDelta"] {{font-size: 0.82rem !important; font-weight: 600 !important;}}

    /* Charts sit inside rounded cards */
    .stPlotlyChart {{
        border-radius: 14px; overflow: hidden;
        border: 1px solid {t['border']}; box-shadow: 0 4px 16px rgba(0, 0, 0, 0.08);
    }}

    /* Header banner (Olist .st-key-header_banner) */
    .st-key-header_banner {{
        background: {t['header_bg']}; padding: 1.5rem !important; border-radius: 16px;
        border: 1px solid {t['header_border']}; box-shadow: 0 10px 30px rgba(0, 0, 0, 0.15);
        margin-bottom: 1.25rem !important;
    }}
    .st-key-header_banner h2, .st-key-header_banner p, .st-key-header_banner div {{
        color: {t['header_text']} !important;
    }}

    /* Tabs */
    button[data-baseweb="tab"] {{border-radius: 8px !important; font-weight: 600 !important; padding: 10px 20px !important;}}

    /* Status pills */
    .pa-pill {{display:inline-block; padding:3px 11px; border-radius:999px; font-size:11.5px; font-weight:700;}}
    .pa-ok {{background:{'#14532d' if is_dark() else '#dcfce7'}; color:{'#4ade80' if is_dark() else '#16a34a'};}}
    .pa-anom {{background:{'#4a2f0b' if is_dark() else '#fef3c7'}; color:{'#fbbf24' if is_dark() else '#d97706'};}}
    .pa-fault {{background:{'#5b2121' if is_dark() else '#fee2e2'}; color:{'#f87171' if is_dark() else '#dc2626'};}}

    /* Dataframes */
    div[data-testid="stDataFrame"] {{border:1px solid {t['border']}; border-radius:12px;
      overflow:hidden; background:{t['card']};}}
</style>
"""


def _inject_css() -> None:
    st.markdown(_theme_css(_tokens()), unsafe_allow_html=True)


# --- Olist chart theming helper (apply_chart_theme pattern) ------------------


def apply_chart_theme(
    fig,
    title: str = "",
    height: int = 400,
    showlegend: bool = False,
    custom_margin: dict | None = None,
    tickangle: int = 0,
):
    """Apply the active theme to a Plotly figure (Olist apply_chart_theme)."""
    t = _tokens()
    margin = custom_margin or {"l": 60, "r": 30, "t": 66, "b": 70}
    fig.update_layout(
        title={
            "text": f"<b>{title}</b>" if title else "",
            "font": {"size": 15, "color": t["text"], "family": "sans-serif"},
            "x": 0.01,
            "y": 0.96,
        },
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font={"color": t["text"], "family": "sans-serif"},
        showlegend=showlegend,
        legend=(
            {
                "orientation": "h", "yanchor": "bottom", "y": -0.3,
                "xanchor": "center", "x": 0.5,
                "font": {"color": t["text"]}, "bgcolor": "rgba(0,0,0,0)",
            }
            if showlegend
            else None
        ),
        xaxis={
            "gridcolor": t["grid"], "zerolinecolor": t["grid"],
            "tickfont": {"color": t["subtext"], "size": 11},
            "title_font": {"color": t["text"], "size": 13},
            "tickangle": tickangle,
            "automargin": True,
        },
        yaxis={
            "gridcolor": t["grid"], "zerolinecolor": t["grid"],
            "tickfont": {"color": t["subtext"], "size": 11},
            "title_font": {"color": t["text"], "size": 13},
            "automargin": True,
        },
        height=height,
        margin=margin,
        # FIX 1 — readable hover tooltips in BOTH themes:
        # light: white bg + dark text + subtle border; dark: dark card + light text.
        hoverlabel={
            "bgcolor": t["hover_label_bg"],
            "bordercolor": t["hover_label_border"],
            "font": {"color": t["hover_label_text"], "size": 13, "family": "sans-serif"},
        },
    )
    return fig


def get_plotly_layout(dark: bool) -> dict:
    """Return the base Plotly layout for a theme (test helper)."""
    t = _DARK if dark else _LIGHT
    return {
        "paper_bgcolor": t["card"],
        "plot_bgcolor": t["card"],
        "font": {"color": t["text"], "family": "sans-serif", "size": 12},
        "colorway": ["#6366F1", "#38BDF8", "#10B981", "#F59E0B", "#EF4444", "#8B5CF6"],
    }


def show_plotly(fig) -> None:
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


def line_chart(series: pd.Series, title: str, color: str = "#6366F1", height: int = 380) -> None:
    fig = go.Figure(
        go.Scatter(x=series.index, y=series.values, mode="lines",
                   line={"color": color, "width": 2.5})
    )
    apply_chart_theme(fig, title, height=height)
    show_plotly(fig)


def _compact_num(v) -> str:
    """Compact number for bar data labels (no 15-digit decimals)."""
    try:
        x = float(v)
    except (TypeError, ValueError):
        return str(v)

    def _trim(s: str) -> str:
        # Strip trailing zeros only within the numeric part (before any suffix).
        if "." in s:
            s = s.rstrip("0").rstrip(".")
        return s

    ax = abs(x)
    if ax >= 1_000_000:
        return _trim(f"{x / 1_000_000:.2f}") + "M"
    if ax >= 1_000:
        return _trim(f"{x / 1_000:.1f}") + "K"
    if ax == int(ax):
        return f"{int(x):,}"
    return _trim(f"{x:.2f}")


def bar_chart(series: pd.Series, title: str, color: str = "#6366F1",
              horizontal: bool = False, height: int = 380) -> None:
    labels = series.index.tolist()
    values = series.values.tolist()
    text = [_compact_num(v) for v in values]

    # FIX 2 — angle long categorical X labels so they never overlap.
    max_len = max((len(str(s)) for s in labels), default=0)
    tickangle = -35 if (not horizontal and (max_len > 8 or len(labels) > 6)) else 0

    # FIX 3 — show compact value labels on bars (inside when space allows).
    if horizontal:
        fig = go.Figure(
            go.Bar(y=labels, x=values, orientation="h", marker_color=color,
                   text=text, textposition="outside", cliponaxis=False)
        )
    else:
        fig = go.Figure(
            go.Bar(x=labels, y=values, marker_color=color,
                   text=text, textposition="outside", cliponaxis=False)
        )
    apply_chart_theme(fig, title, height=height, tickangle=tickangle)
    show_plotly(fig)


def donut_chart(series: pd.Series, title: str, colors: list[str] | None = None,
                height: int = 380) -> None:
    fig = go.Figure(
        go.Pie(labels=series.index.tolist(), values=series.values.tolist(),
               hole=0.5, marker={"colors": colors}, textinfo="percent+label")
    )
    apply_chart_theme(fig, title, height=height, showlegend=True)
    show_plotly(fig)


def kpi(key: str, label: str, value: str, delta: str | None = None,
        border_color: str = "#6366F1") -> None:
    """One Olist-style KPI card (st.metric inside a keyed container)."""
    with st.container(key=key):
        st.markdown(
            f"<style>.st-key-{key} div[data-testid='stMetric']"
            f"{{border-left:4px solid {border_color} !important;}}</style>",
            unsafe_allow_html=True,
        )
        st.metric(label=label, value=value, delta=delta)


def section_title(title: str, subtitle: str = "") -> None:
    st.markdown(f"#### {title}")
    if subtitle:
        st.caption(subtitle)


def page_header() -> None:
    """Olist-style header banner (keyed container, gradient)."""
    with st.container(key="header_banner"):
        st.markdown("## Plantation Operations & Analytics")
        st.markdown(
            "Production data pipeline & plantation performance intelligence | "
            "**Azure (ADF · ADLS Gen2 · Databricks · Delta Lake · Synapse Serverless)**"
        )


def chart_card(render_fn) -> None:
    """Render a chart/table inside a themed card (bordered container)."""
    with st.container(border=True):
        render_fn()


def bi_table(df: pd.DataFrame) -> None:
    """Themed BI table (rounded floats, compact)."""
    formatted = df.copy()
    for col in formatted.columns:
        if pd.api.types.is_float_dtype(formatted[col]):
            formatted[col] = formatted[col].round(2)
    st.dataframe(formatted, use_container_width=True, hide_index=True)


# Border accent color per KPI key (Olist left-border pattern).
_KPI_BORDER = {
    "harvest": "#6366F1", "ops": "#38BDF8", "cost": "#F59E0B",
    "equipment": "#8B5CF6", "workforce": "#10B981", "sensors": "#06B6D4",
    "ok": "#10B981", "anomaly": "#F59E0B", "fault": "#EF4444", "battery": "#EC4899",
}


def _kpi_ribbon(items: list[tuple[str, str, str, str | None]], ns: str) -> None:
    """Render a row of Olist KPI cards. items = [(key, label, value, delta)].

    ``ns`` namespaces container keys so they stay unique across tabs.
    """
    cols = st.columns(len(items))
    for col, (key, label, value, delta) in zip(cols, items):
        with col:
            kpi(f"{ns}_{key}", label, value, delta,
                border_color=_KPI_BORDER.get(key, "#6366F1"))


# ---------------------------------------------------------------------------
# HISTORICAL — Synapse Serverless over Gold
# ---------------------------------------------------------------------------


def hist_overview() -> None:
    v = SYNAPSE_VIEWS
    k_harvest, e1 = _query_or_error(
        synapse_df,
        f"SELECT COUNT(*) AS n, SUM(harvested_weight_kg) AS kg FROM {v['fact_harvest']}",
    )
    k_cost, e2 = _query_or_error(
        synapse_df, f"SELECT SUM(amount) AS amt FROM {v['fact_revenue']}"
    )
    k_equip, e3 = _query_or_error(
        synapse_df, f"SELECT COUNT(*) AS n FROM {v['dim_equipment']}"
    )
    k_emp, e4 = _query_or_error(
        synapse_df, f"SELECT COUNT(*) AS n FROM {v['dim_employee']}"
    )
    for e in (e1, e2, e3, e4):
        show_error(e)
    harvest_rows = int(k_harvest.iloc[0]["n"]) if k_harvest is not None and len(k_harvest) else 0
    harvest_kg = k_harvest.iloc[0]["kg"] if k_harvest is not None and len(k_harvest) else None
    cost_amt = k_cost.iloc[0]["amt"] if k_cost is not None and len(k_cost) else None
    equip_n = k_equip.iloc[0]["n"] if k_equip is not None and len(k_equip) else 0
    emp_n = k_emp.iloc[0]["n"] if k_emp is not None and len(k_emp) else 0

    _kpi_ribbon(
        [
            ("harvest", "Total Harvested", _fmt_kg(harvest_kg), None),
            ("ops", "Harvest Operations", _fmt_int(harvest_rows), None),
            ("cost", "Operating Cost", _fmt_myr(cost_amt), None),
            ("equipment", "Equipment Fleet", _fmt_int(equip_n), None),
            ("workforce", "Workforce", _fmt_int(emp_n), None),
        ],
        ns="ov",
    )

    def _trend():
        trend, err = _query_or_error(
            synapse_df,
            f"SELECT harvest_date, SUM(harvested_weight_kg) AS kg "
            f"FROM {v['fact_harvest']} GROUP BY harvest_date ORDER BY harvest_date",
        )
        show_error(err)
        if trend is not None and len(trend):
            line_chart(trend.set_index("harvest_date")["kg"], "Harvest Trend")

    chart_card(_trend)

    c1, c2 = st.columns(2)
    with c1:
        def _by_crop():
            by_crop, err = _query_or_error(
                synapse_df,
                f"SELECT crop_type, SUM(harvested_weight_kg) AS kg "
                f"FROM {v['fact_harvest']} GROUP BY crop_type ORDER BY kg DESC",
            )
            show_error(err)
            if by_crop is not None and len(by_crop):
                bar_chart(by_crop.set_index("crop_type")["kg"], "Harvest by Crop")

        chart_card(_by_crop)
    with c2:
        def _top_blocks():
            top_blocks, err = _query_or_error(
                synapse_df,
                f"SELECT TOP 10 block_id, SUM(harvested_weight_kg) AS kg, "
                f"COUNT(*) AS operations "
                f"FROM {v['fact_harvest']} GROUP BY block_id ORDER BY kg DESC",
            )
            show_error(err)
            if top_blocks is not None:
                section_title("Top Blocks by Harvest")
                bi_table(top_blocks)

        chart_card(_top_blocks)


def hist_harvest() -> None:
    v = SYNAPSE_VIEWS["fact_harvest"]
    blocks_df, _ = _query_or_error(
        synapse_df, f"SELECT DISTINCT block_id FROM {v} ORDER BY block_id"
    )
    crops_df, _ = _query_or_error(
        synapse_df, f"SELECT DISTINCT crop_type FROM {v} ORDER BY crop_type"
    )
    f1, f2 = st.columns(2)
    block_opts = blocks_df["block_id"].tolist() if blocks_df is not None else []
    crop_opts = crops_df["crop_type"].tolist() if crops_df is not None else []
    sel_blocks = f1.multiselect("Block", block_opts, key="hv_block")
    sel_crops = f2.multiselect("Crop", crop_opts, key="hv_crop")

    base, err = _query_or_error(
        synapse_df,
        f"SELECT harvest_date, block_id, crop_type, harvested_weight_kg, "
        f"quality_grade, moisture_pct, status, destination FROM {v}",
    )
    show_error(err)
    if base is None:
        return
    df = base.copy()
    if sel_blocks:
        df = df[df["block_id"].isin(sel_blocks)]
    if sel_crops:
        df = df[df["crop_type"].isin(sel_crops)]
    if df.empty:
        st.info("No harvest records match the selected filters.")
        return

    completed = df[df["status"] == "COMPLETED"]
    _kpi_ribbon(
        [
            ("harvest", "Total Harvested", _fmt_kg(df["harvested_weight_kg"].sum()), None),
            ("ops", "Operations", _fmt_int(len(df)), None),
            ("ok", "Completed", _fmt_int(len(completed)), None),
            ("sensors", "Completion Rate",
             _fmt_pct(100.0 * len(completed) / len(df) if len(df) else 0), None),
            ("battery", "Avg Moisture", _fmt_pct(df["moisture_pct"].mean()), None),
        ],
        ns="hv",
    )

    chart_card(lambda: line_chart(
        df.groupby("harvest_date")["harvested_weight_kg"].sum().sort_index(),
        "Harvest Trend"))

    c1, c2 = st.columns(2)
    with c1:
        chart_card(lambda: bar_chart(
            df.groupby("crop_type")["harvested_weight_kg"].sum().sort_values(ascending=False),
            "Harvest by Crop"))
        chart_card(lambda: bar_chart(
            df.groupby("quality_grade")["harvested_weight_kg"].count(),
            "Quality Grade (operations)"))
    with c2:
        chart_card(lambda: bar_chart(
            df.groupby("block_id")["harvested_weight_kg"].sum().sort_values(ascending=False),
            "Harvest by Block"))
        chart_card(lambda: bar_chart(
            df.groupby("destination")["harvested_weight_kg"].sum().sort_values(ascending=False),
            "Harvest by Destination"))


def hist_revenue() -> None:
    v = SYNAPSE_VIEWS["fact_revenue"]
    base, err = _query_or_error(
        synapse_df,
        f"SELECT posting_date, fiscal_period, transaction_type, gl_account, "
        f"cost_center_id, amount, debit_credit_indicator, currency FROM {v}",
    )
    show_error(err)
    if base is None or base.empty:
        if base is not None:
            st.info("No finance postings available.")
        return
    df = base.copy()

    total = df["amount"].sum()
    deb = df.loc[df["debit_credit_indicator"] == "S", "amount"].sum()
    n_txn = df["transaction_type"].nunique()
    _kpi_ribbon(
        [
            ("cost", "Total Operating Cost", _fmt_myr(total), None),
            ("ops", "Ledger Lines", _fmt_int(len(df)), None),
            ("harvest", "Debit Lines (S)", _fmt_myr(deb), None),
            ("equipment", "Cost Categories", _fmt_int(n_txn), None),
            ("workforce", "Currency", str(df["currency"].iloc[0]) if len(df) else "—", None),
        ],
        ns="rv",
    )

    chart_card(lambda: line_chart(
        df.groupby("posting_date")["amount"].sum().sort_index(), "Cost Trend"))

    c1, c2 = st.columns(2)
    with c1:
        chart_card(lambda: bar_chart(
            df.groupby("transaction_type")["amount"].sum().sort_values(ascending=False),
            "Cost by Category"))
        chart_card(lambda: bar_chart(
            df.groupby("cost_center_id")["amount"].sum().sort_values(ascending=False),
            "Cost by Cost Center"))
    with c2:
        chart_card(lambda: bar_chart(
            df.groupby("fiscal_period")["amount"].sum().sort_index(),
            "Cost by Fiscal Period"))

    def _gl():
        section_title("Top GL Accounts by Cost")
        bi_table(
            df.groupby("gl_account")["amount"].sum()
            .sort_values(ascending=False).head(10).reset_index()
        )

    chart_card(_gl)


def hist_fertilizer() -> None:
    v = SYNAPSE_VIEWS["fact_fertilizer"]
    base, err = _query_or_error(
        synapse_df,
        f"SELECT application_date, block_id, crop_type, material_id, quantity_kg, "
        f"application_method, application_status, rainfall_mm FROM {v}",
    )
    show_error(err)
    if base is None or base.empty:
        if base is not None:
            st.info("No fertilizer applications available.")
        return
    df = base.copy()

    _kpi_ribbon(
        [
            ("harvest", "Total Applied", _fmt_kg(df["quantity_kg"].sum()), None),
            ("ops", "Applications", _fmt_int(len(df)), None),
            ("equipment", "Materials Used", _fmt_int(df["material_id"].nunique()), None),
            ("workforce", "Blocks Covered", _fmt_int(df["block_id"].nunique()), None),
            ("cost", "Avg/Application", _fmt_kg(df["quantity_kg"].mean()), None),
        ],
        ns="ft",
    )

    chart_card(lambda: line_chart(
        df.groupby("application_date")["quantity_kg"].sum().sort_index(),
        "Application Trend"))

    c1, c2 = st.columns(2)
    with c1:
        chart_card(lambda: bar_chart(
            df.groupby("material_id")["quantity_kg"].sum().sort_values(ascending=False),
            "By Material"))
        chart_card(lambda: bar_chart(
            df.groupby("application_method")["quantity_kg"].sum().sort_values(ascending=False),
            "By Method"))
    with c2:
        chart_card(lambda: bar_chart(
            df.groupby("crop_type")["quantity_kg"].sum().sort_values(ascending=False),
            "By Crop"))

    def _top():
        section_title("Top Blocks by Usage")
        bi_table(
            df.groupby("block_id")["quantity_kg"].sum()
            .sort_values(ascending=False).head(10).reset_index()
        )

    chart_card(_top)


def hist_equipment() -> None:
    fv = SYNAPSE_VIEWS["fact_equipment"]
    dv = SYNAPSE_VIEWS["dim_equipment"]
    fleet, e1 = _query_or_error(
        synapse_df,
        f"SELECT equipment_type, COUNT(*) AS n FROM {dv} GROUP BY equipment_type",
    )
    ops, e2 = _query_or_error(
        synapse_df,
        f"SELECT operation_date, equipment_id, equipment_type, operation_type, status, "
        f"duration_minutes, engine_hours, fuel_consumption_liters, distance_km, "
        f"maintenance_flag FROM {fv}",
    )
    show_error(e1)
    show_error(e2)
    if ops is None or ops.empty:
        if ops is not None:
            st.info("No equipment operations available.")
        return
    df = ops.copy()
    fleet_n = int(fleet["n"].sum()) if fleet is not None and len(fleet) else 0
    maint_ops = df[df["maintenance_flag"]]
    _kpi_ribbon(
        [
            ("equipment", "Equipment Fleet", _fmt_int(fleet_n), None),
            ("ops", "Operations", _fmt_int(len(df)), None),
            ("workforce", "Equipment Used", _fmt_int(df["equipment_id"].nunique()), None),
            ("cost", "Total Fuel (L)", _fmt_int(df["fuel_consumption_liters"].sum()), None),
            ("fault", "Maintenance Ops", _fmt_int(len(maint_ops)), None),
        ],
        ns="eq",
    )

    def _fleet():
        if fleet is not None and len(fleet):
            bar_chart(fleet.set_index("equipment_type")["n"].sort_values(ascending=False),
                      "Fleet by Type")

    c1, c2 = st.columns(2)
    with c1:
        chart_card(_fleet)
        chart_card(lambda: bar_chart(
            df.groupby("status")["operation_type"].count(), "Operations by Status"))
    with c2:
        chart_card(lambda: line_chart(
            df.groupby("operation_date")["fuel_consumption_liters"].sum().sort_index(),
            "Fuel Consumption Trend"))
        chart_card(lambda: bar_chart(
            df.groupby("equipment_type")["duration_minutes"].sum().sort_values(ascending=False),
            "Utilization by Type (min)"))

    def _top():
        section_title("Top Equipment by Operating Time")
        bi_table(
            df.groupby("equipment_id")["duration_minutes"].sum()
            .sort_values(ascending=False).head(10).reset_index()
        )

    chart_card(_top)


def hist_workforce() -> None:
    dv = SYNAPSE_VIEWS["dim_employee"]
    emp, err = _query_or_error(
        synapse_df,
        f"SELECT role, department, cost_center_id, COUNT(*) AS n "
        f"FROM {dv} GROUP BY role, department, cost_center_id",
    )
    show_error(err)
    if emp is None or emp.empty:
        if emp is not None:
            st.info("No employee records available.")
        return
    _kpi_ribbon(
        [
            ("workforce", "Employees", _fmt_int(emp["n"].sum()), None),
            ("sensors", "Roles", _fmt_int(emp["role"].nunique()), None),
            ("equipment", "Departments", _fmt_int(emp["department"].nunique()), None),
            ("cost", "Cost Centers", _fmt_int(emp["cost_center_id"].nunique()), None),
        ],
        ns="wf",
    )

    c1, c2 = st.columns(2)
    with c1:
        chart_card(lambda: bar_chart(
            emp.groupby("role")["n"].sum().sort_values(ascending=False), "Employees by Role"))
    with c2:
        chart_card(lambda: bar_chart(
            emp.groupby("department")["n"].sum().sort_values(ascending=False),
            "Employees by Department"))


HISTORICAL_SECTIONS = {
    "Executive Overview": hist_overview,
    "Harvest": hist_harvest,
    "Financial / Costs": hist_revenue,
    "Fertilizer": hist_fertilizer,
    "Equipment": hist_equipment,
    "Workforce": hist_workforce,
}


# ---------------------------------------------------------------------------
# LIVE — Databricks SQL over live Silver
# ---------------------------------------------------------------------------


def _live_chart(view_key: str, col: str, title: str):
    def _render() -> None:
        sql = f"SELECT reading_ts, {col} FROM {LIVE_VIEWS[view_key]} ORDER BY reading_ts"
        df, err = _query_or_error(databricks_df, sql)
        show_error(err)
        if df is None or df.empty:
            if df is not None:
                st.info("No readings available.")
            return
        line_chart(df.set_index("reading_ts")[col], title)

    return _render


def live_dashboard() -> None:
    status, err = _query_or_error(
        databricks_df,
        f"SELECT sensor_id, block_id, reading_count, ok_count, anomaly_count, "
        f"fault_count, avg_battery_pct, last_reading_ts "
        f"FROM {LIVE_VIEWS['sensor_status']}",
    )
    show_error(err)
    if status is None or status.empty:
        if status is not None:
            st.info("No live sensor data available.")
        return

    n_sensors = status["sensor_id"].nunique()
    n_readings = int(status["reading_count"].sum())
    ok = int(status["ok_count"].sum())
    anomaly = int(status["anomaly_count"].sum())
    fault = int(status["fault_count"].sum())
    batt = status["avg_battery_pct"].mean()

    _kpi_ribbon(
        [
            ("sensors", "Sensors", _fmt_int(n_sensors), None),
            ("ops", "Readings", _fmt_int(n_readings), None),
            ("ok", "OK", _fmt_int(ok), None),
            ("anomaly", "Anomaly", _fmt_int(anomaly), None),
            ("fault", "Fault", _fmt_int(fault), None),
            ("battery", "Avg Battery", _fmt_pct(batt), None),
        ],
        ns="lv",
    )

    st.markdown(
        f'<span class="pa-pill pa-ok">OK {ok}</span>&nbsp;'
        f'<span class="pa-pill pa-anom">ANOMALY {anomaly}</span>&nbsp;'
        f'<span class="pa-pill pa-fault">FAULT {fault}</span>',
        unsafe_allow_html=True,
    )
    st.write("")

    left, right = st.columns([1, 2])
    with left:
        def _health():
            dist = pd.Series({"OK": ok, "ANOMALY": anomaly, "FAULT": fault})
            donut_chart(dist, "Sensor Health", colors=["#10B981", "#F59E0B", "#EF4444"])

        chart_card(_health)
    with right:
        def _per_sensor():
            table = status.copy()
            if "avg_battery_pct" in table:
                table["avg_battery_pct"] = table["avg_battery_pct"].round(1)
            section_title("Per-Sensor Status")
            bi_table(table)

        chart_card(_per_sensor)

    c1, c2 = st.columns(2)
    with c1:
        chart_card(_live_chart("temperature", "air_temperature_c", "Air Temperature (°C)"))
        chart_card(_live_chart("soil_moisture", "soil_ph", "Soil pH"))
        chart_card(_live_chart("soil_moisture", "soil_moisture_pct", "Soil Moisture (%)"))
    with c2:
        chart_card(_live_chart("temperature", "soil_temperature_c", "Soil Temperature (°C)"))
        chart_card(_live_chart("humidity", "humidity_pct", "Humidity (%)"))


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

_PAGE_META = {
    "Executive Overview": (
        "Executive Overview",
        "Operational overview across harvest, costs, fertilizer, equipment and workforce.",
    ),
    "Harvest": ("Harvest Analytics", "Production volume, quality and destination."),
    "Financial / Costs": (
        "Financial / Operating Cost Analytics",
        "Operating cost activity across labor, fertilizer, equipment and payroll.",
    ),
    "Fertilizer": ("Fertilizer Analytics", "Application volume by material, crop and method."),
    "Equipment": ("Equipment Analytics", "Fleet composition, operations and maintenance."),
    "Workforce": ("Workforce", "Employee dimension overview (role, department, cost center)."),
    "Live Sensors": (
        "Live Sensor Monitoring",
        "Real-time operational monitoring from Databricks SQL over live Silver.",
    ),
}


def _sidebar() -> None:
    """Olist-style sidebar: controls (theme toggle) + architecture context."""
    st.sidebar.markdown("### Plantation Analytics")
    dark_on = st.sidebar.toggle("Dark Mode", value=is_dark(), key="_theme_toggle")
    new_theme = "dark" if dark_on else "light"
    if new_theme != current_theme():
        st.session_state["theme"] = new_theme
        st.rerun()
    st.sidebar.markdown("---")
    st.sidebar.markdown(
        "**Architecture Context:**\n"
        "- **Historical:** Synapse Serverless · Gold\n"
        "- **Live:** Databricks SQL · live Silver\n"
        "- **Pipeline:** ADF · ADLS Gen2 · Databricks · Delta"
    )


def main() -> None:
    _load_env()
    st.set_page_config(
        page_title="Plantation Operations & Analytics",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    st.session_state.setdefault("theme", "dark")
    _inject_css()
    _sidebar()

    syn_cfg = synapse_config()
    dbx_cfg = databricks_config()

    page_header()

    sections = list(HISTORICAL_SECTIONS.keys()) + ["Live Sensors"]
    tabs = st.tabs(sections)

    for section, tab in zip(sections, tabs):
        with tab:
            title, subtitle = _PAGE_META[section]
            section_title(title, subtitle)
            if section == "Live Sensors":
                if not databricks_configured(dbx_cfg):
                    _not_configured(
                        "Live path (Databricks SQL)",
                        ["DATABRICKS_SQL_SERVER_HOSTNAME", "DATABRICKS_SQL_HTTP_PATH",
                         "DATABRICKS_SQL_ACCESS_TOKEN"],
                    )
                else:
                    live_dashboard()
            else:
                if not synapse_configured(syn_cfg):
                    _not_configured(
                        "Historical path (Synapse)",
                        ["SYNAPSE_SQL_SERVER", "SYNAPSE_SQL_DATABASE",
                         "SYNAPSE_SQL_USERNAME", "SYNAPSE_SQL_PASSWORD"],
                    )
                else:
                    HISTORICAL_SECTIONS[section]()


if __name__ == "__main__":
    main()
