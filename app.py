"""
app.py — CS2 Market Analytics Terminal v8.1

Naprawki vs v8:
  • Czas wyświetlany w strefie Europe/Warsaw (serwer działa w UTC).
  • Każda funkcja matematyczna (gap, multiplier, breakeven) sprawdza
    czy OBIE ceny (Steam I Skinport) są != None przed obliczeniem.
    Brak danych → szary myślnik "—", nigdy fałszywy wynik.
  • Sygnał WYBUCH: odpala się tylko gdy obie ceny są świeże i nie-NULL.
  • Portfel: sqlite3.Row konwertowane do dict przez dict(t) — brak AttributeError.
  • use_container_width zastąpione przez width='stretch' (nowe API Streamlit).
"""

import math
import sqlite3
from datetime import datetime, timedelta, timezone

import plotly.graph_objects as go
import streamlit as st

# ─────────────────────────────────────────────────────────────────────────────
# Strefa czasowa Warsaw — serwer działa w UTC, UI pokazuje czas lokalny
# ─────────────────────────────────────────────────────────────────────────────
try:
    from zoneinfo import ZoneInfo
    TZ_WARSAW = ZoneInfo("Europe/Warsaw")
except ImportError:
    # Python < 3.9 lub brak tzdata — fallback do UTC+1/+2 manualnie
    TZ_WARSAW = None  # obsłużone w ts_to_warsaw()

from database import (
    add_to_watchlist,
    close_trade,
    get_closed_trades,
    get_latest_price,
    get_open_trades,
    get_price_as_of,
    get_price_history,
    get_watchlist,
    initialize_database,
    get_connection,
    open_trade,
    remove_from_watchlist,
)

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
MAX_DATA_AGE_H       = 4.0
SKP_FRESHNESS_MIN    = 60
DELTA_STALENESS_MULT = 2.5
VOL_HIGH             = 1_000
VOL_MED              = 100
VOL_MAX_BAR          = 50_000
STEAM_FEE            = 0.85
MIN_FEE_PLN          = 0.05
WYBUCH_THRESHOLD     = 0.10
MULTI_GOLDEN         = 1.30
MULTI_TRANSFER       = 1.10

st.set_page_config(
    page_title="CS2 SIGNAL TERMINAL",
    page_icon="▲",
    layout="wide",
    initial_sidebar_state="expanded",
)
initialize_database()

# ─────────────────────────────────────────────────────────────────────────────
# CSS
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&family=IBM+Plex+Sans+Condensed:wght@400;600;700&display=swap');

:root {
    --bg:#050709; --bg1:#080b0f; --bg2:#0c1016; --bg3:#111820;
    --bdr:#14202e; --bdr2:#1a2b3c;
    --cyan:#00d4ff; --gold:#e8a000; --skp:#ff8c00;
    --green:#00c853; --red:#f5222d; --amber:#ff9500; --purple:#b060ff;
    --dim:#3d5166; --muted:#5a7080; --text:#9ab0c0; --hi:#cce0ee;
    --mono:'IBM Plex Mono','Courier New',monospace;
    --sans:'IBM Plex Sans Condensed',sans-serif;
}
html,body,
[data-testid="stAppViewContainer"],
[data-testid="stAppViewContainer"]>.main { background:var(--bg) !important; }
[data-testid="stHeader"]  { background:var(--bg) !important; border-bottom:1px solid var(--bdr); }
[data-testid="stSidebar"] { background:var(--bg1) !important; border-right:1px solid var(--bdr) !important; }
footer,#MainMenu { visibility:hidden; }
.block-container  { padding:0.4rem 1.1rem 1rem !important; max-width:100% !important; }
section.main>div  { padding-top:0.3rem !important; }

*,p,span,div,label { font-family:var(--mono) !important; color:var(--text); }
h1,h2,h3,h4 { font-family:var(--sans) !important; font-weight:700; letter-spacing:0.06em; text-transform:uppercase; }
h1 { font-size:0.98rem !important; color:var(--cyan) !important; margin-bottom:0 !important; }
h2 { font-size:0.72rem !important; color:var(--gold) !important; border-bottom:1px solid var(--bdr2); padding-bottom:2px; margin:8px 0 4px; }

[data-testid="stTabs"] [role="tablist"] { border-bottom:1px solid var(--bdr2); gap:0; }
[data-testid="stTabs"] [role="tab"] { font-family:var(--sans) !important; font-size:0.62rem !important; font-weight:700; letter-spacing:0.14em; text-transform:uppercase; color:var(--dim) !important; border-radius:0 !important; padding:5px 16px !important; border-bottom:2px solid transparent !important; background:transparent !important; }
[data-testid="stTabs"] [role="tab"][aria-selected="true"] { color:var(--cyan) !important; border-bottom-color:var(--cyan) !important; }

[data-testid="stTextInput"] input,
[data-baseweb="select"]>div { background:var(--bg2) !important; border:1px solid var(--bdr2) !important; border-radius:0 !important; color:var(--hi) !important; font-family:var(--mono) !important; font-size:0.74rem !important; }
[data-baseweb="popover"] ul { background:var(--bg2) !important; border:1px solid var(--bdr2) !important; border-radius:0 !important; }
[data-baseweb="popover"] li:hover { background:var(--bg3) !important; }
[data-testid="stNumberInput"] input { background:var(--bg2) !important; border:1px solid var(--bdr2) !important; border-radius:0 !important; color:var(--hi) !important; font-family:var(--mono) !important; }

[data-testid="stButton"] button { background:var(--bg2) !important; color:var(--cyan) !important; border:1px solid var(--bdr2) !important; border-radius:0 !important; font-family:var(--sans) !important; font-size:0.62rem !important; font-weight:700; letter-spacing:0.12em; text-transform:uppercase; padding:3px 10px !important; }
[data-testid="stButton"] button:hover { border-color:var(--cyan) !important; background:rgba(0,212,255,0.05) !important; }
[data-testid="stFormSubmitButton"] button { background:rgba(0,200,83,0.12) !important; color:var(--green) !important; border-color:var(--green) !important; font-size:0.70rem !important; padding:5px 16px !important; }

[data-testid="stMetric"] { background:var(--bg2); border:1px solid var(--bdr); border-radius:0; padding:5px 10px !important; }
[data-testid="stMetricLabel"] { font-family:var(--sans) !important; font-size:0.56rem !important; color:var(--dim) !important; letter-spacing:0.14em; text-transform:uppercase; }
[data-testid="stMetricValue"] { font-family:var(--mono) !important; font-size:0.96rem !important; color:var(--hi) !important; }
[data-testid="stMetricDelta"] svg { display:none; }

[data-testid="stSelectbox"] label,
[data-testid="stNumberInput"] label { font-family:var(--sans) !important; font-size:0.56rem !important; color:var(--dim) !important; letter-spacing:0.12em; text-transform:uppercase; }
[data-testid="stSidebar"] label { font-size:0.60rem !important; color:var(--muted) !important; }
[data-testid="stToggle"] label { font-family:var(--sans) !important; font-size:0.62rem !important; color:var(--muted) !important; letter-spacing:0.10em; text-transform:uppercase; }
[data-testid="stForm"] { background:var(--bg2); border:1px solid var(--bdr2); padding:12px 14px 8px; }

hr { border-color:var(--bdr2) !important; margin:4px 0 !important; }
::-webkit-scrollbar { width:3px; height:3px; }
::-webkit-scrollbar-track { background:var(--bg1); }
::-webkit-scrollbar-thumb { background:var(--bdr2); }

.statusbar { font-size:0.61rem; color:var(--dim); border-top:1px solid var(--bdr); border-bottom:1px solid var(--bdr); background:var(--bg1); padding:3px 0; margin-bottom:6px; display:flex; gap:18px; flex-wrap:wrap; }
.statusbar .hi   { color:var(--cyan); }
.statusbar .ok   { color:var(--green); }
.statusbar .warn { color:var(--amber); }
.statusbar .sep  { color:var(--bdr2); }

.stale-banner { background:#1a0a00; border:1px solid #5a2a00; padding:6px 12px; font-size:0.68rem; color:#ff9500; font-family:'IBM Plex Mono',monospace; margin-bottom:8px; }

.alert-wybuch { background:#0a0800; border:1px solid #c8a000; padding:8px 14px; margin:2px 0; font-family:'IBM Plex Mono',monospace; font-size:0.72rem; display:flex; align-items:center; gap:12px; }
.alert-icon  { font-size:1.1rem; flex-shrink:0; }
.alert-name  { color:var(--hi); font-weight:600; flex:1; min-width:0; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
.alert-meta  { color:#a09060; font-size:0.66rem; white-space:nowrap; }
.alert-action{ color:#ffd700; font-weight:700; white-space:nowrap; }

.price-matrix { width:100%; border-collapse:collapse; font-family:'IBM Plex Mono','Courier New',monospace; font-size:0.72rem; background:var(--bg1); }
.price-matrix thead tr { background:var(--bg); border-bottom:1px solid var(--bdr2); }
.price-matrix thead th { font-family:'IBM Plex Sans Condensed',sans-serif; font-size:0.56rem; font-weight:700; letter-spacing:0.14em; text-transform:uppercase; color:#3d5166; padding:5px 8px; text-align:left; border-right:1px solid var(--bdr); white-space:nowrap; }
.price-matrix thead th:last-child { border-right:none; }
.price-matrix tbody tr { border-bottom:1px solid var(--bdr); transition:background 0.08s; }
.price-matrix tbody tr:hover { background:#0d1520; }
.price-matrix tbody td { padding:4px 8px; vertical-align:middle; border-right:1px solid var(--bdr); white-space:nowrap; }
.price-matrix tbody td:last-child { border-right:none; }
.price-matrix tfoot tr { border-top:1px solid var(--bdr2); background:var(--bg); }
.price-matrix tfoot td { padding:4px 8px; font-family:'IBM Plex Sans Condensed',sans-serif; font-size:0.58rem; letter-spacing:0.08em; text-transform:uppercase; color:var(--dim); }

.row-wybuch { background:linear-gradient(90deg,#1a1300 0%,#0c0900 100%); border-left:3px solid #ffd700 !important; }

.cell-item  { color:#7a9ab0; font-size:0.70rem; max-width:190px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
.cell-price { color:#e8a000; font-weight:600; text-align:right; }
.cell-skp   { color:#ff8c00; text-align:right; }
.cell-await { color:#3d5166; text-align:right; font-style:italic; font-size:0.66rem; }
.cell-gap-fire { color:#ffd700; font-weight:700; text-align:right; }
.cell-gap-ok   { color:#5a7080; text-align:right; }
.cell-multi-gold { color:#ffd700; font-weight:700; text-align:right; }
.cell-multi-ok   { color:#00c853; text-align:right; }
.cell-multi-low  { color:#5a7080; text-align:right; }
.cell-beven { color:#7a9ab0; text-align:right; font-size:0.68rem; }
.cell-up    { color:#00c853; text-align:right; }
.cell-dn    { color:#f5222d; text-align:right; }
.cell-nd    { color:#2a3a48; text-align:right; }
.cell-stale { color:#5a4010; text-align:right; font-size:0.66rem; }
.cell-spark { padding:3px 8px; text-align:center; min-width:100px; }
.cell-liq   { text-align:left; white-space:nowrap; }
.cell-sig   { text-align:center; font-size:0.80rem; }
.cell-vol   { color:#4a6070; text-align:right; font-size:0.68rem; }
.cell-qty   { color:#7a9ab0; text-align:right; }

.liq-high   { color:#00c853; font-weight:600; }
.liq-med    { color:#e8a000; }
.liq-danger { color:#f5222d; }
.liq-none   { color:#2a3a48; }
.vol-bar-wrap { display:inline-block; width:52px; height:4px; background:#14202e; vertical-align:middle; margin-left:5px; }
.vol-bar-fill { height:4px; }

[data-testid="stExpander"] { border:1px solid var(--bdr) !important; border-radius:0 !important; background:var(--bg2) !important; margin-bottom:2px; }
[data-testid="stExpander"] summary { background:var(--bg2) !important; padding:5px 10px !important; }
[data-testid="stExpander"] summary:hover { background:var(--bg3) !important; }
[data-testid="stExpander"] summary p,
[data-testid="stExpander"] summary span { font-family:var(--mono) !important; font-size:0.72rem !important; color:var(--text) !important; }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _row(r: sqlite3.Row | None) -> dict | None:
    return dict(r) if r else None


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def ts_to_warsaw(dt: datetime | None) -> str:
    """Konwertuje datetime UTC → czas wyświetlany w strefie Europe/Warsaw."""
    if dt is None:
        return "—"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    if TZ_WARSAW is not None:
        dt_local = dt.astimezone(TZ_WARSAW)
    else:
        # Fallback: Polska jest UTC+1 zimą / UTC+2 latem
        # Proste przybliżenie: dodajemy 1h (nie uwzględnia DST idealnie)
        dt_local = dt + timedelta(hours=1)
    return dt_local.strftime("%Y-%m-%d %H:%M")


def parse_ts(ts) -> datetime | None:
    if ts is None:
        return None
    if isinstance(ts, datetime):
        return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    if isinstance(ts, str):
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            return None
    return None


def age_hours(ts) -> float | None:
    dt = parse_ts(ts)
    return None if dt is None else (now_utc() - dt).total_seconds() / 3600


def age_minutes(ts) -> float | None:
    dt = parse_ts(ts)
    return None if dt is None else (now_utc() - dt).total_seconds() / 60


def is_stale(ts, max_age_h: float = MAX_DATA_AGE_H) -> bool:
    a = age_hours(ts)
    return True if a is None else a > max_age_h


def _safe_float(v) -> float | None:
    """Zwraca float lub None. NIGDY nie zwraca NaN, Inf ani 0.0 jeśli v <= 0."""
    if v is None:
        return None
    try:
        f = float(v)
        if not math.isfinite(f):
            return None
        if f <= 0:
            return None
        return f
    except (TypeError, ValueError):
        return None


# ─────────────────────────────────────────────────────────────────────────────
# CACHED DB READS
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_data(ttl=60)
def cached_get_last_harvest() -> tuple[str, float | None]:
    """Zwraca (czas w strefie Warsaw jako string, wiek w godzinach)."""
    try:
        with get_connection() as conn:
            r = conn.execute("SELECT MAX(timestamp) AS ts FROM price_history;").fetchone()
        if r and r["ts"]:
            dt = parse_ts(r["ts"])
            if dt:
                age = (now_utc() - dt).total_seconds() / 3600
                return ts_to_warsaw(dt), age
    except Exception:
        pass
    return "—", None


@st.cache_data(ttl=60)
def cached_get_watchlist() -> list[str]:
    return get_watchlist()


@st.cache_data(ttl=60)
def cached_get_latest_price(item_name: str) -> dict | None:
    return _row(get_latest_price(item_name))


@st.cache_data(ttl=60)
def cached_get_price_as_of(item_name: str, hours: float) -> dict | None:
    return _row(get_price_as_of(item_name, hours))


@st.cache_data(ttl=60)
def cached_get_price_history(item_name: str, limit: int = 2000) -> list[dict]:
    return [dict(r) for r in get_price_history(item_name, limit)]


# ─────────────────────────────────────────────────────────────────────────────
# STALENESS-AWARE DELTA
# ─────────────────────────────────────────────────────────────────────────────

def _delta_core(latest_val, baseline_val, baseline_ts, hours, prefix) -> dict:
    empty = {"pct": None, "label": f"{prefix}{hours:.0f}h", "suppressed": True}
    # Oba muszą być nie-None i > 0
    lv = _safe_float(latest_val)
    bv = _safe_float(baseline_val)
    if lv is None or bv is None:
        return empty
    b_age = age_hours(baseline_ts)
    if b_age is None or b_age > hours * DELTA_STALENESS_MULT:
        return empty
    pct   = (lv - bv) / bv * 100
    label = f"{prefix}{b_age:.0f}h*" if b_age > hours * 1.15 else f"{prefix}{hours:.0f}h"
    return {"pct": pct, "label": label, "suppressed": False}


def delta_info(item_name: str, hours: float) -> dict:
    empty = {"pct": None, "label": f"Δ {hours:.0f}h", "suppressed": True}
    latest = cached_get_latest_price(item_name)
    if not latest or is_stale(latest.get("timestamp")):
        return empty
    baseline = cached_get_price_as_of(item_name, hours)
    if not baseline:
        return empty
    return _delta_core(
        latest.get("steam_price"), baseline.get("steam_price"),
        baseline.get("timestamp"), hours, "Δ "
    )


# ─────────────────────────────────────────────────────────────────────────────
# SIGNAL LOGIC — NULL-SAFE
# ─────────────────────────────────────────────────────────────────────────────

def calc_price_gap(steam: float | None, skinport: float | None) -> float | None:
    """
    Gap = (Steam - Skinport) / Steam
    Zwraca None jeśli którakolwiek wartość jest None lub <= 0.
    NIGDY nie oblicza na podstawie 0.0.
    """
    s = _safe_float(steam)
    k = _safe_float(skinport)
    if s is None or k is None:
        return None
    return (s - k) / s


def is_wybuch(
    steam:    float | None,
    skinport: float | None,
    skp_ts,
) -> tuple[bool, float | None]:
    """
    Warunki (WSZYSTKIE muszą być spełnione):
    1. Obie ceny != None i > 0
    2. Skinport timestamp świeży (< SKP_FRESHNESS_MIN minut)
    3. Gap > WYBUCH_THRESHOLD

    Przy braku danych: (False, None) — nigdy nie generuje fałszywego sygnału.
    """
    gap = calc_price_gap(steam, skinport)
    if gap is None:
        return False, None

    skp_age_min = age_minutes(skp_ts)
    if skp_age_min is None or skp_age_min > SKP_FRESHNESS_MIN:
        return False, gap

    return gap > WYBUCH_THRESHOLD, gap


def calc_multiplier(steam: float | None, skinport: float | None) -> float | None:
    """(Steam × 0.85) / Skinport. None jeśli brakuje danych."""
    s = _safe_float(steam)
    k = _safe_float(skinport)
    if s is None or k is None:
        return None
    return (s * STEAM_FEE) / k


def calc_breakeven(steam: float | None) -> float | None:
    """Minimalna cena sprzedaży żeby wyjść na zero po prowizji Steam."""
    s = _safe_float(steam)
    if s is None:
        return None
    return (s / STEAM_FEE) + (MIN_FEE_PLN if s < 1.0 else 0)


def fmt_pct(x, suppressed: bool = False) -> str:
    """Formatuje procent. Przy braku danych zwraca '—'."""
    if suppressed or x is None or (isinstance(x, float) and not math.isfinite(x)):
        return "—"
    return f"{'+'if x>=0 else ''}{x:.2f}%"


def fmt_price(x: float | None) -> str:
    """Formatuje cenę. Przy None zwraca '—'."""
    if x is None:
        return "—"
    return f"{x:.2f}"


# ─────────────────────────────────────────────────────────────────────────────
# LIQUIDITY BADGE
# ─────────────────────────────────────────────────────────────────────────────

def liquidity_score(vol) -> tuple[str, str, str]:
    if vol is None:
        return '<span class="liq-none">—</span>', "none", ""
    v = int(vol)
    bar_color = "#00c853" if v >= VOL_HIGH else ("#e8a000" if v >= VOL_MED else "#f5222d")
    bar_w = max(round(min(v / VOL_MAX_BAR, 1.0) * 52), 1)
    bar   = (f'<span class="vol-bar-wrap">'
             f'<span class="vol-bar-fill" style="width:{bar_w}px;background:{bar_color}"></span>'
             f'</span>')
    vol_fmt = f"{v:,}".replace(",", "\u202f")
    if v >= VOL_HIGH:
        return f'<span class="liq-high">🔥 HIGH</span> <span class="cell-vol">({vol_fmt})</span>', "high", bar
    if v >= VOL_MED:
        return f'<span class="liq-med">🟢 MED</span> <span class="cell-vol">({vol_fmt})</span>', "med", bar
    return f'<span class="liq-danger">🔴 LOW</span> <span class="cell-vol">({vol_fmt})</span>', "danger", bar


# ─────────────────────────────────────────────────────────────────────────────
# SVG SPARKLINE
# ─────────────────────────────────────────────────────────────────────────────

def make_svg_spark(prices: list, stale: bool = False, w: int = 100, h: int = 26) -> str:
    if len(prices) < 2:
        return (f'<svg width="{w}" height="{h}" xmlns="http://www.w3.org/2000/svg">'
                f'<line x1="0" y1="{h//2}" x2="{w}" y2="{h//2}" stroke="#1a2b3c" stroke-width="1"/></svg>')
    color  = "#5a4010" if stale else ("#00c853" if prices[-1] >= prices[0] else "#f5222d")
    fill_c = ("rgba(90,64,16,0.07)" if stale else
               ("rgba(0,200,83,0.07)" if prices[-1] >= prices[0] else "rgba(245,34,45,0.07)"))
    mn, mx = min(prices), max(prices)
    rng    = mx - mn if mx != mn else 1.0
    pad    = 2
    def px(i): return round(pad + (i / (len(prices) - 1)) * (w - 2 * pad), 2)
    def py(v):  return round(pad + (1 - (v - mn) / rng) * (h - 2 * pad), 2)
    pts    = [(px(i), py(v)) for i, v in enumerate(prices)]
    line_d = "M " + " L ".join(f"{x},{y}" for x, y in pts)
    area_d = line_d + f" L {pts[-1][0]},{h-pad} L {pts[0][0]},{h-pad} Z"
    return (f'<svg width="{w}" height="{h}" xmlns="http://www.w3.org/2000/svg">'
            f'<path d="{area_d}" fill="{fill_c}" stroke="none"/>'
            f'<path d="{line_d}" fill="none" stroke="{color}" stroke-width="1.4" '
            f'stroke-linejoin="round" stroke-linecap="round"/>'
            f'<circle cx="{pts[-1][0]}" cy="{pts[-1][1]}" r="2" fill="{color}"/></svg>')


# ─────────────────────────────────────────────────────────────────────────────
# WYBUCH ALERT BANNERS
# ─────────────────────────────────────────────────────────────────────────────

def render_wybuch_banners(rows: list) -> None:
    fired = [r for r in rows if r["_wybuch"]]
    if not fired:
        return
    banners = []
    for r in fired:
        gap_pct = r["_gap"] * 100 if r["_gap"] is not None else 0
        banners.append(
            f'<div class="alert-wybuch">'
            f'<span class="alert-icon">⚡</span>'
            f'<span class="alert-name">{r["ITEM"]}</span>'
            f'<span class="alert-meta">'
            f'Steam <b style="color:#e8a000">{fmt_price(r["PRICE"])} zł</b>'
            f' &nbsp;│&nbsp; '
            f'Skinport <b style="color:#ff8c00">{fmt_price(r["_skp"])} zł</b>'
            f' &nbsp;│&nbsp; '
            f'Gap <b style="color:#ffd700">+{gap_pct:.1f}%</b>'
            f'</span>'
            f'<span class="alert-action">⚡ WYBUCH / LAG — KUP NA STEAM</span>'
            f'</div>'
        )
    st.markdown("".join(banners), unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# HTML PRICE MATRIX
# ─────────────────────────────────────────────────────────────────────────────

def render_price_matrix(rows: list, d3_lbl: str, d24_lbl: str) -> None:

    def pct_cls(d: dict) -> str:
        if d["suppressed"] or d["pct"] is None:
            return "cell-nd"
        return "cell-up" if d["pct"] >= 0 else "cell-dn"

    def gap_cls(gap: float | None, fired: bool) -> str:
        if gap is None:
            return "cell-await"
        return "cell-gap-fire" if fired else "cell-gap-ok"

    def multi_cls(m: float | None) -> str:
        if m is None:
            return "cell-await"
        if m >= MULTI_GOLDEN:
            return "cell-multi-gold"
        if m >= MULTI_TRANSFER:
            return "cell-multi-ok"
        return "cell-multi-low"

    header = (
        "<thead><tr>"
        "<th>ITEM</th><th>SPARK</th>"
        "<th style='text-align:right'>STEAM (zł)</th>"
        "<th style='text-align:right'>SKINPORT (zł)</th>"
        "<th style='text-align:right'>BREAKEVEN</th>"
        "<th style='text-align:right'>MULTIPLIER</th>"
        "<th style='text-align:right'>PRICE GAP</th>"
        f"<th style='text-align:right'>{d3_lbl}</th>"
        f"<th style='text-align:right'>{d24_lbl}</th>"
        "<th>PŁYNNOŚĆ</th>"
        "<th style='text-align:center'>SYGNAŁ</th>"
        "</tr></thead>"
    )

    total_steam = sum(r["PRICE"] for r in rows if r["PRICE"] and not r["_stale"])
    total_skp   = sum(r["_skp"]  for r in rows if r["_skp"]  is not None)
    n_steam     = sum(1 for r in rows if r["PRICE"] and not r["_stale"])
    n_skp       = sum(1 for r in rows if r["_skp"] is not None)

    body_rows = []
    for r in rows:
        stale     = r["_stale"]
        svg       = make_svg_spark(r["_spark"], stale=stale)
        row_cls   = "row-wybuch" if r["_wybuch"] else ""
        steam_cls = "cell-stale" if stale else "cell-price"

        gap   = r["_gap"]
        fired = r["_wybuch"]
        # Jeśli gap jest None (brak danych Skinport) → wyświetl "Awaiting Data"
        if gap is not None:
            gap_str = f"+{gap*100:.1f}%" if gap >= 0 else f"{gap*100:.1f}%"
        else:
            gap_str = "—"

        m = r["_multiplier"]
        multi_str = f"{m:.2f}×" if m is not None else "—"

        beven = r["_breakeven"]
        beven_str = f"{beven:.2f}" if beven is not None else "—"

        badge, _, bar = liquidity_score(r["VOL"])
        d3  = r["_d3"]; d24 = r["_d24"]

        sig_html = ('⚡ <b style="color:#ffd700">WYBUCH</b>' if fired
                    else '<span style="color:#2a3a48">·</span>')

        if stale:
            delta_cells = ('<td class="cell-stale">⚠</td>'
                           '<td class="cell-stale">⚠</td>')
        else:
            delta_cells = (
                f'<td class="{pct_cls(d3)}">{fmt_pct(d3["pct"],d3["suppressed"])}</td>'
                f'<td class="{pct_cls(d24)}">{fmt_pct(d24["pct"],d24["suppressed"])}</td>'
            )

        body_rows.append(
            f'<tr class="{row_cls}">'
            f'<td class="cell-item">{r["ITEM"]}</td>'
            f'<td class="cell-spark">{svg}</td>'
            f'<td class="{steam_cls}" style="text-align:right">{fmt_price(r["PRICE"])}</td>'
            f'<td class="{"cell-skp" if r["_skp"] else "cell-await"}"   style="text-align:right">{fmt_price(r["_skp"]) if r["_skp"] else "—"}</td>'
            f'<td class="{"cell-beven" if beven else "cell-await"}" style="text-align:right">{beven_str}</td>'
            f'<td class="{multi_cls(m)}" style="text-align:right">{multi_str}</td>'
            f'<td class="{gap_cls(gap, fired)}" style="text-align:right">{gap_str}</td>'
            + delta_cells +
            f'<td class="cell-liq">{badge}{bar}</td>'
            f'<td class="cell-sig">{sig_html}</td>'
            f'</tr>'
        )

    footer = (
        "<tfoot><tr>"
        f'<td colspan="2" style="color:#5a7080;font-size:0.58rem">TOTAL ({len(rows)})</td>'
        f'<td style="text-align:right;color:#e8a000">{total_steam:.2f} zł <span style="color:#3d5166;font-size:0.56rem">({n_steam} live)</span></td>'
        f'<td style="text-align:right;color:#ff8c00">{total_skp:.2f} zł <span style="color:#3d5166;font-size:0.56rem">({n_skp} items)</span></td>'
        f'<td colspan="7"></td>'
        "</tr></tfoot>"
    )

    st.markdown(
        f'<div style="overflow-x:auto;border:1px solid #14202e;background:#080b0f">'
        f'<table class="price-matrix">{header}'
        f'<tbody>{"".join(body_rows)}</tbody>'
        f'{footer}</table></div>',
        unsafe_allow_html=True,
    )


# ─────────────────────────────────────────────────────────────────────────────
# PLOTLY CHART
# ─────────────────────────────────────────────────────────────────────────────

def base_layout() -> dict:
    return dict(
        paper_bgcolor="#050709", plot_bgcolor="#080b0f",
        font=dict(family="IBM Plex Mono,Courier New,monospace", color="#5a7080", size=10),
        xaxis=dict(gridcolor="#14202e", zeroline=False, linecolor="#14202e",
                   tickcolor="#3d5166", tickfont=dict(size=9, color="#3d5166")),
        margin=dict(l=48, r=14, t=28, b=32),
        hovermode="x unified",
        hoverlabel=dict(bgcolor="#0c1016", bordercolor="#1a2b3c",
                        font=dict(family="IBM Plex Mono,Courier New,monospace",
                                  color="#9ab0c0", size=10)),
        legend=dict(bgcolor="#050709", bordercolor="#14202e", borderwidth=1,
                    font=dict(size=9, color="#5a7080")),
    )


def render_item_chart(item_name: str, height: int = 320) -> None:
    history = cached_get_price_history(item_name)
    if len(history) < 2:
        st.markdown(
            '<p style="color:#3d5166;font-size:0.70rem;padding:8px 0">NOT ENOUGH DATA</p>',
            unsafe_allow_html=True,
        )
        return

    ts_raw     = [r["timestamp"]     for r in history]
    price_list = [r["steam_price"]   for r in history]
    vol_list   = [r["volume"]        for r in history]
    skp_list   = [_safe_float(r.get("external_price")) for r in history]
    parsed_ts  = [parse_ts(ts) or ts for ts in ts_raw]

    # Przerwy w danych → None zamiast fałszywej linii
    GAP = timedelta(minutes=90)
    ts_p: list = []; p_p: list = []; v_p: list = []; s_p: list = []
    for i, (t, p, v, s) in enumerate(zip(parsed_ts, price_list, vol_list, skp_list)):
        if i > 0 and isinstance(parsed_ts[i-1], datetime) and isinstance(t, datetime):
            if (t - parsed_ts[i-1]) > GAP:
                mid = parsed_ts[i-1] + (t - parsed_ts[i-1]) / 2
                ts_p.append(mid); p_p.append(None); v_p.append(None); s_p.append(None)
        ts_p.append(t); p_p.append(p); v_p.append(v); s_p.append(s)

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=ts_p, y=v_p, name="Vol",
        marker_color="rgba(0,212,255,0.07)", marker_line_width=0,
        yaxis="y2", hovertemplate="Vol: %{y:,}<extra></extra>",
    ))
    fig.add_trace(go.Scatter(
        x=ts_p, y=p_p, mode="lines+markers", name="Steam (PLN)",
        line=dict(color="#00d4ff", width=1.5),
        marker=dict(size=4, color="#00d4ff", symbol="circle",
                    line=dict(color="#050709", width=1)),
        connectgaps=False,
        hovertemplate="<b>%{x|%Y-%m-%d %H:%M}</b><br>Steam: %{y:.2f} zł<extra></extra>",
    ))
    if any(s is not None for s in s_p):
        fig.add_trace(go.Scatter(
            x=ts_p, y=s_p, mode="lines+markers", name="Skinport (PLN)",
            line=dict(color="#ff8c00", width=1.5, dash="dash"),
            marker=dict(size=3, color="#ff8c00", symbol="diamond",
                        line=dict(color="#050709", width=1)),
            connectgaps=False,
            hovertemplate="<b>%{x|%Y-%m-%d %H:%M}</b><br>Skinport: %{y:.2f} zł<extra></extra>",
        ))

    layout = base_layout()
    layout["title"] = dict(
        text=f'<span style="font-size:10px;color:#3d5166;font-family:IBM Plex Sans Condensed,sans-serif">{item_name.upper()}</span>',
        x=0.005, y=0.97, xanchor="left",
    )
    layout["yaxis"] = dict(
        gridcolor="#14202e", zeroline=False, linecolor="#14202e",
        tickcolor="#3d5166", tickfont=dict(size=9, color="#3d5166"),
        title="zł", title_font=dict(color="#3d5166", size=9), tickformat=".2f",
    )
    layout["yaxis2"] = dict(
        overlaying="y", side="right", showgrid=False, title="vol",
        tickfont=dict(color="#2a3d50", size=8), title_font=dict(color="#2a3d50", size=8),
    )
    layout["height"]   = height
    layout["margin"]   = dict(l=48, r=48, t=24, b=28)
    layout["dragmode"] = "pan"
    layout["legend"]   = dict(orientation="h", y=-0.14, x=0,
                               bgcolor="rgba(0,0,0,0)", borderwidth=0,
                               font=dict(size=9, color="#5a7080"))
    fig.update_layout(**layout)

    valid = [p for p in price_list if p is not None]
    if valid:
        last = valid[-1]; chg = last - valid[0]
        chg_pct   = chg / valid[0] * 100 if valid[0] else 0
        chg_color = "#00c853" if chg >= 0 else "#f5222d"
        sign      = "+" if chg >= 0 else ""
        latest_skp = next((s for s in reversed(skp_list) if s is not None), None)
        skp_part   = f'SKP <b style="color:#ff8c00">{latest_skp:.2f}</b> &nbsp;' if latest_skp else ""
        data_age   = age_hours(parsed_ts[-1]) if parsed_ts else None
        fresh_w    = (f'<span style="color:#ff9500"> ⚠ {data_age:.1f}h ago</span>'
                      if data_age and data_age > MAX_DATA_AGE_H else "")
        st.markdown(
            f'<div style="display:flex;gap:16px;padding:4px 0 2px;border-bottom:1px solid #14202e;'
            f'margin-bottom:2px;flex-wrap:wrap;font-size:0.66rem">'
            f'<span style="color:#3d5166">STM <b style="color:#e8a000">{last:.2f} zł</b></span>'
            f'<span style="color:#3d5166">{skp_part}</span>'
            f'<span style="color:#3d5166">CHG <b style="color:{chg_color}">{sign}{chg:.2f} ({sign}{chg_pct:.2f}%)</b></span>'
            f'<span style="color:#3d5166">H <b style="color:#9ab0c0">{max(valid):.2f}</b></span>'
            f'<span style="color:#3d5166">L <b style="color:#9ab0c0">{min(valid):.2f}</b></span>'
            f'<span style="color:#3d5166">PTS <b style="color:#9ab0c0">{len(valid)}</b></span>'
            f'{fresh_w}</div>',
            unsafe_allow_html=True,
        )

    # width="stretch" zamiast use_container_width=True (nowe API Streamlit)
    st.plotly_chart(fig, width="stretch",
                    config={"displaylogo": False, "scrollZoom": True,
                            "modeBarButtonsToRemove": ["select2d", "lasso2d"]})


# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## ▲ CS2 SIGNAL TERMINAL")
    st.caption("Steam + Skinport · PLN · Czas: Europe/Warsaw")
    st.markdown("---")
    st.markdown("## ADD TO WATCHLIST")
    new_item = st.text_input(
        "Item name", placeholder="AK-47 | Redline (Field-Tested)",
        label_visibility="collapsed",
    )
    if st.button("＋ ADD"):
        if new_item.strip():
            added = add_to_watchlist(new_item.strip())
            if added:
                st.success("Dodano.")
                st.cache_data.clear()
                st.rerun()
            else:
                st.warning("Już na watchliście.")
        else:
            st.warning("Wpisz nazwę itemu.")
    st.markdown("---")
    st.markdown("## WATCHLIST")
    wl_sb = cached_get_watchlist()
    if not wl_sb:
        st.caption("Pusta.")
    else:
        for item in wl_sb:
            c1, c2 = st.columns([5, 1])
            c1.caption(item)
            if c2.button("✕", key=f"rm_{item}"):
                remove_from_watchlist(item)
                st.cache_data.clear()
                st.rerun()
    st.markdown("---")
    st.markdown("## LEGENDA")
    st.markdown(
        f'<div style="font-size:0.58rem;color:#3d5166;line-height:2.0">'
        f'<b style="color:#ffd700">⚡ WYBUCH / LAG</b><br>'
        f'<span style="padding-left:8px">(Steam − SKP) / Steam &gt; {WYBUCH_THRESHOLD*100:.0f}%</span><br>'
        f'<span style="padding-left:8px">SKP musi być świeże (&lt; {SKP_FRESHNESS_MIN} min)</span><br>'
        f'<span style="padding-left:8px">Obie ceny muszą być != NULL</span><br><br>'
        f'<span style="color:#3d5166">— = brak danych (Awaiting Data)</span><br>'
        f'<span style="color:#00d4ff">━━</span> Steam &nbsp;'
        f'<span style="color:#ff8c00">╌╌</span> Skinport'
        f'</div>',
        unsafe_allow_html=True,
    )


# ─────────────────────────────────────────────────────────────────────────────
# TOP BAR
# ─────────────────────────────────────────────────────────────────────────────
last_harvest_str, last_harvest_age = cached_get_last_harvest()
wl_count = len(cached_get_watchlist())

hdr_l, hdr_r = st.columns([8, 2])
with hdr_l:
    st.markdown("# ▲ CS2 MARKET ANALYTICS TERMINAL")
with hdr_r:
    st.markdown("<div style='padding-top:5px'></div>", unsafe_allow_html=True)
    if st.button("⟳ REFRESH"):
        st.cache_data.clear()
        st.rerun()

harvest_cls = "ok"
age_sfx     = ""
if last_harvest_age is not None and last_harvest_age > 1.0:
    harvest_cls = "warn"
    age_sfx     = f' <span class="warn">({last_harvest_age:.1f}h temu)</span>'

st.markdown(
    f'<div class="statusbar">'
    f'<span>GIEŁDA <span class="hi">STEAM + SKINPORT · PLN</span></span>'
    f'<span class="sep">│</span>'
    f'<span>OSTATNI HARVEST <span class="{harvest_cls}">{last_harvest_str}</span>{age_sfx}</span>'
    f'<span class="sep">│</span>'
    f'<span>CYKL <span class="hi">30 MIN</span></span>'
    f'<span class="sep">│</span>'
    f'<span>ŚLEDZONYCH <span class="hi">{wl_count}</span></span>'
    f'</div>',
    unsafe_allow_html=True,
)

if last_harvest_age is not None and last_harvest_age > MAX_DATA_AGE_H:
    st.markdown(
        f'<div class="stale-banner">⚠ HARVESTER OFFLINE — '
        f'ostatnie dane {last_harvest_age:.1f}h temu. '
        f'Ceny i sygnały wstrzymane. Uruchom harvester.py.</div>',
        unsafe_allow_html=True,
    )

tab_matrix, tab_chart, tab_portfolio = st.tabs([
    "  SIGNAL MATRIX  ", "  PRICE CHART  ", "  DUCHOWY PORTFEL  ",
])


# ─────────────────────────────────────────────────────────────────────────────
# TAB 1 — SIGNAL MATRIX
# ─────────────────────────────────────────────────────────────────────────────
with tab_matrix:
    watchlist = cached_get_watchlist()
    if not watchlist:
        st.info("Watchlist pusta. Dodaj itemy przez sidebar.")
    else:
        hide_danger = st.toggle("Ukryj LOW liquidity", value=True)

        rows = []
        for item in watchlist:
            latest     = cached_get_latest_price(item)
            item_stale = not latest or is_stale(latest.get("timestamp"))

            # Ceny — _safe_float gwarantuje None zamiast 0.0
            steam  = _safe_float(latest.get("steam_price"))   if latest else None
            skp    = _safe_float(latest.get("external_price")) if latest else None
            skp_ts = latest.get("timestamp")                  if latest else None
            vol    = latest.get("volume")                     if latest else None
            _, liq_tier, _ = liquidity_score(vol)

            # Sygnały — None jeśli brak danych
            fired, gap = is_wybuch(steam, skp, skp_ts)
            multiplier = calc_multiplier(steam, skp)
            breakeven  = calc_breakeven(steam)

            d3  = delta_info(item, 3)
            d24 = delta_info(item, 24)

            hist  = cached_get_price_history(item, 48)
            spark = [r["steam_price"] for r in hist if r.get("steam_price")] if hist else []

            rows.append({
                "ITEM":        item,
                "PRICE":       steam,
                "VOL":         vol,
                "_skp":        skp,
                "_gap":        gap,
                "_wybuch":     fired,
                "_multiplier": multiplier,
                "_breakeven":  breakeven,
                "_liq_tier":   liq_tier,
                "_d3":         d3,
                "_d24":        d24,
                "_d3_lbl":     d3["label"],
                "_d24_lbl":    d24["label"],
                "_spark":      spark,
                "_stale":      item_stale,
            })

        # Sortuj: WYBUCH na górze, potem po gap malejąco, None na dole
        rows.sort(key=lambda r: (r["_wybuch"], r["_gap"] or 0), reverse=True)
        visible = [r for r in rows if not (hide_danger and r["_liq_tier"] == "danger")]

        if not visible:
            st.caption("Wszystkie itemy odfiltrowane przez płynność.")
        else:
            render_wybuch_banners(visible)
            st.markdown("<div style='margin-top:6px'></div>", unsafe_allow_html=True)
            d3_lbl  = visible[0]["_d3_lbl"]
            d24_lbl = visible[0]["_d24_lbl"]
            render_price_matrix(visible, d3_lbl, d24_lbl)

        # KPI strip
        st.markdown("---")
        k = st.columns(5)
        n_live   = sum(1 for r in rows if r["PRICE"] and not r["_stale"])
        n_wybuch = sum(1 for r in rows if r["_wybuch"])
        n_skp    = sum(1 for r in rows if r["_skp"] is not None)
        n_hub    = sum(1 for r in rows if r["_multiplier"] is not None and r["_multiplier"] >= MULTI_GOLDEN)
        gap_vals = [r["_gap"] * 100 for r in rows if r["_gap"] is not None]
        avg_gap  = sum(gap_vals) / len(gap_vals) if gap_vals else None

        k[0].metric("LIVE DATA",       n_live)
        k[1].metric("⚡ WYBUCH",       n_wybuch)
        k[2].metric("💎 TRANSFER HUB", n_hub)
        k[3].metric("SKP COVERAGE",    f"{n_skp}/{len(watchlist)}")
        k[4].metric("AVG GAP",         f"{avg_gap:+.1f}%" if avg_gap is not None else "—")

        # Interaktywne wykresy
        st.markdown("---")
        st.markdown(
            '<div style="font-family:\'IBM Plex Sans Condensed\',sans-serif;font-size:0.60rem;'
            'font-weight:700;letter-spacing:0.14em;text-transform:uppercase;color:#3d5166;margin-bottom:6px">'
            'WYKRESY CENY · hover · scroll=zoom · drag=pan'
            '</div>',
            unsafe_allow_html=True,
        )
        for row in visible:
            d24_txt    = fmt_pct(row["_d24"]["pct"]) if not row["_d24"]["suppressed"] else "—"
            stale_tag  = " ⚠" if row["_stale"] else ""
            wybuch_tag = " ⚡" if row["_wybuch"] else ""
            gap_txt    = f'+{row["_gap"]*100:.1f}%' if row["_gap"] is not None else "—"
            price_str  = f'{row["PRICE"]:.2f} zł' if row["PRICE"] else "—"
            label = f"{row['ITEM']}   {price_str}   GAP {gap_txt}   Δ24h {d24_txt}{stale_tag}{wybuch_tag}"
            with st.expander(label, expanded=False):
                render_item_chart(row["ITEM"], height=300)


# ─────────────────────────────────────────────────────────────────────────────
# TAB 2 — PRICE CHART
# ─────────────────────────────────────────────────────────────────────────────
with tab_chart:
    watchlist = cached_get_watchlist()
    if not watchlist:
        st.info("Watchlist pusta.")
    else:
        sel = st.selectbox("WYBIERZ ITEM", options=watchlist, label_visibility="collapsed")
        render_item_chart(sel, height=460)


# ─────────────────────────────────────────────────────────────────────────────
# TAB 3 — DUCHOWY PORTFEL
# ─────────────────────────────────────────────────────────────────────────────
with tab_portfolio:
    st.markdown("## DUCHOWY PORTFEL — PAPER TRADING")
    st.caption(
        "P&L = ((Steam_now × 0.85) − cena_zakupu) × ilość  ·  prowizja Steam 15%"
    )

    watchlist = cached_get_watchlist()

    st.markdown("## OTWÓRZ POZYCJĘ")
    if not watchlist:
        st.info("Watchlist pusta.")
    else:
        with st.form("open_position_form", clear_on_submit=True):
            fc1, fc2, fc3 = st.columns([3, 2, 1])

            trade_item = fc1.selectbox("Item", options=watchlist, label_visibility="visible")

            current_lat   = cached_get_latest_price(trade_item)
            # _safe_float gwarantuje że default nie będzie 0.0 ani None-crash
            default_price = _safe_float(
                current_lat.get("steam_price") if current_lat else None
            ) or 0.01

            buy_price_input = fc2.number_input(
                "Cena zakupu (PLN)",
                min_value=0.01,
                value=float(round(default_price, 2)),
                step=0.01,
                format="%.2f",
            )

            quantity_input = fc3.number_input(
                "Ilość",
                min_value=1,
                max_value=9_999,
                value=1,
                step=1,
            )

            if current_lat and not is_stale(current_lat.get("timestamp")):
                stm = _safe_float(current_lat.get("steam_price"))
                if stm:
                    st.caption(
                        f"Aktualna cena Steam: **{stm:.2f} zł**  ·  "
                        f"Łączny koszt: **{buy_price_input * quantity_input:.2f} zł**"
                    )
            elif current_lat:
                age_h = age_hours(current_lat.get("timestamp"))
                st.warning(f"⚠ Dane stale ({age_h:.1f}h temu). Wpisz cenę ręcznie.")

            submitted = st.form_submit_button("⚡ OTWÓRZ POZYCJĘ")
            if submitted:
                if buy_price_input <= 0:
                    st.error("Cena zakupu musi być > 0.")
                else:
                    tid = open_trade(
                        trade_item,
                        buy_price=round(buy_price_input, 2),
                        quantity=int(quantity_input),
                    )
                    st.success(
                        f"✅ Pozycja #{tid}: "
                        f"{int(quantity_input)}× {trade_item} "
                        f"@ {buy_price_input:.2f} zł/szt  "
                        f"(koszt: {buy_price_input * quantity_input:.2f} zł)"
                    )
                    st.cache_data.clear()
                    st.rerun()

    # ── Otwarte pozycje ───────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("## OTWARTE POZYCJE")
    open_trades_raw = get_open_trades()
    # KLUCZOWE: konwersja sqlite3.Row → dict żeby .get() działało
    open_trades = [dict(t) for t in open_trades_raw]

    if not open_trades:
        st.caption("Brak otwartych pozycji.")
    else:
        def _pcls(v):
            if v is None or (isinstance(v, float) and not math.isfinite(v)):
                return "cell-nd"
            return "cell-up" if v >= 0 else "cell-dn"

        rows_t = []
        for t in open_trades:
            item = t["item_name"]
            buy  = t["buy_price"]
            qty  = t.get("quantity") or 1
            lat2 = cached_get_latest_price(item)
            now_p = _safe_float(lat2.get("steam_price")) if lat2 and not is_stale(lat2.get("timestamp")) else None
            if now_p is not None:
                pnl = ((now_p * STEAM_FEE) - buy) * qty
                roi = ((now_p * STEAM_FEE) - buy) / buy * 100 if buy > 0 else None
            else:
                pnl = roi = None
            rows_t.append({
                "id":     t["id"],
                "item":   item,
                "opened": ts_to_warsaw(parse_ts(t["timestamp"])),
                "buy":    buy,
                "qty":    qty,
                "now":    now_p,
                "pnl":    pnl,
                "roi":    roi,
            })

        hdr = (
            "<thead><tr>"
            "<th>ID</th><th>ITEM</th><th>OTWARTO (WAW)</th>"
            "<th style='text-align:right'>ILość</th>"
            "<th style='text-align:right'>ZAKUP/szt</th>"
            "<th style='text-align:right'>TERAZ</th>"
            "<th style='text-align:right'>P&amp;L (total)</th>"
            "<th style='text-align:right'>ROI %</th>"
            "</tr></thead>"
        )
        body_p = []
        for r in rows_t:
            now_s = f'{r["now"]:.2f} zł' if r["now"] is not None else "⚠ stale"
            pnl_s = (
                (f'+{r["pnl"]:.2f} zł' if r["pnl"] >= 0 else f'{r["pnl"]:.2f} zł')
                if r["pnl"] is not None else "—"
            )
            roi_s = (
                (f'+{r["roi"]:.2f}%' if r["roi"] >= 0 else f'{r["roi"]:.2f}%')
                if r["roi"] is not None else "—"
            )
            body_p.append(
                f'<tr>'
                f'<td class="cell-vol">{r["id"]}</td>'
                f'<td class="cell-item">{r["item"]}</td>'
                f'<td class="cell-vol">{r["opened"]}</td>'
                f'<td class="cell-qty"  style="text-align:right">{r["qty"]}</td>'
                f'<td class="cell-price"style="text-align:right">{r["buy"]:.2f} zł</td>'
                f'<td class="cell-price"style="text-align:right">{now_s}</td>'
                f'<td class="{_pcls(r["pnl"])}" style="text-align:right">{pnl_s}</td>'
                f'<td class="{_pcls(r["roi"])}" style="text-align:right;font-weight:600">{roi_s}</td>'
                f'</tr>'
            )
        st.markdown(
            f'<div style="overflow-x:auto;border:1px solid #14202e;background:#080b0f">'
            f'<table class="price-matrix">{hdr}<tbody>{"".join(body_p)}</tbody></table></div>',
            unsafe_allow_html=True,
        )

        st.markdown("## ZAMKNIJ POZYCJĘ")
        cc1, cc2 = st.columns([2, 2])
        close_id = cc1.selectbox(
            "ID pozycji",
            options=[t["id"] for t in open_trades],
            label_visibility="collapsed",
            key="close_sel",
        )
        if cc2.button("✗ ZAMKNIJ"):
            close_trade(close_id)
            st.cache_data.clear()
            st.success(f"Pozycja #{close_id} zamknięta.")
            st.rerun()

    # ── Podsumowanie portfela ─────────────────────────────────────────────────
    if open_trades:
        total_cost  = sum(t["buy_price"] * (t.get("quantity") or 1) for t in open_trades)
        total_value = 0.0
        for t in open_trades:
            qty  = t.get("quantity") or 1
            lat3 = cached_get_latest_price(t["item_name"])
            now_p3 = _safe_float(lat3.get("steam_price")) if lat3 and not is_stale(lat3.get("timestamp")) else None
            if now_p3 is not None:
                total_value += now_p3 * STEAM_FEE * qty
            else:
                total_value += t["buy_price"] * qty   # fallback: koszt nabycia

        total_pnl = total_value - total_cost
        total_roi = total_pnl / total_cost * 100 if total_cost else 0

        st.markdown("---")
        st.markdown("## PODSUMOWANIE PORTFELA")
        s = st.columns(4)
        s[0].metric("POZYCJE",          len(open_trades))
        s[1].metric("KOSZT CAŁKOWITY",  f"{total_cost:.2f} zł")
        s[2].metric("WARTOŚĆ (po 15%)", f"{total_value:.2f} zł")
        sign = "+" if total_roi >= 0 else ""
        s[3].metric("TOTAL P&L",        f"{sign}{total_pnl:.2f} zł",
                    delta=f"{sign}{total_roi:.2f}%")

    # ── Historia zamkniętych ──────────────────────────────────────────────────
    closed_raw = get_closed_trades()
    closed = [dict(t) for t in closed_raw]   # sqlite3.Row → dict
    if closed:
        st.markdown("---")
        with st.expander("HISTORIA ZAMKNIĘTYCH"):
            hdr_c = (
                "<thead><tr><th>ID</th><th>ITEM</th>"
                "<th style='text-align:right'>ILość</th>"
                "<th style='text-align:right'>ZAKUP/szt</th>"
                "<th>OTWARTO (WAW)</th></tr></thead>"
            )
            body_c = "".join(
                f'<tr>'
                f'<td class="cell-vol">{t["id"]}</td>'
                f'<td class="cell-item">{t["item_name"]}</td>'
                f'<td class="cell-qty"  style="text-align:right">{t.get("quantity") or 1}</td>'
                f'<td class="cell-price"style="text-align:right">{t["buy_price"]:.2f} zł</td>'
                f'<td class="cell-vol">{ts_to_warsaw(parse_ts(t["timestamp"]))}</td>'
                f'</tr>'
                for t in closed
            )
            st.markdown(
                f'<div style="overflow-x:auto;border:1px solid #14202e;background:#080b0f">'
                f'<table class="price-matrix">{hdr_c}<tbody>{body_c}</tbody></table></div>',
                unsafe_allow_html=True,
            )
