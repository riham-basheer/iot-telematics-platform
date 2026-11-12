import base64
import sys
from pathlib import Path
import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.db_connection import get_db_engine


# ---------------------------------
# 1. Design System & Theme-Adaptive CSS
# ---------------------------------
def apply_custom_styles():
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@500;600;700;800&family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@500;600;700&display=swap');

        .stApp { font-family: 'Inter', sans-serif; }
        h1, h2, h3, h4, h5, h6 {
            font-family: 'Plus Jakarta Sans', sans-serif !important;
            letter-spacing: -0.025em;
            color: var(--text-color);
        }

        /* --- Top Metric Cards --- */
        .metric-card {
            background-color: var(--secondary-background-color);
            border: 1px solid rgba(128, 128, 128, 0.22);
            border-radius: 12px;
            padding: 16px 18px;
            height: 100%;
            display: flex;
            flex-direction: column;
            justify-content: space-between;
            transition: all 0.2s ease;
        }
        .metric-card:hover {
            transform: translateY(-2px);
            box-shadow: 0 8px 20px rgba(0, 0, 0, 0.12);
            border-color: rgba(128, 128, 128, 0.45);
        }
        .metric-card-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        .card-label {
            font-family: 'Plus Jakarta Sans', sans-serif;
            font-size: 0.78rem;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            color: var(--text-color);
            opacity: 0.75;
        }
        .card-value {
            font-family: 'JetBrains Mono', monospace;
            font-size: 1.85rem;
            font-weight: 800;
            line-height: 1.2;
            letter-spacing: -0.03em;
            margin: 6px 0 2px 0;
            color: var(--text-color);
        }
        .card-subtext {
            font-size: 0.82rem;
            color: var(--text-color);
            opacity: 0.75;
            line-height: 1.4;
        }
        .card-badge {
            font-family: 'Plus Jakarta Sans', sans-serif;
            font-size: 0.70rem;
            font-weight: 700;
            padding: 3px 8px;
            border-radius: 5px;
            letter-spacing: 0.06em;
            text-transform: uppercase;
            white-space: nowrap;
        }

        /* Compact Powertrain Legend */
        .pt-row {
            display: flex;
            align-items: center;
            gap: 10px;
            flex-wrap: wrap;
            margin-top: 4px;
            font-family: 'JetBrains Mono', monospace;
            font-size: 0.78rem;
        }
        .pt-item {
            display: inline-flex;
            align-items: center;
            gap: 5px;
            color: var(--text-color);
            opacity: 0.85;
        }
        .pt-dot {
            width: 8px;
            height: 8px;
            border-radius: 50%;
            display: inline-block;
        }
        .dot-ice  { background-color: #64748B; }
        .dot-hev  { background-color: #2563EB; }
        .dot-phev { background-color: #7C3AED; }
        .dot-ev   { background-color: #059669; }

        /* --- Cockpit Cards Container Styling --- */
        div[data-testid="stVerticalBlockBorderWrapper"] {
            border-radius: 14px !important;
            border: 1px solid rgba(128, 128, 128, 0.22) !important;
            background-color: var(--secondary-background-color) !important;
            transition: all 0.25s ease !important;
            padding: 2px !important;
        }
        div[data-testid="stVerticalBlockBorderWrapper"]:hover {
            border-color: rgba(128, 128, 128, 0.45) !important;
            box-shadow: 0 10px 24px rgba(0, 0, 0, 0.12) !important;
            transform: translateY(-2px) !important;
        }

        .portal-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 12px;
        }
        /* Scaled-up prominent icon badge */
        .portal-icon-wrapper {
            width: 58px;
            height: 58px;
            border-radius: 14px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 1.75rem;
            box-shadow: 0 4px 14px rgba(0, 0, 0, 0.08);
            border: 1px solid rgba(128, 128, 128, 0.18);
        }
        .portal-title {
            font-size: 1.18rem;
            font-weight: 700;
            color: var(--text-color);
            margin-bottom: 4px;
        }
        .portal-desc {
            font-size: 0.90rem;
            color: var(--text-color);
            opacity: 0.75;
            line-height: 1.45;
            margin-bottom: 14px;
        }

        /* Scannable Feature Chips */
        .chip-container {
            display: flex;
            flex-wrap: wrap;
            gap: 7px;
            margin-bottom: 16px;
        }
        .feature-chip {
            font-size: 0.78rem;
            font-weight: 600;
            padding: 4px 10px;
            border-radius: 6px;
            background: rgba(128, 128, 128, 0.12);
            border: 1px solid rgba(128, 128, 128, 0.22);
            color: var(--text-color);
            opacity: 0.90;
        }

        /* --- Catchy Themed Action CTA Buttons --- */
        div[data-testid="stPageLink"] a {
            border-radius: 9px !important;
            padding: 11px 18px !important;
            display: flex !important;
            justify-content: center !important;
            align-items: center !important;
            text-decoration: none !important;
            transition: all 0.25s cubic-bezier(0.4, 0, 0.2, 1) !important;
            border: 1px solid rgba(255, 255, 255, 0.2) !important;
        }
        div[data-testid="stPageLink"] a p {
            color: #FFFFFF !important;
            font-weight: 700 !important;
            font-size: 0.90rem !important;
            letter-spacing: 0.02em !important;
            margin: 0 !important;
        }

        /* Column 1: Blue Button */
        div[data-testid="column"]:nth-of-type(1) div[data-testid="stPageLink"] a {
            background: linear-gradient(135deg, #2563EB 0%, #1D4ED8 100%) !important;
            box-shadow: 0 4px 14px rgba(37, 99, 235, 0.40) !important;
        }
        div[data-testid="column"]:nth-of-type(1) div[data-testid="stPageLink"] a:hover {
            background: linear-gradient(135deg, #3B82F6 0%, #2563EB 100%) !important;
            box-shadow: 0 6px 20px rgba(37, 99, 235, 0.65) !important;
            transform: translateY(-2px) !important;
        }

        /* Column 2: Emerald Green Button */
        div[data-testid="column"]:nth-of-type(2) div[data-testid="stPageLink"] a {
            background: linear-gradient(135deg, #059669 0%, #047857 100%) !important;
            box-shadow: 0 4px 14px rgba(5, 150, 105, 0.40) !important;
        }
        div[data-testid="column"]:nth-of-type(2) div[data-testid="stPageLink"] a:hover {
            background: linear-gradient(135deg, #10B981 0%, #059669 100%) !important;
            box-shadow: 0 6px 20px rgba(5, 150, 105, 0.65) !important;
            transform: translateY(-2px) !important;
        }

        .header-tag {
            display: inline-block;
            background: rgba(37, 99, 235, 0.15);
            color: #2563EB;
            padding: 5px 12px;
            font-weight: 700;
            font-size: 0.74rem;
            border-radius: 6px;
            border: 1px solid rgba(37, 99, 235, 0.35);
            letter-spacing: 0.05em;
        }
        .academy-logo-placeholder {
            width: 48px;
            height: 48px;
            background: linear-gradient(135deg, #2563EB 0%, #7C3AED 100%);
            color: #FFFFFF;
            font-weight: 800;
            font-size: 1.15rem;
            display: flex;
            align-items: center;
            justify-content: center;
            border-radius: 8px;
            box-shadow: 0 4px 12px rgba(37, 99, 235, 0.25);
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


# ---------------------------------
# 2. Icon Loader Helper (Larger Scale)
# ---------------------------------
def render_icon(icon_path: Path, fallback_emoji: str) -> str:
    """Renders custom image icon scaled to 38px if found; otherwise displays fallback emoji."""
    if icon_path.is_file():
        try:
            suffix = icon_path.suffix.lower()
            mime = "image/svg+xml" if suffix == ".svg" else f"image/{suffix.lstrip('.')}"
            encoded = base64.b64encode(icon_path.read_bytes()).decode()
            return f'<img src="data:{mime};base64,{encoded}" style="width: 38px; height: 38px; object-fit: contain;" />'
        except Exception:
            pass
    return f'<span style="font-size: 1.75rem;">{fallback_emoji}</span>'


# ---------------------------------
# 3. Data Retrieval
# ---------------------------------
@st.cache_data(ttl=60)
def fetch_system_metrics():
    """Queries Postgres for trip counts, fleet size, and active date boundaries."""
    engine = get_db_engine()
    trips = pd.read_sql(
        "SELECT COUNT(*) FROM fact_trips_summary;", con=engine
    ).iloc[0, 0]
    vehicles = pd.read_sql("SELECT COUNT(*) FROM dim_vehicle;", con=engine).iloc[
        0, 0
    ]
    prices = pd.read_sql(
        "SELECT COUNT(*) FROM dim_energy_pricing_history;", con=engine
    ).iloc[0, 0]
    dates = pd.read_sql(
        "SELECT MIN(trip_start_time) AS min_ts, MAX(trip_end_time) AS max_ts FROM fact_trips_summary;",
        con=engine,
    ).iloc[0]

    min_ts, max_ts = pd.to_datetime(dates["min_ts"]), pd.to_datetime(
        dates["max_ts"]
    )
    if pd.notnull(min_ts) and pd.notnull(max_ts):
        days = max(1, (max_ts.date() - min_ts.date()).days + 1)
        duration_val = f"{days:,}"
        date_range_str = f"Days Active • {min_ts.strftime('%b %d')} – {max_ts.strftime('%b %d, %Y')}"
    else:
        duration_val, date_range_str = "0", "Awaiting Batch Sync"

    return trips, vehicles, prices, duration_val, date_range_str


# ---------------------------------
# 4. Clean Metric & Pill Renderers
# ---------------------------------
def render_metric_card(
    label: str, value: str, subtext_html: str, tag: str, color: str
):
    st.markdown(
        f"""
        <div class="metric-card" style="border-top: 3.5px solid {color};">
            <div class="metric-card-header">
                <span class="card-label">{label}</span>
                <span class="card-badge" style="background-color: {color}22; color: {color}; border: 1px solid {color}55;">{tag}</span>
            </div>
            <div class="card-value">{value}</div>
            <div class="card-subtext">{subtext_html}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_powertrain_line(ice=264, hev=93, phev=24, ev=3) -> str:
    return f"""
    <div class="pt-row">
        <span class="pt-item"><span class="pt-dot dot-ice"></span><strong>{ice}</strong> ICE</span>
        <span class="pt-item"><span class="pt-dot dot-hev"></span><strong>{hev}</strong> HEV</span>
        <span class="pt-item"><span class="pt-dot dot-phev"></span><strong>{phev}</strong> PHEV</span>
        <span class="pt-item"><span class="pt-dot dot-ev"></span><strong>{ev}</strong> EV</span>
    </div>
    """


# ---------------------------------
# 5. View Sections
# ---------------------------------
def render_header():
    left, right = st.columns([0.82, 0.18])
    with left:
        st.title("Ann Arbor Fleet Intelligence Platform")
        st.caption(
            "Transforming high-frequency automotive telemetry into verified cost savings, "
            "emissions benchmarks, and municipal electrification planning."
        )
    with right:
        st.markdown(
            '<div style="text-align: right; padding-top: 10px;"><span class="header-tag">FLEET LAKEHOUSE</span></div>',
            unsafe_allow_html=True,
        )
    st.divider()


def render_executive_kpis():
    try:
        trips, vehicles, prices, duration_val, dates_str = (
            fetch_system_metrics()
        )
        cards = [
            (
                "Monitored Fleet Assets",
                f"{vehicles:,}",
                render_powertrain_line(),
                "FLEET ASSETS",
                "#2563EB",
            ),
            (
                "Analyzed Trip Cycles",
                f"{trips:,}",
                "Integrated distance, fuel & energy",
                "PROCESSED TRIPS",
                "#0891B2",
            ),
            (
                "Retail Energy Benchmarks",
                f"{prices:,}",
                "Weekly EIA gas & grid tariffs matched",
                "ENERGY TARIFFS",
                "#D97706",
            ),
            (
                "Operational Window",
                duration_val,
                dates_str,
                "TEMPORAL RANGE",
                "#7C3AED",
            ),
        ]
        cols = st.columns(4)
        for col, (label, val, subtext, tag, color) in zip(cols, cards):
            with col:
                render_metric_card(label, val, subtext, tag, color)
    except Exception as err:
        st.error(f"PostgreSQL metadata catalog unreachable: {err}")

    st.write("")


def render_navigation_portals():
    st.markdown("### Where to Next?")
    st.caption("Choose a page below to explore the data.")

    portals = [
        {
            "icon_path": Path("streamlit_app/assets/icons/executive_icon.png"),
            "fallback_icon": "⚡",
            "icon_bg": "rgba(37, 99, 235, 0.15)",
            "title": "Executive Analytics & ESG",
            "badge": "COSTS & ESG",
            "badge_color": "#2563EB",
            "desc": "Track vehicle operating costs, fuel waste from idling, and carbon emissions.",
            "chips": [
                "Fleet Costs",
                "Idling Waste",
                "CO₂ Avoided",
                "Winter HVAC",
            ],
            "page": "pages/2_Executive_Analytics.py",
            "btn_label": "Open Executive Analytics →",
        },
        {
            "icon_path": Path("streamlit_app/assets/icons/spatial_icon.png"),
            "fallback_icon": "🗺️",
            "icon_bg": "rgba(16, 185, 129, 0.15)",
            "title": "Spatial Infrastructure & Depots",
            "badge": "MAPS & CHARGING",
            "badge_color": "#059669",
            "desc": "Map trip traffic bottlenecks, public chargers, and depot placement.",
            "chips": [
                "Traffic Heatmaps",
                "EV Chargers",
                "Depot Siting",
                "EV Feasibility",
            ],
            "page": "pages/3_Spatial_Infrastructure.py",
            "btn_label": "Open Spatial Infrastructure →",
        },
    ]

    cols = st.columns(len(portals), gap="medium")
    for col, p in zip(cols, portals):
        with col:
            with st.container(border=True):
                icon_html = render_icon(p["icon_path"], p["fallback_icon"])
                chips_html = "".join(
                    f'<span class="feature-chip">{c}</span>' for c in p["chips"]
                )
                st.markdown(
                    f"""
                    <div class="portal-header">
                        <div class="portal-icon-wrapper" style="background-color: {p['icon_bg']};">
                            {icon_html}
                        </div>
                        <span class="card-badge" style="background-color: {p['badge_color']}22; color: {p['badge_color']}; border: 1px solid {p['badge_color']}55;">
                            {p['badge']}
                        </span>
                    </div>
                    <div class="portal-title">{p['title']}</div>
                    <div class="portal-desc">{p['desc']}</div>
                    <div class="chip-container">{chips_html}</div>
                    """,
                    unsafe_allow_html=True,
                )
                st.page_link(
                    p["page"], label=p["btn_label"], use_container_width=True
                )

    st.write("")
    st.divider()


def render_lakehouse_architecture():
    st.markdown("### How the Lakehouse Works")
    st.caption(
        "A 3-stage Medallion pipeline turning raw vehicle pings into actionable insights."
    )

    steps = [
        (
            "🥉 BRONZE",
            "Raw Telemetry",
            "Ingests 1 Hz speed, fuel flow, battery voltage, current, and GPS coordinates.",
        ),
        (
            "🥈 SILVER",
            "Cleaned & Enriched",
            "Removes sensor errors, integrates distance & energy, and joins local weather.",
        ),
        (
            "🥇 GOLD",
            "Analytics & Tariffs",
            "Applies weekly energy prices to calculate financial spend and CO₂ reductions.",
        ),
    ]

    cols = st.columns(3, gap="medium")
    for col, (stage, title, caption) in zip(cols, steps):
        with col:
            with st.container(border=True):
                st.markdown(f"**{stage}**")
                st.markdown(f"**{title}**")
                st.caption(caption)

    st.divider()


def render_footer():
    left, right = st.columns([0.60, 0.40], gap="large")
    with left:
        st.markdown("##### Research & Data Sources")
        st.markdown(
            """
            - **Vehicle Energy Dataset (VED)**: Naturalistic driving dataset in Ann Arbor, Michigan.  
              *Citation*: G. S. Oh, D. J. Leblanc, and H. Peng, *IEEE Transactions on Intelligent Transportation Systems*. [GitHub Repository](https://github.com/gsoh/VED)
            - **Meteorological Observations**: NOAA ASOS Station **KARB** (Ann Arbor Municipal Airport) via NOAA NCEI.
            - **Energy Pricing Feeds**: Retail gasoline & commercial grid tariffs via [U.S. Energy Information Administration (EIA)](https://www.eia.gov/opendata/).
            - **Charging Infrastructure**: Alternative Fuel Data Center registry via [NREL Developer Network](https://developer.nrel.gov/).
            """
        )

    with right:
        st.markdown("##### Engineering & Institutional Attribution")
        logo_path = Path("streamlit_app/assets/logo.jpg")

        l_col, r_col = st.columns([0.28, 1.7])
        with l_col:
            if logo_path.is_file():
                st.image(str(logo_path), width=50)
            else:
                st.markdown(
                    '<div class="academy-logo-placeholder">DE</div>',
                    unsafe_allow_html=True,
                )

        with r_col:
            st.markdown(
                """
                **Riham Elsayed**  
                *Data Engineer*  
                <span style="font-size: 0.85rem; opacity: 0.85;">
                <strong>Academy</strong>: Andalusia Academy<br>
                <strong>Program</strong>: Train to Hire - Data Engineering Track
                </span>
                """,
                unsafe_allow_html=True,
            )


# ---------------------------------
# Main Application Entry Point
# ---------------------------------
if __name__ == "__main__":
    st.set_page_config(
        page_title="Ann Arbor Fleet Intelligence",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    apply_custom_styles()
    render_header()
    render_executive_kpis()
    render_navigation_portals()
    render_lakehouse_architecture()
    render_footer()