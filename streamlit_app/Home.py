import sys
from pathlib import Path
import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.db_connection import get_db_engine


# ---------------------------------
# 1. Design System & CSS Injection
# ---------------------------------
def apply_custom_styles():
    """Injects custom typography, hover transitions, and pill badge styling.
    These dont happen in a native Streamlit API, so I use raw HTML/CSS injection via st.markdown() with unsafe_allow_html=True.
    """
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@500;600;700;800&family=Inter:wght@400;500;600&family=JetBrains+Mono:wght@500;600;700&display=swap');

        .stApp { font-family: 'Inter', sans-serif; }
        h1, h2, h3, h4, h5, h6 {
            font-family: 'Plus Jakarta Sans', sans-serif !important;
            letter-spacing: -0.025em;
            color: var(--text-color);
        }

        /* Metric Cards & Hover Elevations */
        .metric-card {
            background-color: var(--secondary-background-color);
            border: 1px solid rgba(128, 128, 128, 0.2);
            border-radius: 10px;
            padding: 18px 22px;
            height: 100%;
            display: flex;
            flex-direction: column;
            justify-content: space-between;
            transition: all 0.2s ease;
        }
        .metric-card:hover {
            transform: translateY(-3px);
            box-shadow: 0 10px 24px rgba(0, 0, 0, 0.12);
            border-color: rgba(128, 128, 128, 0.4);
        }
        .metric-card-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 6px;
        }
        .card-label {
            font-family: 'Plus Jakarta Sans', sans-serif;
            font-size: 0.72rem;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            opacity: 0.8;
            color: var(--text-color);
        }
        .card-value {
            font-family: 'JetBrains Mono', monospace;
            font-size: 1.95rem;
            font-weight: 700;
            line-height: 1.2;
            letter-spacing: -0.03em;
            color: var(--text-color);
            margin: 4px 0 8px 0;
        }
        .card-subtext {
            font-size: 0.82rem;
            opacity: 0.8;
            line-height: 1.4;
            color: var(--text-color);
        }
        .card-badge {
            font-family: 'Plus Jakarta Sans', sans-serif;
            font-size: 0.68rem;
            font-weight: 700;
            padding: 3px 8px;
            border-radius: 5px;
            letter-spacing: 0.06em;
            text-transform: uppercase;
        }

        /* Powertrain Pill Rectangles */
        .pt-badge-row { display: flex; gap: 6px; flex-wrap: wrap; margin-top: 6px; }
        .pt-badge {
            font-family: 'JetBrains Mono', monospace;
            font-size: 0.74rem;
            font-weight: 600;
            padding: 3px 8px;
            border-radius: 6px;
            letter-spacing: 0.02em;
            display: inline-flex;
            align-items: center;
            gap: 5px;
            color: var(--text-color);
        }
        .pt-badge strong { font-weight: 800; }
        .pt-ice  { background: rgba(148, 163, 184, 0.16); border: 1.5px solid rgba(148, 163, 184, 0.55); }
        .pt-hev  { background: rgba(37, 99, 235, 0.16);   border: 1.5px solid rgba(37, 99, 235, 0.55); }
        .pt-phev { background: rgba(124, 58, 237, 0.16);  border: 1.5px solid rgba(124, 58, 237, 0.55); }
        .pt-ev   { background: rgba(16, 185, 129, 0.16);  border: 1.5px solid rgba(16, 185, 129, 0.55); }

        /* Action CTA Navigation Buttons */
        div[data-testid="stPageLink-NavLink"] {
            background-color: var(--secondary-background-color) !important;
            border: 1.5px solid #2563EB !important;
            border-radius: 8px !important;
            padding: 10px 16px !important;
            transition: all 0.2s ease !important;
            display: flex !important;
            justify-content: center !important;
            align-items: center !important;
            margin-top: 10px !important;
        }
        div[data-testid="stPageLink-NavLink"]:hover {
            background-color: #2563EB !important;
            box-shadow: 0 4px 14px rgba(37, 99, 235, 0.3) !important;
        }
        div[data-testid="stPageLink-NavLink"] p {
            color: var(--text-color) !important;
            font-weight: 700 !important;
            font-size: 0.90rem !important;
            margin: 0 !important;
        }
        div[data-testid="stPageLink-NavLink"]:hover p { color: #FFFFFF !important; }

        .header-tag {
            display: inline-block;
            background: rgba(37, 99, 235, 0.15);
            color: #2563EB;
            padding: 5px 12px;
            font-weight: 700;
            font-size: 0.75rem;
            border-radius: 6px;
            border: 1px solid rgba(37, 99, 235, 0.35);
            letter-spacing: 0.05em;
        }
        .academy-logo-placeholder {
            width: 52px;
            height: 52px;
            background: linear-gradient(135deg, #2563EB 0%, #7C3AED 100%);
            color: #FFFFFF;
            font-weight: 800;
            font-size: 1.25rem;
            display: flex;
            align-items: center;
            justify-content: center;
            border-radius: 10px;
            box-shadow: 0 4px 12px rgba(37, 99, 235, 0.25);
        }
        </style>
        """,
        unsafe_allow_html=True
    )


# --------------------------------- #
# 2. Data Retrieval
# --------------------------------- #
@st.cache_data(ttl=60)
def fetch_system_metrics():
    """Queries Postgres for trip counts, fleet size, and active date boundaries."""
    engine = get_db_engine()
    trips = pd.read_sql("SELECT COUNT(*) FROM fact_trips_summary;", con=engine).iloc[0, 0]
    vehicles = pd.read_sql("SELECT COUNT(*) FROM dim_vehicle;", con=engine).iloc[0, 0]
    prices = pd.read_sql("SELECT COUNT(*) FROM dim_energy_pricing_history;", con=engine).iloc[0, 0]
    dates = pd.read_sql(
        "SELECT MIN(trip_start_time) AS min_ts, MAX(trip_end_time) AS max_ts FROM fact_trips_summary;",
        con=engine
    ).iloc[0]

    min_ts, max_ts = pd.to_datetime(dates["min_ts"]), pd.to_datetime(dates["max_ts"])
    if pd.notnull(min_ts) and pd.notnull(max_ts):
        days = max(1, (max_ts.date() - min_ts.date()).days + 1)
        duration_label = f"{days} Days Active"
        date_range_str = f"{min_ts.strftime('%b %d')} – {max_ts.strftime('%b %d, %Y')}"
    else:
        duration_label, date_range_str = "0 Days", "Awaiting Batch Sync"

    return trips, vehicles, prices, duration_label, date_range_str


# ---------------------------------
# 3. Reusable UI Components
# ---------------------------------
def render_metric_card(label: str, value: str, subtext: str, tag: str, color: str):
    """Renders a single styled metric card with top accent border."""
    st.markdown(
        f"""
        <div class="metric-card" style="border-top: 4px solid {color};">
            <div class="metric-card-header">
                <span class="card-label">{label}</span>
                <span class="card-badge" style="background-color: {color}1f; color: {color}; border: 1px solid {color}4d;">{tag}</span>
            </div>
            <div class="card-value">{value}</div>
            <div class="card-subtext">{subtext}</div>
        </div>
        """,
        unsafe_allow_html=True
    )


def render_powertrain_pills(ice=264, hev=93, phev=24, ev=3) -> str:
    """Builds the HTML snippet for powertrain asset badge pills."""
    return f"""
    <div class="pt-badge-row">
        <span class="pt-badge pt-ice"><strong>{ice}</strong> ICE</span>
        <span class="pt-badge pt-hev"><strong>{hev}</strong> HEV</span>
        <span class="pt-badge pt-phev"><strong>{phev}</strong> PHEV</span>
        <span class="pt-badge pt-ev"><strong>{ev}</strong> EV</span>
    </div>
    """


# ---------------------------------
# 4. View Sections
# ---------------------------------
def render_header():
    left, right = st.columns([0.82, 0.18])
    with left:
        st.title("Ann Arbor Fleet Intelligence Platform")
        st.caption(
            "Turn high-frequency vehicular telemetry into verified financial savings, "
            "tailpipe carbon displacement, and grid electrification feasibility."
        )
    with right:
        st.markdown(
            '<div style="text-align: right; padding-top: 14px;"><span class="header-tag">FLEET LAKEHOUSE</span></div>',
            unsafe_allow_html=True
        )
    st.divider()


def render_executive_kpis():
    try:
        trips, vehicles, prices, duration_str, dates_str = fetch_system_metrics()
        cards = [
            ("Monitored Fleet Assets", f"{vehicles:,}", render_powertrain_pills(), "DIM_VEHICLE", "#2563EB"),
            ("Analyzed Trip Cycles", f"{trips:,}", "Integrated distance, fuel & energy", "GOLD LAKE", "#0891B2"),
            ("Retail Energy Benchmarks", f"{prices:,}", "Weekly EIA gas & grid tariffs matched", "EIA PRICING", "#D97706"),
            ("Operational Window", duration_str, dates_str, "FALL 2017", "#7C3AED"),
        ]
        cols = st.columns(4)
        for col, (label, val, subtext, tag, color) in zip(cols, cards):
            with col:
                render_metric_card(label, val, subtext, tag, color)
    except Exception as err:
        st.error(f"PostgreSQL metadata catalog unreachable: {err}")

    st.write("")


def render_navigation_portals():
    st.markdown("### Where Would You Like to Go?")
    st.caption("Select an operational cockpit to explore insights from the telematics lakehouse.")

    portals = [
        {
            "title": "Executive Analytics & ESG",
            "badge": ":blue-background[**RECOMMENDED**]",
            "bullets": [
                "**Total Cost of Ownership (TCO)**: Compare operating cost ($/100 km) across gas, hybrid, and electric powertrains.",
                "**Capital Waste Discovery**: Quantify fuel and dollar losses caused by stationary engine idling.",
                "**Carbon Displacement (ESG)**: Track net kilograms of CO₂ avoided compared to an all-gasoline baseline.",
                "**Cold-Weather Impact**: Measure battery drain and HVAC power draws during Michigan winters.",
            ],
            "page": "pages/2_Executive_Analytics.py",
            "btn_label": "Open Executive Analytics →",
        },
        {
            "title": "Spatial Infrastructure & Depots",
            "badge": ":green-background[**PLANNING**]",
            "bullets": [
                "**Idling Bottleneck Heatmaps**: Identify high-congestion intersections wasting driver hours and fuel.",
                "**Corridor Prioritization**: Rank road corridors by wasted fuel volume to prioritize traffic interventions.",
                "**Depot Siting**: Overlay fleet stopping patterns against public NREL charging locations to pinpoint charging deserts.",
                "**Electrification Feasibility**: Determine which routes have duty cycles best suited for pure EV replacement.",
            ],
            "page": "pages/3_Spatial_Infrastructure.py",
            "btn_label": "Open Spatial Infrastructure →",
        },
    ]

    cols = st.columns(2, gap="large")
    for col, p in zip(cols, portals):
        with col:
            with st.container(border=True):
                head_col, badge_col = st.columns([0.78, 0.22])
                with head_col:
                    st.markdown(f"#### {p['title']}")
                with badge_col:
                    st.markdown(p["badge"])

                st.markdown("Make data-driven procurement and sustainability decisions with fleet-level rollups:" if "Executive" in p["title"] else "Plan municipal charging investments and target route congestion:")
                st.markdown("\n".join(f"- {b}" for b in p["bullets"]))
                st.page_link(p["page"], label=p["btn_label"], use_container_width=True)

    st.divider()


def render_lakehouse_architecture():
    st.markdown("### How the Lakehouse Works")
    st.caption("A streamlined Medallion pipeline transforming raw automotive sensors into executive decisions.")

    steps = [
        (":orange-background[**1. RAW SENSORS**]", "**1 Hz CAN-bus Telemetry**", "Ingests continuous high-frequency streams: vehicle speed, OBD-II fuel flow, battery pack voltage & current, and GPS coordinate breadcrumbs."),
        (":gray-background[**2. ENRICHMENT**]", "**Weather & Physics Integrals**", "Cleans dropouts, integrates distance via trapezoidal Riemann sums, resolves stoichiometric fuel from MAF sensors, and joins NOAA airport weather."),
        (":green-background[**3. INTELLIGENCE**]", "**Point-in-Time Cost & ESG**", "Matches weekly EIA Michigan retail gas prices and commercial electricity tariffs to calculate true financial expenditure and net CO₂ displacement."),
    ]

    cols = st.columns(3, gap="medium")
    for col, (badge, title, caption) in zip(cols, steps):
        with col:
            with st.container(border=True):
                st.markdown(badge)
                st.markdown(title)
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
            - **Meteorological Observations**: NOAA ASOS Station **KARB** (Ann Arbor Municipal Airport) via NOAA National Centers for Environmental Information (NCEI).
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
                st.image(str(logo_path), width=55)
            else:
                st.markdown('<div class="academy-logo-placeholder">DE</div>', unsafe_allow_html=True)

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
                unsafe_allow_html=True
            )


# ---------------------------------
# Main Application Entry Point
# ---------------------------------
if __name__ == "__main__":
    st.set_page_config(
            page_title="Ann Arbor Fleet Intelligence",
            layout="wide",
            initial_sidebar_state="expanded"
        )
    apply_custom_styles()
    render_header()
    render_executive_kpis()
    render_navigation_portals()
    render_lakehouse_architecture()
    render_footer()
