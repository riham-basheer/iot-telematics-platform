import sys
from pathlib import Path
import pandas as pd
import streamlit as st

# Resolve repository root
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.db_connection import get_db_engine

st.set_page_config(
    page_title="Executive Analytics & ESG",
    layout="wide",
    initial_sidebar_state="expanded"
)

POWERTRAIN_LABELS = {
    "ICE": "ICE (Gasoline)",
    "HEV": "HEV (Hybrid Electric)",
    "PHEV": "PHEV (Plug-in Hybrid)",
    "EV": "EV (All-Electric)",
}

# -------------------------------------- #
#  Design System & Typography
# -------------------------------------- #
def apply_page_styles():
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

        /* 6 KPI Metric Cards */
        .kpi-card {
            background-color: var(--secondary-background-color);
            border: 1px solid rgba(128, 128, 128, 0.2);
            border-radius: 10px;
            padding: 16px 18px;
            height: 100%;
            display: flex;
            flex-direction: column;
            justify-content: space-between;
            transition: all 0.2s ease;
        }
        .kpi-card:hover {
            transform: translateY(-3px);
            box-shadow: 0 8px 20px rgba(0, 0, 0, 0.12);
            border-color: rgba(128, 128, 128, 0.4);
        }
        .kpi-card-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 6px;
        }
        .kpi-label {
            font-family: 'Plus Jakarta Sans', sans-serif;
            font-size: 0.70rem;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            opacity: 0.8;
            color: var(--text-color);
        }
        .kpi-value {
            font-family: 'JetBrains Mono', monospace;
            font-size: 1.65rem;
            font-weight: 700;
            line-height: 1.2;
            letter-spacing: -0.03em;
            color: var(--text-color);
            margin: 4px 0 6px 0;
        }
        .kpi-subtext {
            font-size: 0.78rem;
            opacity: 0.75;
            line-height: 1.4;
            color: var(--text-color);
        }
        .kpi-badge {
            font-family: 'Plus Jakarta Sans', sans-serif;
            font-size: 0.65rem;
            font-weight: 700;
            padding: 2px 7px;
            border-radius: 4px;
            letter-spacing: 0.05em;
            text-transform: uppercase;
        }

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
        </style>
        """,
        unsafe_allow_html=True
    )


# -------------------------------------- #
#Data Retrieval Layer
# -------------------------------------- #
@st.cache_data(ttl=60)
def load_fact_trips_data():
    """Loads fact_trips_summary joined with dim_vehicle from PostgreSQL."""
    query = """
        SELECT 
            f.trip_id,
            f.veh_id,
            f.engine_type,
            f.season_category,
            f.trip_start_time,
            f.trip_end_time,
            f.duration_minutes,
            f.distance_km,
            f.avg_speed_kmh,
            f.idling_duration_minutes,
            f.fuel_consumed_liters,
            f.energy_consumed_kwh,
            f.total_cost_usd,
            f.co2_emissions_kg,
            f.co2_avoided_kg,
            f.avg_ambient_temp_c,
            f.avg_hvac_power_watts,
            COALESCE(v.vehicle_class, 'Unknown') AS vehicle_class
        FROM fact_trips_summary f
        LEFT JOIN dim_vehicle v ON f.veh_id = v.veh_id;
    """
    return pd.read_sql(query, con=get_db_engine())


# -------------------------------------- #
# Reusable KPI Component
# -------------------------------------- #
def render_kpi_card(label: str, value: str, subtext: str, tag: str, color: str):
    """ because there's 6 of them """
    st.markdown(
        f"""
        <div class="kpi-card" style="border-top: 4px solid {color};">
            <div class="kpi-card-header">
                <span class="kpi-label">{label}</span>
                <span class="kpi-badge" style="background-color: {color}1f; color: {color}; border: 1px solid {color}4d;">{tag}</span>
            </div>
            <div class="kpi-value">{value}</div>
            <div class="kpi-subtext">{subtext}</div>
        </div>
        """,
        unsafe_allow_html=True
    )


# -------------------------------------- #
# Filter Controls & Slicers
# -------------------------------------- #
def render_sidebar_filters(df: pd.DataFrame) -> pd.DataFrame:
    st.sidebar.markdown("### Fleet Slicers")

    # 1. Season Filter
    available_seasons = sorted(df["season_category"].dropna().unique().tolist())
    selected_seasons = st.sidebar.multiselect(
        "Seasonal Range Selector",
        options=available_seasons,
        default=available_seasons,
        help="Filter across multi-season baselines (Fall, Winter, Spring, Summer)."
    )


    # 2. Powertrain Slicer
    available_powertrains = sorted(df["engine_type"].dropna().unique().tolist())
    selected_powertrains = st.sidebar.multiselect(
        "Powertrain Slicer",
        options=available_powertrains,
        default=available_powertrains,
        #format_func=lambda x: POWERTRAIN_LABELS.get(x, x),
        help="ICE: Gasoline engine | HEV: Self-charging hybrid | PHEV: Plug-in hybrid with battery reserve | EV: Pure battery electric."
    )

    # 3. Vehicle Class Filter
    available_classes = sorted(df["vehicle_class"].dropna().unique().tolist())
    selected_classes = st.sidebar.multiselect(
        "Vehicle Class Filter",
        options=available_classes,
        default=available_classes,
        help="Filter by duty class (Car, SUV, Van)."
    )

    filtered_df = df[
        (df["season_category"].isin(selected_seasons)) &
        (df["engine_type"].isin(selected_powertrains)) &
        (df["vehicle_class"].isin(selected_classes))
    ]

    st.sidebar.divider()
    st.sidebar.caption(
        f"**Active Slice**: `{len(filtered_df):,}` / `{len(df):,}` trips "
        f"({filtered_df['veh_id'].nunique()} active vehicles)"
    )

    return filtered_df


# -------------------------------------- #
# Header & Executive KPI Cards
# -------------------------------------- #
def render_page_header():
    head_left, head_right = st.columns([0.80, 0.20])
    with head_left:
        st.title("Fleet Electrification & Operational Efficiency")
        st.caption(
            "Enterprise analytics verifying Total Cost of Ownership (TCO), "
            "carbon reduction baselines, and powertrain thermodynamic performance."
        )
    with head_right:
        st.markdown(
            '<div style="text-align: right; padding-top: 14px;"><span class="header-tag">EXECUTIVE TCO</span></div>',
            unsafe_allow_html=True
        )
    st.divider()


def render_kpi_summary_cards(filtered_df: pd.DataFrame):
    st.markdown("#### Executive KPI Summary Cards")

    # Aggregate 6 metrics
    tot_distance = filtered_df["distance_km"].sum()
    tot_fuel = filtered_df["fuel_consumed_liters"].sum()
    tot_elec = filtered_df["energy_consumed_kwh"].sum()
    tot_cost = filtered_df["total_cost_usd"].sum()
    tot_co2_kg = filtered_df["co2_emissions_kg"].sum()
    tot_avoided_kg = filtered_df["co2_avoided_kg"].sum()

    # Unit formatting: convert kg to Metric Tons (MT) if > 1,000 kg for readability
    co2_val_str = f"{tot_co2_kg / 1000:,.2f} MT" if tot_co2_kg >= 1000 else f"{tot_co2_kg:,.1f} kg"
    avoided_val_str = f"{tot_avoided_kg / 1000:,.2f} MT" if tot_avoided_kg >= 1000 else f"{tot_avoided_kg:,.1f} kg"

    kpis = [
        ("Total Distance Traveled", f"{tot_distance:,.0f} km", "Cumulative odometer distance", "RIEMANN", "#2563EB"),
        ("Total Fuel Consumed", f"{tot_fuel:,.1f} L", "Net gasoline consumed", "PID/MAF", "#0891B2"),
        ("Total Grid Energy", f"{tot_elec:,.1f} kWh", "Net electrical traction draw", "BATTERY", "#10B981"),
        ("Total Fleet Energy Cost", f"${tot_cost:,.2f}", "Dynamic point-in-time expenditure", "TARIFF", "#D97706"),
        ("Total Carbon Emissions", co2_val_str, "Scope 1 & 2 emissions", "SCOPE 1+2", "#DC2626"),
        ("CO₂ Emissions Avoided", avoided_val_str, "Net emissions offset vs ICE", "DISPLACED", "#7C3AED"),
    ]

    cols = st.columns(6)
    for col, (label, val, subtext, tag, color) in zip(cols, kpis):
        with col:
            render_kpi_card(label, val, subtext, tag, color)

    st.write("")


# -------------------------------------- #
# Main Application Logic
# -------------------------------------- #
if __name__ == "__main__":
    apply_page_styles()
    render_page_header()

    try:
        raw_df = load_fact_trips_data()
    except Exception as e:
        st.error(f"Failed to query PostgreSQL data warehouse: {e}")
        st.stop()

    if raw_df.empty:
        st.warning("No records found in fact_trips_summary.")
        st.stop()

    filtered_df = render_sidebar_filters(raw_df)
    if filtered_df.empty:
        st.warning("No trip records match the selected filters. Please adjust the sidebar slicers.")
        st.stop()

    render_kpi_summary_cards(filtered_df)
