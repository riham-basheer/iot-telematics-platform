import sys
from pathlib import Path
import numpy as np
import pandas as pd
import pydeck as pdk
import streamlit as st

# Resolve repository root
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.db_connection import get_db_engine

st.set_page_config(
    page_title="Spatial Infrastructure & Depots",
    layout="wide",
    initial_sidebar_state="expanded",
)


# -----------------------------------------------------------------------------
# 1. Theme-Adaptive CSS Design System
# -----------------------------------------------------------------------------
def apply_page_styles():
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

        /* 4 Spatial KPI Metric Cards */
        .kpi-card {
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
        .kpi-card:hover {
            transform: translateY(-2px);
            box-shadow: 0 8px 20px rgba(0, 0, 0, 0.12);
            border-color: rgba(128, 128, 128, 0.45);
        }
        .kpi-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 4px;
        }
        .kpi-label {
            font-family: 'Plus Jakarta Sans', sans-serif;
            font-size: 0.76rem;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            color: var(--text-color);
            opacity: 0.75;
        }
        .kpi-badge {
            font-family: 'Plus Jakarta Sans', sans-serif;
            font-size: 0.68rem;
            font-weight: 700;
            padding: 2px 7px;
            border-radius: 5px;
            letter-spacing: 0.05em;
            text-transform: uppercase;
        }
        .kpi-value {
            font-family: 'JetBrains Mono', monospace;
            font-size: 1.85rem;
            font-weight: 800;
            line-height: 1.2;
            letter-spacing: -0.03em;
            margin: 6px 0 2px 0;
            color: var(--text-color);
        }
        .kpi-subtext {
            font-size: 0.80rem;
            color: var(--text-color);
            opacity: 0.75;
            line-height: 1.4;
        }

        .header-tag {
            display: inline-block;
            background: rgba(16, 185, 129, 0.15);
            color: #059669;
            padding: 5px 12px;
            font-weight: 700;
            font-size: 0.74rem;
            border-radius: 6px;
            border: 1px solid rgba(16, 185, 129, 0.35);
            letter-spacing: 0.05em;
        }

        .legend-box {
            display: flex;
            gap: 18px;
            align-items: center;
            margin-bottom: 12px;
            font-size: 0.85rem;
        }
        .legend-item {
            display: inline-flex;
            align-items: center;
            gap: 6px;
            font-weight: 600;
            color: var(--text-color);
        }
        .legend-dot {
            width: 10px;
            height: 10px;
            border-radius: 50%;
            display: inline-block;
        }
        /* 1. Reduce top canvas padding to pull cards up */
        .block-container {
            padding-top: 2rem !important;
        }

        /* 2. Style the top header bar and inject the title like Page 2 */
        header[data-testid="stHeader"] {
            background-color: var(--background-color);
        }
        header[data-testid="stHeader"]::before {
            content: "Spatial Infrastructure & Depots";
            position: absolute;
            left: 4.5rem;
            top: 50%;
            transform: translateY(-50%);
            font-family: 'Plus Jakarta Sans', sans-serif;
            font-size: 1.15rem;
            font-weight: 700;
            color: var(--text-color);
            letter-spacing: -0.02em;
            pointer-events: none;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


# -----------------------------------------------------------------------------
# 2. Database Retrieval Layer (with Coordinate Deduplication for Chargers)
# -----------------------------------------------------------------------------
@st.cache_data(ttl=60)
def load_spatial_datasets():
    """Loads spatial hotspot clusters and deduplicated NREL charging stations."""
    engine = get_db_engine()
    hotspots = pd.read_sql("SELECT * FROM fact_spatial_hotspots;", con=engine)
    raw_stations = pd.read_sql("SELECT * FROM dim_charging_stations;", con=engine)

    # Group stations sharing identical coordinates so pins don't stack on top of each other
    if not raw_stations.empty:
        stations = (
            raw_stations.groupby(["latitude", "longitude"], as_index=False)
            .agg(
                station_id=("station_id", "first"),
                station_name=("station_name", lambda s: " / ".join(dict.fromkeys(s))),
                l2_ports_count=("l2_ports_count", "sum"),
                dc_fast_ports_count=("dc_fast_ports_count", "sum"),
                access_type=("access_type", "first"),
                unit_count=("station_id", "count"),
            )
        )
    else:
        stations = raw_stations

    return hotspots, stations, len(raw_stations)


# -----------------------------------------------------------------------------
# 3. Geodesic Distance Helper (Haversine Formula)
# -----------------------------------------------------------------------------
def calculate_nearest_chargers(
    hotspots_df: pd.DataFrame, stations_df: pd.DataFrame
) -> pd.DataFrame:
    """Computes the nearest public charger and distance for each unique hotspot."""
    if hotspots_df.empty or stations_df.empty:
        return pd.DataFrame()

    h_lat = np.radians(hotspots_df["cluster_latitude"].to_numpy())
    h_lon = np.radians(hotspots_df["cluster_longitude"].to_numpy())
    s_lat = np.radians(stations_df["latitude"].to_numpy())
    s_lon = np.radians(stations_df["longitude"].to_numpy())

    nearest_names = []
    nearest_dists = []

    R = 6371.0  # Earth radius in km

    for lat1, lon1 in zip(h_lat, h_lon):
        dlat = s_lat - lat1
        dlon = s_lon - lon1
        a = (
            np.sin(dlat / 2.0) ** 2
            + np.cos(lat1) * np.cos(s_lat) * np.sin(dlon / 2.0) ** 2
        )
        c = 2.0 * np.arcsin(np.sqrt(a))
        distances_km = R * c

        min_idx = np.argmin(distances_km)
        nearest_names.append(stations_df.iloc[min_idx]["station_name"])
        nearest_dists.append(round(distances_km[min_idx], 2))

    res_df = hotspots_df.copy()
    res_df["nearest_station"] = nearest_names
    res_df["distance_to_charger_km"] = nearest_dists

    def assign_action(row):
        dist = row["distance_to_charger_km"]
        hours = row["total_idling_hours"]
        if dist >= 1.5 and hours >= 2.0:
            return "🚨 High Priority: Deploy 50 kW DCFC Hub (Charging Desert)"
        elif dist >= 0.8:
            return "⚠️ Medium Priority: Siting Level 2 Curbside Charging"
        else:
            return "🟢 Optimal Coverage: Fleet Anti-Idling & Route Optimization"

    res_df["recommended_action"] = res_df.apply(assign_action, axis=1)
    return res_df


# -----------------------------------------------------------------------------
# 4. Sidebar Slicers (Aggregates Unique Intersections Across Selected Seasons)
# -----------------------------------------------------------------------------
def render_sidebar(hotspots_df: pd.DataFrame) -> pd.DataFrame:
    st.sidebar.markdown("### Spatial Slicers")

    available_seasons = sorted(
        hotspots_df["season_category"].dropna().unique().tolist()
    )
    selected_seasons = st.sidebar.multiselect(
        "Seasonal Category",
        options=available_seasons,
        default=available_seasons,
        help="Filter idling clusters across operational seasons.",
    )

    # 1. Filter by selected seasons first
    season_filtered = hotspots_df[
        hotspots_df["season_category"].isin(selected_seasons)
    ]

    if season_filtered.empty:
        return pd.DataFrame()

    # 2. Collapse across selected seasons so each physical (lat, lon) intersection is unique
    grouped_intersections = (
        season_filtered.groupby(
            ["cluster_latitude", "cluster_longitude"], as_index=False
        )
        .agg(
            seasons_active=("season_category", "nunique"),
            total_idling_events=("total_idling_events", "sum"),
            total_idling_hours=("total_idling_hours", "sum"),
            wasted_fuel_liters=("wasted_fuel_liters", "sum"),
            wasted_cost_usd=("wasted_cost_usd", "sum"),
        )
    )

    min_hours = float(grouped_intersections["total_idling_hours"].min())

    max_hours = float(grouped_intersections["total_idling_hours"].max())

    threshold = st.sidebar.slider(
        "Minimum Idling Delay (Hours)",
        min_value=0.0,
        max_value=round(max_hours, 1),
        value=0.0,
        step=0.1,
        help="Filter out minor intersection stops to highlight chronic congestion hubs.",
    )


    final_df = grouped_intersections[
        grouped_intersections["total_idling_hours"] >= threshold
    ].copy()

    total_unique_intersections = hotspots_df[
        ["cluster_latitude", "cluster_longitude"]
    ].drop_duplicates().shape[0]

    st.sidebar.divider()
    st.sidebar.markdown(
        f"**Active Intersections:** :green-background[**{len(final_df):,}**] / {total_unique_intersections:,} unique zones"
    )
    return final_df


# -----------------------------------------------------------------------------
# 5. KPI Metric Cards
# -----------------------------------------------------------------------------

    

def render_kpi_cards(
    filtered_hotspots: pd.DataFrame,
    stations_df: pd.DataFrame,
    raw_station_count: int,
):
    st.markdown("#### Spatial KPI Summary Cards")
    tot_clusters = len(filtered_hotspots)
    tot_idle_hours = filtered_hotspots["total_idling_hours"].sum()
    tot_wasted_fuel = filtered_hotspots["wasted_fuel_liters"].sum()
    tot_wasted_cost = filtered_hotspots["wasted_cost_usd"].sum()

    tot_l2_ports = int(stations_df["l2_ports_count"].sum())
    tot_dcfc_ports = int(stations_df["dc_fast_ports_count"].sum())

    c1, c2, c3, c4 = st.columns(4, gap="medium")

    with c1:
        st.markdown(
            f"""
            <div class="kpi-card" style="border-top: 3.5px solid #059669;">
                <div class="kpi-header">
                    <span class="kpi-label">Congestion Hubs</span>
                    <span class="kpi-badge" style="background: rgba(5, 150, 105, 0.15); color: #059669; border: 1px solid rgba(5, 150, 105, 0.3);">GRID ~110M</span>
                </div>
                <div class="kpi-value">{tot_clusters:,}</div>
                <div class="kpi-subtext">Unique intersection bottleneck zones</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with c2:
        st.markdown(
            f"""
            <div class="kpi-card" style="border-top: 3.5px solid #2563EB;">
                <div class="kpi-header">
                    <span class="kpi-label">Fleet Delay</span>
                    <span class="kpi-badge" style="background: rgba(37, 99, 235, 0.15); color: #2563EB; border: 1px solid rgba(37, 99, 235, 0.3);">HOURS</span>
                </div>
                <div class="kpi-value">{tot_idle_hours:,.1f}</div>
                <div class="kpi-subtext">Cumulative stationary engine hours</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with c3:
        st.markdown(
            f"""
            <div class="kpi-card" style="border-top: 3.5px solid #F59E0B;">
                <div class="kpi-header">
                    <span class="kpi-label">Wasted Fuel</span>
                    <span class="kpi-badge" style="background: rgba(245, 158, 11, 0.15); color: #F59E0B; border: 1px solid rgba(245, 158, 11, 0.3);">CAPEX LOSS</span>
                </div>
                <div class="kpi-value">${tot_wasted_cost:,.2f}</div>
                <div class="kpi-subtext">Loss from {tot_wasted_fuel:,.1f} L burned at 0 km/h</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with c4:
        st.markdown(
            f"""
            <div class="kpi-card" style="border-top: 3.5px solid #7C3AED;">
                <div class="kpi-header">
                    <span class="kpi-label">Public Charging</span>
                    <span class="kpi-badge" style="background: rgba(124, 58, 237, 0.15); color: #7C3AED; border: 1px solid rgba(124, 58, 237, 0.3);">NREL AFDC</span>
                </div>
                <div class="kpi-value">{raw_station_count:,}</div>
                <div class="kpi-subtext"><strong>{tot_l2_ports}</strong> L2 Ports • <strong>{tot_dcfc_ports}</strong> DC Fast Ports</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.write("")


# -----------------------------------------------------------------------------
# 6. Interactive Pydeck Map (Pixel-Locked Crisp Station Markers)
# -----------------------------------------------------------------------------
def render_pydeck_map(
    filtered_hotspots: pd.DataFrame, stations_df: pd.DataFrame
):
    st.markdown("##### Geospatial Congestion & Charging Infrastructure Map")

    st.markdown(
        """
        <div class="legend-box">
            <span class="legend-item"><span class="legend-dot" style="background: #EF4444;"></span> Fleet Idling Heatmap</span>
            <span class="legend-item"><span class="legend-dot" style="background: #10B981; border: 1.5px solid #FFFFFF;"></span> DC Fast Charger</span>
            <span class="legend-item"><span class="legend-dot" style="background: #3B82F6; border: 1.5px solid #FFFFFF;"></span> Level 2 AC Charger</span>
        </div>
        """,
        unsafe_allow_html=True,
    )

    stations_map = stations_df.copy()
    stations_map["charger_type"] = stations_map["dc_fast_ports_count"].apply(
        lambda x: "DC Fast Charger" if x > 0 else "Level 2 AC Charger"
    )
    stations_map["fill_color"] = stations_map["dc_fast_ports_count"].apply(
        lambda x: [16, 185, 129, 235] if x > 0 else [59, 130, 246, 220]
    )

    # Layer 1: Congestion Heatmap (weighted by total_idling_hours)
    heatmap_layer = pdk.Layer(
        "HeatmapLayer",
        data=filtered_hotspots,
        get_position=["cluster_longitude", "cluster_latitude"],
        get_weight="total_idling_hours",
        radiusPixels=32,
        intensity=1.4,
        threshold=0.06,
        pickable=False,
    )

    # Layer 2: Crisp, pixel-bounded Charging Pins (won't balloon when zooming in)
    stations_layer = pdk.Layer(
        "ScatterplotLayer",
        data=stations_map,
        get_position=["longitude", "latitude"],
        get_fill_color="fill_color",
        get_line_color=[255, 255, 255, 220],
        stroked=True,
        line_width_min_pixels=1.5,
        get_radius=18,
        radius_min_pixels=5,
        radius_max_pixels=10,
        pickable=True,
        auto_highlight=True,
    )

    view_state = pdk.ViewState(
        latitude=42.278,
        longitude=-83.743,
        zoom=11.8,
        pitch=20,
    )

    tooltip = {
        "html": """
            <b>{station_name}</b><br/>
            <b>Type:</b> {charger_type}<br/>
            <b>L2 Ports:</b> {l2_ports_count} | <b>DC Fast Ports:</b> {dc_fast_ports_count}<br/>
            <b>Access:</b> {access_type}
        """,
        "style": {
            "backgroundColor": "#0F172A",
            "color": "#FFFFFF",
            "fontSize": "12px",
            "borderRadius": "6px",
            "padding": "8px 10px",
            "border": "1px solid rgba(255,255,255,0.2)",
        },
    }

    deck = pdk.Deck(
        layers=[heatmap_layer, stations_layer],
        initial_view_state=view_state,
        tooltip=tooltip,
        map_style="dark",
    )

    st.pydeck_chart(deck, use_container_width=True)


# -----------------------------------------------------------------------------
# 7. CapEx Infrastructure Gap & Planning Matrix (Unique Intersections, No Index)
# -----------------------------------------------------------------------------
def render_planning_table(planner_df: pd.DataFrame):
    st.markdown("##### Infrastructure Gap & CapEx Planning Matrix")
    st.caption(
        "Ranks unique recurring congestion intersections across the selected seasons, "
        "calculates geodesic distance to the nearest public charger, and prescribes CapEx interventions."
    )

    if planner_df.empty:
        st.info("No hotspots match the active threshold filters.")
        return

    display_df = (
        planner_df[
            [
                "cluster_latitude",
                "cluster_longitude",
                "seasons_active",
                "total_idling_hours",
                "wasted_fuel_liters",
                "wasted_cost_usd",
                "nearest_station",
                "distance_to_charger_km",
                "recommended_action",
            ]
        ]
        .sort_values("total_idling_hours", ascending=False)
        .head(25)
        .copy()
    )

    display_df.columns = [
        "Latitude",
        "Longitude",
        "Seasons Active",
        "Total Idle (Hrs)",
        "Fuel Lost (L)",
        "Wasted Budget ($)",
        "Nearest Public Station",
        "Charger Dist (km)",
        "Prescribed CapEx Action",
    ]

    st.dataframe(
        display_df.style.format(
            {
                "Latitude": "{:.3f}",
                "Longitude": "{:.3f}",
                "Seasons Active": "{:d} / 5",
                "Total Idle (Hrs)": "{:,.2f} hrs",
                "Fuel Lost (L)": "{:,.2f} L",
                "Wasted Budget ($)": "${:,.2f}",
                "Charger Dist (km)": "{:.2f} km",
            }
        ),
        use_container_width=True,
        hide_index=True,
        height=400,
    )


# -----------------------------------------------------------------------------
# Main Application Entry Point
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    apply_page_styles()

    try:
        hotspots_df, stations_df, raw_station_count = load_spatial_datasets()
    except Exception as err:
        st.error(f"Failed to load spatial datasets from PostgreSQL: {err}")
        st.stop()

    if hotspots_df.empty:
        st.warning(
            "fact_spatial_hotspots is empty. Please run src/batch/spark_spatial_hotspots.py first."
        )
        st.stop()


    filtered_hotspots = render_sidebar(hotspots_df)

    if filtered_hotspots.empty:
        st.warning("No intersections match the selected filters.")
        st.stop()

    render_kpi_cards(filtered_hotspots, stations_df, raw_station_count)
    render_pydeck_map(filtered_hotspots, stations_df)

    st.write("")

    planner_df = calculate_nearest_chargers(filtered_hotspots, stations_df)
    render_planning_table(planner_df)