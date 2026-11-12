import sys
from pathlib import Path
import pandas as pd
import streamlit as st
import numpy as np

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

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

POWERTRAIN_COLORS = {
    "ICE": "#0EA5E9",  # Bright blue
    "HEV": "#3B82F6",  # Standard blue
    "PHEV": "#6366F1", # Indigo
    "EV": "#10B981"    # Emerald green
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

        /* 6 KPI Metric Cards - Left aligned with larger numbers */
        .kpi-card {
            background-color: var(--secondary-background-color);
            border: 1px solid rgba(128, 128, 128, 0.2);
            border-radius: 10px;
            padding: 18px 22px;
            height: 100%;
            display: flex;
            flex-direction: column;
            justify-content: space-between;
            text-align: left;
            align-items: flex-start;
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
            width: 100%;
            margin-bottom: 6px;
        }
        .kpi-label {
            font-family: 'Plus Jakarta Sans', sans-serif;
            font-size: 0.74rem;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.06em;
            opacity: 0.8;
            color: var(--text-color);
        }
        .kpi-value {
            font-family: 'JetBrains Mono', monospace;
            font-size: 2.15rem;
            font-weight: 700;
            line-height: 1.2;
            letter-spacing: -0.03em;
            color: var(--text-color);
            margin: 4px 0 6px 0;
        }
        .kpi-subtext {
            font-size: 0.80rem;
            opacity: 0.75;
            line-height: 1.35;
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
            white-space: nowrap;
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
        /* 1. Reduce top canvas padding to pull cards up */
        .block-container {
            padding-top: 2rem !important;
        }

        /* 2. Style the top header bar and inject the title */
        header[data-testid="stHeader"] {
            background-color: var(--background-color);
        }
        header[data-testid="stHeader"]::before {
            content: "Fleet Electrification & Operational Efficiency";
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
            COALESCE(v.vehicle_class, 'Unspecified') AS vehicle_class
        FROM fact_trips_summary f
        LEFT JOIN dim_vehicle v ON f.veh_id = v.veh_id;
    """
    return pd.read_sql(query, con=get_db_engine())


# -------------------------------------- #
# Reusable KPI Component
# -------------------------------------- #
def render_kpi_card(label: str, value: str, subtext: str, color: str):
    """ because there's 6 of them """
    st.markdown(
      f"""
        <div class="kpi-card" style="border-top: 4px solid {color};">
            <div class="kpi-label">{label}</div>
            <div class="kpi-value">{value}</div>
            <div class="kpi-subtext">{subtext}</div>
        </div>
        """,
      unsafe_allow_html=True,
    )


# -------------------------------------- #
# Filter Controls & Slicers
# -------------------------------------- #

def render_sidebar_filters(df: pd.DataFrame) -> pd.DataFrame:
    st.sidebar.markdown("### Fleet Slicers")

    # 1. Calendar Date Range Picker
    
    trip_dates = pd.to_datetime(df["trip_start_time"], format="ISO8601", utc=True).dt.date
    min_date = trip_dates.min()
    max_date = trip_dates.max()

    if pd.notnull(min_date) and pd.notnull(max_date):
        date_selection = st.sidebar.date_input(
            "Calendar Date Range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
            help="Filter telemetry trips between two dates within the warehouse horizon."
        )
        if isinstance(date_selection, (list, tuple)) and len(date_selection) == 2:
            start_date, end_date = date_selection
        elif isinstance(date_selection, (list, tuple)) and len(date_selection) == 1:
            start_date = end_date = date_selection[0]
        else:
            start_date, end_date = min_date, max_date
    else:
        start_date, end_date = None, None

    # 2. Season Slicer
    available_seasons = sorted(df["season_category"].dropna().unique().tolist())
    selected_seasons = st.sidebar.multiselect(
        "Seasonal Category",
        options=available_seasons,
        default=available_seasons,
        help="Filter across multi-season baselines (Fall, Winter, Spring, Summer)."
    )

    # 3. Powertrain Slicer 
    available_powertrains = sorted(df["engine_type"].dropna().unique().tolist())
    selected_powertrains = st.sidebar.multiselect(
        "Powertrain Slicer",
        options=available_powertrains,
        default=available_powertrains,
        #format_func=lambda x: POWERTRAIN_LABELS.get(x, x),
        help="ICE: Gasoline engine | HEV: Self-charging hybrid | PHEV: Plug-in hybrid with battery reserve | EV: Pure battery electric."
    )

    # 4. Vehicle Class Filter
    available_classes = sorted(df["vehicle_class"].dropna().unique().tolist())
    selected_classes = st.sidebar.multiselect(
        "Vehicle Class Filter",
        options=available_classes,
        default=available_classes,
        help="Filter by duty class (Car, SUV, Van, Unspecified)."
    )

    # Apply all filter conditions
    mask = (
        (df["season_category"].isin(selected_seasons)) &
        (df["engine_type"].isin(selected_powertrains)) &
        (df["vehicle_class"].isin(selected_classes))
    )
    if start_date and end_date:
        mask &= (trip_dates >= start_date) & (trip_dates <= end_date)

    filtered_df = df[mask]



    st.sidebar.divider()
    st.sidebar.markdown(f"**Active Slice:** :blue-background[**{len(filtered_df):,}**] / **{len(df):,}** trips")
    st.sidebar.markdown(f"**Active Assets:** :green-background[**{filtered_df['veh_id'].nunique()}**] vehicles")

    return filtered_df


# -------------------------------------- #
# Executive KPI Cards
# -------------------------------------- #

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

    # Row 1: Physical Telemetry Operations
    r1_c1, r1_c2, r1_c3 = st.columns(3, gap="medium")
    with r1_c1:
        render_kpi_card("Total Distance Traveled",f"{tot_distance:,.0f} km","Cumulative odometer distance", "#2563EB",)
    with r1_c2:
        render_kpi_card("Total Fuel Consumed", f"{tot_fuel:,.1f} L", "Net gasoline consumed","#0891B2",)
    with r1_c3:
        render_kpi_card( "Total Grid Energy",f"{tot_elec:,.1f} kWh", "Net electrical traction draw", "#10B981",)

    st.write("") 

    # Row 2: Financial & Carbon Impact
    r2_c1, r2_c2, r2_c3 = st.columns(3, gap="medium")
    with r2_c1:
        render_kpi_card("Total Fleet Energy Cost", f"${tot_cost:,.2f}", "Dynamic point-in-time expenditure", "#D97706",)
    with r2_c2:
        render_kpi_card("Total Carbon Emissions", co2_val_str, "Scope 1 & 2 emissions", "#DC2626",)
    with r2_c3:
        render_kpi_card("CO₂ Emissions Avoided", avoided_val_str, "Net emissions offset vs ICE", "#7C3AED",)

    st.write("")


# -------------------------------------- #
# Tab 1: Cost & Energy Intensity
# -------------------------------------- #
def render_tco_benchmark_tab(filtered_df: pd.DataFrame):
    tco_summary = filtered_df.groupby("engine_type").agg(
        Trips=("trip_id", "count"),
        Distance_km=("distance_km", "sum"),
        Total_Cost_USD=("total_cost_usd", "sum"),
        Fuel_Liters=("fuel_consumed_liters", "sum"),
        Grid_kWh=("energy_consumed_kwh", "sum"),
        CO2_kg=("co2_emissions_kg", "sum"),
        CO2_Avoided_kg=("co2_avoided_kg", "sum")
    ).reset_index()

    # Calculate normalized intensities per 100 km
    tco_summary["Cost_Per_100km"] = (
        tco_summary["Total_Cost_USD"] / tco_summary["Distance_km"].replace(0, np.nan)
    ) * 100
    tco_summary["Fuel_Per_100km"] = (
        tco_summary["Fuel_Liters"] / tco_summary["Distance_km"].replace(0, np.nan)
    ) * 100
    tco_summary["kWh_Per_100km"] = (
        tco_summary["Grid_kWh"] / tco_summary["Distance_km"].replace(0, np.nan)
    ) * 100

    tco_summary = tco_summary.fillna(0.0)

    # Order rows logically
    powertrain_order = ["ICE", "HEV", "PHEV", "EV"]
    tco_summary["sort_order"] = tco_summary["engine_type"].map(lambda x: powertrain_order.index(x) if x in powertrain_order else 99)
    tco_summary = tco_summary.sort_values("sort_order").drop(columns=["sort_order"])

    col_chart1, col_chart2 = st.columns(2, gap="large")

    with col_chart1:
        st.markdown("##### Fleet Operating Cost (dollars per 100 km)")
        fig_cost = px.bar(
            tco_summary,
            x="engine_type",
            y="Cost_Per_100km",
            color="engine_type",
            color_discrete_map=POWERTRAIN_COLORS,
            labels={"engine_type": "Powertrain", "Cost_Per_100km": "Operating Cost ($/100 km)"}
        )

        # Add downward line overlay across the tops of the bars
        fig_cost.add_trace(
            go.Scatter(
                x=tco_summary["engine_type"],
                y=tco_summary["Cost_Per_100km"],
                mode="lines+markers",
                name="Cost Trend",
                line=dict(color="#F59E0B", width=3, shape="spline"),
                marker=dict(size=8, color="#FBBF24")
            )
        )

        fig_cost.update_layout(
            showlegend=False,
            height=340,
            margin=dict(l=20, r=20, t=30, b=30),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(title="", showgrid=False),
            yaxis=dict(title="USD ($) per 100 km", showgrid=True, gridcolor="rgba(83, 208, 255, 0.34)"),
            font=dict(family="Inter, sans-serif")
        )
        
        # Ensure value labels only appear on the bars (not duplicated on the line markers)
        fig_cost.update_traces(
            selector=dict(type="bar"),
            texttemplate="$%{y:.2f}",
            textposition="outside"
        )
        st.plotly_chart(fig_cost, use_container_width=True)

    with col_chart2:
        st.markdown("##### Fuel & Energy Intensity per 100 km")
        fig_intensity = go.Figure()
        
        # Fuel Intensity bar (L / 100 km)
        fig_intensity.add_trace(go.Bar(
            name="Fuel Intensity (L/100 km)",
            x=tco_summary["engine_type"],
            y=tco_summary["Fuel_Per_100km"],
            marker_color="#D97706",
            text=[f"{v:.1f} L" if v > 0 else "" for v in tco_summary["Fuel_Per_100km"]],
            textposition="outside"
        ))

        # Electric Intensity bar (kWh / 100 km)
        fig_intensity.add_trace(go.Bar(
            name="Grid Intensity (kWh/100 km)",
            x=tco_summary["engine_type"],
            y=tco_summary["kWh_Per_100km"],
            marker_color="#10B981",
            text=[f"{v:.1f} kWh" if v > 0 else "" for v in tco_summary["kWh_Per_100km"]],
            textposition="outside"
        ))

        fig_intensity.update_layout(
            barmode="group",
            height=340,
            margin=dict(l=20, r=20, t=30, b=30),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(title="", showgrid=False),
            yaxis=dict(title="Consumption per 100 km", showgrid=True, gridcolor="rgba(83, 208, 255, 0.34)"),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            font=dict(family="Inter, sans-serif")
        )
        st.plotly_chart(fig_intensity, use_container_width=True)

    # ROI Takeaway & Tabular Rollup
    st.info(
        "💡 **Business Takeaway**: Explores the return on investment (ROI) of fleet electrification by "
        "demonstrating that pure electric (EV) and plug-in hybrid (PHEV) assets operate at a substantial "
        "discount per kilometer compared to conventional internal combustion engines (ICE)."
    )

    st.markdown("##### Powertrain Performance Rollup")
    display_table = tco_summary.rename(columns={
        "engine_type": "Powertrain",
        "Distance_km": "Total Distance (km)",
        "Total_Cost_USD": "Total Cost ($)",
        "Cost_Per_100km": "Cost / 100km ($)",
        "Fuel_Per_100km": "Fuel / 100km (L)",
        "kWh_Per_100km": "Grid / 100km (kWh)",
        "CO2_kg": "Gross CO₂ (kg)",
        "CO2_Avoided_kg": "Net CO₂ Avoided (kg)"
    })
    st.dataframe(
        display_table.style.format({
            "Total Distance (km)": "{:,.1f}",
            "Total Cost ($)": "${:,.2f}",
            "Cost / 100km ($)": "${:,.2f}",
            "Fuel / 100km (L)": "{:,.2f}",
            "Grid / 100km (kWh)": "{:,.2f}",
            "Gross CO₂ (kg)": "{:,.1f}",
            "Net CO₂ Avoided (kg)": "{:,.1f}"
        }),
        use_container_width=True
    )


# -------------------------------------------------------- #
# Tab 2: Operational Waste Breakdown (Idling vs Driving)
# # ------------------------------------------------------ #

def render_operational_waste_tab(filtered_df: pd.DataFrame):
    tot_duration_mins = filtered_df["duration_minutes"].sum()
    tot_idling_mins = filtered_df["idling_duration_minutes"].sum()
    tot_fuel_liters = filtered_df["fuel_consumed_liters"].sum()

    #tot_cost_usd = filtered_df["total_cost_usd"].sum()

    # Approximate idling waste ratio based on idling time vs trip duration
    trip_idle_ratio = (
        filtered_df["idling_duration_minutes"] / filtered_df["duration_minutes"].replace(0, np.nan)
    ).clip(0.0, 0.85).fillna(0.0)

    # Calculate Wasted Idling Fuel and Productive Fuel
    wasted_fuel_liters = (filtered_df["fuel_consumed_liters"] * trip_idle_ratio).sum()
    productive_fuel_liters = max(0.0, tot_fuel_liters - wasted_fuel_liters)

    # Calculate Capital Lost to Idling
    capital_lost_usd = (filtered_df["total_cost_usd"] * trip_idle_ratio).sum()

    idle_time_pct = (tot_idling_mins / max(1.0, tot_duration_mins)) * 100
    idle_fuel_pct = (wasted_fuel_liters / max(0.001, tot_fuel_liters)) * 100 if tot_fuel_liters > 0 else 0.0

    col_donut, col_callouts = st.columns([0.55, 0.45], gap="large")

    with col_donut:
        st.markdown("##### Fuel Allocation: Productive Driving vs. Stationary Idling")
        
        donut_data = pd.DataFrame({
            "Category": ["Productive Driving Fuel", "Wasted Idling Fuel"],
            "Liters": [productive_fuel_liters, wasted_fuel_liters]
        })

        fig_donut = px.pie(
            donut_data,
            names="Category",
            values="Liters",
            hole=0.62,
            color="Category",
            color_discrete_map={
                "Productive Driving Fuel": "#0187C5",  # Deep Navy Theme Bright Blue
                "Wasted Idling Fuel": "#F59E0B"        # Amber/Orange Warning
            }
        )
        fig_donut.update_layout(
            height=340,
            margin=dict(l=10, r=10, t=10, b=10),
            paper_bgcolor="rgba(0,0,0,0)",
            legend=dict(orientation="h", yanchor="bottom", y=-0.15, xanchor="center", x=0.5),
            font=dict(family="Inter, sans-serif", color="#E2E8F0")
        )
        fig_donut.update_traces(
            textinfo="percent+value",
            texttemplate="%{value:,.1f} L<br>(%{percent})",
            hoverinfo="label+value+percent"
        )
        st.plotly_chart(fig_donut, use_container_width=True)

    with col_callouts:
        st.markdown("##### Financial & Delay Impact")
        
        st.markdown(
            f"""
            <div class="kpi-card" style="border-left: 4px solid #EF4444; margin-bottom: 16px;">
                <div style="display: flex; flex-direction: column; align-items: flex-start; gap: 8px;">
                    <span class="kpi-badge" style="background-color: rgba(239, 68, 68, 0.15); color: #EF4444; border: 1px solid rgba(239, 68, 68, 0.3);">CAPITAL IMPACT</span>
                    <div class="kpi-label" style="margin-top: 4px;">Capital Lost to Idling</div>
                </div>
                <div class="kpi-value" style="margin-top: 8px;">${capital_lost_usd:,.2f}</div>
                <div class="kpi-subtext">Direct operational budget lost to zero-speed combustion stops.</div>
            </div>

            <div class="kpi-card" style="border-left: 4px solid #F59E0B;">
                <div style="display: flex; flex-direction: column; align-items: flex-start; gap: 8px;">
                    <span class="kpi-badge" style="background-color: rgba(245, 158, 11, 0.15); color: #F59E0B; border: 1px solid rgba(245, 158, 11, 0.3);">OPERATIONAL DELAY</span>
                    <div class="kpi-label" style="margin-top: 4px;">Total Idling Duration</div>
                </div>
                <div class="kpi-value" style="margin-top: 8px;">{tot_idling_mins / 60:,.1f} Hours</div>
                <div class="kpi-subtext">Represents <strong>{idle_time_pct:.1f}%</strong> of total fleet engine run-time.</div>
            </div>
            """,
            unsafe_allow_html=True
        )

    st.warning(
        "🛑 **Takeaway**: Surfaces unmonitored capital loss from auxiliary stops, staging queues, "
        "and driver habits. Implementing fleet anti-idling policies or electric start-stop technology "
        f"may directly reclaim up to **${capital_lost_usd:,.2f}** and **{wasted_fuel_liters:,.1f} Liters** of wasted fuel."
    )



# -------------------------------------- #
# Tab 3: Climate Sensitivity Analysis
# -------------------------------------- #
def render_climate_sensitivity_tab(filtered_df: pd.DataFrame):
    st.markdown("##### Seasonal Weather & Climate Sensitivity Analysis")
    
    # ffilter out null temperatures
    df_temp = filtered_df.dropna(subset=["avg_ambient_temp_c"]).copy()
    
    if df_temp.empty:
        st.warning("No temperature data available for the selected filters.")
        return

   
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    # 1. Primary Y-Axis: HVAC Power Load (Blue)
    df_hvac = df_temp.dropna(subset=["avg_hvac_power_watts"])
    if not df_hvac.empty:
        # Scatter
        fig.add_trace(
            go.Scatter(
                x=df_hvac["avg_ambient_temp_c"],
                y=df_hvac["avg_hvac_power_watts"],
                mode="markers",
                name="HVAC Load (W)",
                marker=dict(color="#0EA5E9", size=4, opacity=0.4),
                showlegend=False
            ),
            secondary_y=False
        )
        
        # 2nd-Order Polynomial Trendline
        z_hvac = np.polyfit(df_hvac["avg_ambient_temp_c"], df_hvac["avg_hvac_power_watts"], 2)
        p_hvac = np.poly1d(z_hvac)
        x_trend = np.linspace(df_hvac["avg_ambient_temp_c"].min(), df_hvac["avg_ambient_temp_c"].max(), 100)
        
        fig.add_trace(
            go.Scatter(
                x=x_trend, y=p_hvac(x_trend), mode="lines", name="HVAC Trend", line=dict(color="#0EA5E9", width=3)), secondary_y=False
        )

    # 2. Secondary Y-Axis: EV Battery Depletion Rate (Amber)
    # Filter strictly for EVs with distance > 0 to avoid division by zero
    df_ev = df_temp[(df_temp["engine_type"] == "EV") & (df_temp["distance_km"] > 0)].copy()
    if not df_ev.empty:
        df_ev["ev_depletion_rate"] = (df_ev["energy_consumed_kwh"] / df_ev["distance_km"]) * 100
        df_ev = df_ev.dropna(subset=["ev_depletion_rate"])
        
        # Scatter
        fig.add_trace(
            go.Scatter(
                x=df_ev["avg_ambient_temp_c"],
                y=df_ev["ev_depletion_rate"],
                mode="markers",
                name="EV Depletion (kWh/100km)",
                marker=dict(color="#F59E0B", size=4, opacity=0.4),
                showlegend=False
            ),
            secondary_y=True
        )
        
        # 2nd-Order Polynomial Trendline
        z_ev = np.polyfit(df_ev["avg_ambient_temp_c"], df_ev["ev_depletion_rate"], 2)
        p_ev = np.poly1d(z_ev)
        x_trend_ev = np.linspace(df_ev["avg_ambient_temp_c"].min(), df_ev["avg_ambient_temp_c"].max(), 100)
        
        fig.add_trace(
            go.Scatter(
                x=x_trend_ev,
                y=p_ev(x_trend_ev),
                mode="lines",
                name="EV Depletion Trend",
                line=dict(color="#F59E0B", width=3)
            ),
            secondary_y=True
        )

    fig.update_layout(
        height=400,
        margin=dict(l=20, r=20, t=30, b=20),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        font=dict(family="Inter, sans-serif", color="#E2E8F0")
    )
    
    fig.update_xaxes(title_text="Ambient Temperature (°C)", showgrid=True, gridcolor="rgba(143, 197, 218, 0.34)")
    fig.update_yaxes(title_text="Average Cabin HVAC Energy Load (W)", secondary_y=False, showgrid=True, gridcolor="rgba(143, 197, 218, 0.34)")
    
    fig.update_yaxes(title_text="EV Battery Depletion Rate (kWh/100 km)", secondary_y=True, showgrid=False)
    

    st.plotly_chart(fig, use_container_width=True)

# -------------------------------------- #
#                Main
# -------------------------------------- #

if __name__ == "__main__":
    apply_page_styles()

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

    #st.divider()

    # Three analytical serving tabs
    tab_tco, tab_waste, tab_climate = st.tabs([
        "Powertrain Benchmark (Cost & Energy)",
        "Operational Waste (Idling vs Driving)",
        "Seasonal Weather & Climate Impact"
    ])


    with tab_tco:
        render_tco_benchmark_tab(filtered_df)

    with tab_waste:
        render_operational_waste_tab(filtered_df)

    with tab_climate:
        render_climate_sensitivity_tab(filtered_df)