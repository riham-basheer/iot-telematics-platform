import os
import sys
import argparse
import pandas as pd
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (LongType, StringType, DoubleType, TimestampType, StructType, StructField, IntegerType)
from psycopg2.extras import execute_batch
import glob


# Add project root directory to sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.db_connection import get_db_engine, get_db_cursor

# Automotive Constants for Gasoline Stoichiometry
GASOLINE_AIR_FUEL_RATIO = 14.7
GASOLINE_DENSITY_G_L = 745.0
ICE_BASELINE_CO2_PER_KM = 0.19635  # ~8.5 L/100km * 2.31 kg CO2/L


def get_spark_session(app_name="VED_Silver_to_Gold"):
    return SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .getOrCreate()


def get_scd2_energy_pricing_df(spark):
    """
    Reads the SCD Type 2 pricing dimension directly from PostgreSQL.
    """
    query = """
        SELECT 
            energy_type, 
            CAST(price_per_unit AS DOUBLE PRECISION) AS price_per_unit, 
            CAST(co2_kg_per_unit AS DOUBLE PRECISION) AS co2_kg_per_unit, 
            valid_from, 
            valid_to 
        FROM dim_energy_pricing_history;
    """
    with get_db_cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()

    schema = StructType([
        StructField("energy_type", StringType(), False),
        StructField("price_per_unit", DoubleType(), False),
        StructField("co2_kg_per_unit", DoubleType(), False),
        StructField("valid_from", TimestampType(), True),
        StructField("valid_to", TimestampType(), True)
    ])
    return spark.createDataFrame(rows, schema=schema)

def get_dim_vehicle_df(spark):
    """
    Loads authoritative vehicle metadata from PostgreSQL dim_vehicle
    to preserve true powertrain classifications (ICE, HEV, PHEV, EV).
    """
    query = """
        SELECT veh_id, engine_type 
        FROM dim_vehicle;
    """
    with get_db_cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()

    schema = StructType([
        StructField("veh_id", IntegerType(), False),
        StructField("engine_type", StringType(), False)
    ])
    return spark.createDataFrame(rows, schema=schema)


def compute_trip_physics(df):
    """
    Computes kinematics, idling duration, and trapezoidal Riemann sum integrals
    for 1 Hz vehicular telemetry partitioned by vehicle and trip.

    Numerical Integrals & Physics:
    - Time Delta: Discretizes pings per (vehid, trip)
    - Kinematics & Idling: Trapezoidal distance (km) and acceleration (m/s²); flags idling 
      when speed == 0 while engine (RPM/fuel/MAF > 0) or HV battery (|I| > 0.1A) is active
    - Fuel Consumption (L): Integrates direct fuel_rate_l_hr, falling back to stoichiometric 
      MAF: Fuel_Rate = (maf_g_sec * 3600) / (14.7 * 745.0)
    - Electric & HVAC Energy (kWh): Integrates traction power (P = -V * I, inverted for discharge) 
      and auxiliary cabin climate power (hvac_power_watts) scaled by Δt / 3.6e6
    
    Returns:
    - PySpark DataFrame aggregated to trip grain with duration, distance, idling, 
      consumed fuel/energy, ambient conditions, and kinematic bounds

    Pipeline Scope:
    - Distance (km)
    - Fuel consumption (Liters) with MAF fallback
    - High-voltage battery energy (kWh)
    - Cabin climate HVAC energy (kWh)
    """
    
    trip_window = Window.partitionBy("vehid", "trip").orderBy("timestamp_ms")

    # 1. Delta t in seconds between successive pings .. if gap is more than 30 ot negative, drop
    prev_ms = F.lag("timestamp_ms").over(trip_window)
    delta_t_raw = (F.col("timestamp_ms") - prev_ms) / 1000.0
    delta_t = F.when((delta_t_raw > 0.0) & (delta_t_raw <= 30.0), delta_t_raw).otherwise(0.0)

    # 2. Kinematic delta: Trapezoidal Distance (km) this is distance traveled
    prev_speed = F.lag("vehicle_speed_kmh").over(trip_window)  #previous row speed in km/h
    avg_speed = (F.col("vehicle_speed_kmh") + F.coalesce(prev_speed, F.col("vehicle_speed_kmh"))) / 2.0
    step_distance_km = avg_speed * (delta_t / 3600.0)

    # 3. Idling duration: speed == 0 while engine or battery is active
    is_idling = (F.col("vehicle_speed_kmh") == 0.0) & (
        (F.coalesce(F.col("engine_rpm"), F.lit(0.0)) > 0.0) |
        (F.coalesce(F.col("fuel_rate_l_hr"), F.lit(0.0)) > 0.0) |
        (F.coalesce(F.col("maf_g_sec"), F.lit(0.0)) > 0.0) |
        (F.coalesce(F.abs(F.col("hv_battery_current_a")), F.lit(0.0)) > 0.1)
    )
    step_idling_s = F.when(is_idling, delta_t).otherwise(0.0)

    # 4. Instantaneous Acceleration
    delta_v_ms = (F.col("vehicle_speed_kmh") - F.coalesce(prev_speed, F.col("vehicle_speed_kmh"))) * (1000.0 / 3600.0)
    accel = F.when(delta_t > 0.0, delta_v_ms / delta_t).otherwise(0.0)

    # 5. Fuel Rate Resolution: Use direct PID if present, else fallback to stoichiometric MAF
    maf_derived_fuel = (F.col("maf_g_sec") * 3600.0) / (GASOLINE_AIR_FUEL_RATIO * GASOLINE_DENSITY_G_L)
    effective_fuel_rate = F.coalesce(F.col("fuel_rate_l_hr"), maf_derived_fuel)

    prev_fuel_rate = F.lag(effective_fuel_rate).over(trip_window)
    avg_fuel_rate = (effective_fuel_rate + F.coalesce(prev_fuel_rate, effective_fuel_rate)) / 2.0
    step_fuel_l = F.when(
        effective_fuel_rate.isNotNull() & prev_fuel_rate.isNotNull(),
        avg_fuel_rate * (delta_t / 3600.0)
    ).otherwise(None)

    # 6. Electric Traction Energy: P = V * I (Watts)
    # In VED, negative current = discharge (consumed).
    # Invert sign so net energy consumption is positive::
    curr_batt_power = -1.0 * (F.col("hv_battery_voltage_v") * F.col("hv_battery_current_a"))
    prev_batt_power = F.lag(curr_batt_power).over(trip_window)
    avg_batt_power = (curr_batt_power + F.coalesce(prev_batt_power, curr_batt_power)) / 2.0

    step_elec_kwh = F.when(
        curr_batt_power.isNotNull() & prev_batt_power.isNotNull(),
        avg_batt_power * (delta_t / 3.6e6)
    ).otherwise(None)

    # 7. HVAC Auxiliary Energy (kWh)
    prev_hvac_power = F.lag("hvac_power_watts").over(trip_window)
    avg_hvac_power = (F.col("hvac_power_watts") + F.coalesce(prev_hvac_power, F.col("hvac_power_watts"))) / 2.0

    step_hvac_kwh = F.when(
        F.col("hvac_power_watts").isNotNull() & prev_hvac_power.isNotNull(),
        avg_hvac_power * (delta_t / 3.6e6)
    ).otherwise(None) # to leave the ICE vehicles as null

    # Signal presence for Powertrain classification
    has_fuel_reading = F.col("fuel_rate_l_hr").isNotNull() | F.col("maf_g_sec").isNotNull() | F.col("engine_rpm").isNotNull()
    has_battery_reading = F.col("hv_battery_voltage_v").isNotNull() | F.col("hv_battery_soc_pct").isNotNull() 

    # Append step calculations
    df_steps = df.withColumn("delta_t_s", delta_t) \
                 .withColumn("step_dist_km", step_distance_km) \
                 .withColumn("step_idling_s", step_idling_s) \
                 .withColumn("acceleration_ms2", accel) \
                 .withColumn("step_fuel_l", step_fuel_l) \
                 .withColumn("step_elec_kwh", step_elec_kwh) \
                 .withColumn("step_hvac_kwh", step_hvac_kwh) \
                 .withColumn("has_fuel", has_fuel_reading) \
                 .withColumn("has_batt", has_battery_reading)

    # 8. Trip Aggregations (Gold Rollup)
    trip_summary = df_steps.groupBy("vehid", "trip", "batch_week").agg(
        F.min("event_time").alias("trip_start_time"),
        F.max("event_time").alias("trip_end_time"),
        F.round(F.sum("delta_t_s") / 60.0, 2).alias("duration_minutes"),
        F.round(F.sum("step_dist_km"), 2).alias("distance_km"),
        F.round(F.avg("vehicle_speed_kmh"), 2).alias("avg_speed_kmh"),
        F.round(F.sum("step_idling_s") / 60.0, 2).alias("idling_duration_minutes"),
        F.round(F.max("acceleration_ms2"), 2).alias("max_acceleration_ms2"),
        F.round(F.min("acceleration_ms2"), 2).alias("max_deceleration_ms2"),
        F.round(F.sum("step_fuel_l"), 3).alias("fuel_consumed_liters"),
        F.round(F.sum("step_elec_kwh"), 3).alias("energy_consumed_kwh"),
        F.round(F.sum("step_hvac_kwh"), 4).alias("total_hvac_kwh"),
        F.round(F.avg("ambient_temp_c"), 2).alias("avg_ambient_temp_c"),
        F.round(F.avg("hvac_power_watts"), 2).alias("avg_hvac_power_watts"),
        F.round(F.max("precipitation_mm"), 1).alias("max_precipitation_mm"),
        F.max("has_fuel").alias("trip_has_fuel"),
        F.max("has_batt").alias("trip_has_batt")
    )

    # Classify powertrain from observed telemetry signals
    # (Initial fallback classification; downstream enrich_with_scd2_costing_and_esg joins dim_vehicle as authoritative truth)
    trip_summary = trip_summary.withColumn(
        "engine_type",
        F.when(F.col("trip_has_fuel") & F.col("trip_has_batt"), F.lit("PHEV"))
         .when(F.col("trip_has_fuel"), F.lit("ICE"))
         .when(F.col("trip_has_batt"), F.lit("EV"))
         .otherwise(F.lit("ICE"))
    ).drop("trip_has_fuel", "trip_has_batt")

    # Fill default zeros for thedatabase contract
    trip_summary = trip_summary \
        .withColumn("fuel_consumed_liters", F.coalesce(F.col("fuel_consumed_liters"), F.lit(0.0))) \
        .withColumn("energy_consumed_kwh", F.coalesce(F.col("energy_consumed_kwh"), F.lit(0.0)))

    return trip_summary


def enrich_with_scd2_costing_and_esg(trips_df, pricing_df, vehicle_df=None):
    """
    Performs SCD Type 2 point-in-time matching to calculate total costs and emissions.
    """
    # Authoritative vehicle dimension broadcast join to ensure HEV, ICE, PHEV, and EV are accurately attributed
    if vehicle_df is not None:
        trips_df = trips_df.drop("engine_type").join(
            F.broadcast(vehicle_df),
            trips_df["vehid"] == vehicle_df["veh_id"],
            how="left"
        ).drop(vehicle_df["veh_id"])
        trips_df = trips_df.withColumn(
            "engine_type",
            F.coalesce(F.col("engine_type"), F.lit("ICE"))
        )

    gas_tariffs = pricing_df.filter(F.col("energy_type") == "Gasoline_Regular") \
                            .select(
                                F.col("price_per_unit").alias("gas_price"),
                                F.col("co2_kg_per_unit").alias("gas_co2_factor"),
                                F.col("valid_from").alias("gas_valid_from"),
                                F.col("valid_to").alias("gas_valid_to")
                            )

    elec_tariffs = pricing_df.filter(F.col("energy_type") == "Electricity_Commercial") \
                             .select(
                                 F.col("price_per_unit").alias("elec_price"),
                                 F.col("co2_kg_per_unit").alias("elec_co2_factor"),
                                 F.col("valid_from").alias("elec_valid_from"),
                                 F.col("valid_to").alias("elec_valid_to")
                             )

    gas_cond = (
        (trips_df["trip_start_time"] >= gas_tariffs["gas_valid_from"]) &
        (gas_tariffs["gas_valid_to"].isNull() | (trips_df["trip_start_time"] < gas_tariffs["gas_valid_to"]))
    )

    elec_cond = (
        (trips_df["trip_start_time"] >= elec_tariffs["elec_valid_from"]) &
        (elec_tariffs["elec_valid_to"].isNull() | (trips_df["trip_start_time"] < elec_tariffs["elec_valid_to"]))
    )

    enriched = trips_df.join(F.broadcast(gas_tariffs), on=gas_cond, how="left") \
                       .join(F.broadcast(elec_tariffs), on=elec_cond, how="left")

    # Financial Cost ($)
    fuel_cost = F.col("fuel_consumed_liters") * F.coalesce(F.col("gas_price"), F.lit(0.7710))
    elec_cost = F.col("energy_consumed_kwh") * F.coalesce(F.col("elec_price"), F.lit(0.1120))
    total_cost = F.round(F.greatest(F.lit(0.0), fuel_cost + elec_cost), 2)

    # Emissions (kg CO2)
    scope1_emissions = F.col("fuel_consumed_liters") * F.coalesce(F.col("gas_co2_factor"), F.lit(2.3100))
    scope2_emissions = F.col("energy_consumed_kwh") * F.coalesce(F.col("elec_co2_factor"), F.lit(0.5400))
    total_co2 = F.round(F.greatest(F.lit(0.0), scope1_emissions + scope2_emissions), 2)

    # Avoided Emissions vs Fleet ICE baseline
    baseline_co2 = F.col("distance_km") * F.lit(ICE_BASELINE_CO2_PER_KM)
    avoided_co2 = F.round(
        F.when(F.col("engine_type").isin("EV", "PHEV", "HEV"),
               F.greatest(F.lit(0.0), baseline_co2 - total_co2))
         .otherwise(F.lit(0.0)),
        2
    )

    # Season Category
    year = F.year("trip_start_time")
    month = F.month("trip_start_time")
    season_cat = (
        F.when((year == 2017) & (month == 11), F.lit("FALL_2017"))
        .when(((year == 2017) & (month == 12)) | ((year == 2018) & (month.isin(1, 2))), F.lit("WINTER_2018"))
        .when((year == 2018) & (month.isin(3, 4, 5)), F.lit("SPRING_2018"))
        .when((year == 2018) & (month.isin(6, 7, 8)), F.lit("SUMMER_2018"))
        .otherwise(F.lit("FALL_LIVE"))
    )

    # Deterministic 64-bit BIGINT trip_id for database contract
    trip_hash = F.abs(F.hash(
        F.concat(F.col("vehid"), F.lit("_"), F.col("trip"), F.lit("_"), F.date_format("trip_start_time", "yyyyMMddHHmmss"))
    )).cast(LongType())

    # Build final schema matching fact_trips_summary
    gold_df = enriched.select(
        trip_hash.alias("trip_id"),
        F.col("vehid").alias("veh_id"),
        F.col("engine_type"),
        season_cat.alias("season_category"),
        F.col("trip_start_time"),
        F.col("trip_end_time"),
        F.col("duration_minutes"),
        F.col("distance_km"),
        F.col("avg_speed_kmh"),
        F.col("idling_duration_minutes"),
        F.col("fuel_consumed_liters"),
        F.col("energy_consumed_kwh"),
        total_cost.alias("total_cost_usd"),
        total_co2.alias("co2_emissions_kg"),
        avoided_co2.alias("co2_avoided_kg"),
        F.col("avg_ambient_temp_c"),
        F.col("avg_hvac_power_watts"),
        F.col("batch_week")
    )

    return gold_df


def sync_gold_to_postgres(gold_df):
    """Idempotently upserts Gold trips into PostgreSQL fact_trips_summary."""
    
    print("5. Syncing Gold metrics to PostgreSQL fact_trips_summary...")
    pdf = gold_df.toPandas()
    rows = [tuple(x) for x in pdf.to_numpy()]

    upsert_sql = """
        INSERT INTO fact_trips_summary (
            trip_id, veh_id, engine_type, season_category, trip_start_time, trip_end_time,
            duration_minutes, distance_km, avg_speed_kmh, idling_duration_minutes,
            fuel_consumed_liters, energy_consumed_kwh, total_cost_usd,
            co2_emissions_kg, co2_avoided_kg, avg_ambient_temp_c, avg_hvac_power_watts
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (trip_id) DO UPDATE SET
            veh_id = EXCLUDED.veh_id,
            engine_type = EXCLUDED.engine_type,
            season_category = EXCLUDED.season_category,
            trip_start_time = EXCLUDED.trip_start_time,
            trip_end_time = EXCLUDED.trip_end_time,
            duration_minutes = EXCLUDED.duration_minutes,
            distance_km = EXCLUDED.distance_km,
            avg_speed_kmh = EXCLUDED.avg_speed_kmh,
            idling_duration_minutes = EXCLUDED.idling_duration_minutes,
            fuel_consumed_liters = EXCLUDED.fuel_consumed_liters,
            energy_consumed_kwh = EXCLUDED.energy_consumed_kwh,
            total_cost_usd = EXCLUDED.total_cost_usd,
            co2_emissions_kg = EXCLUDED.co2_emissions_kg,
            co2_avoided_kg = EXCLUDED.co2_avoided_kg,
            avg_ambient_temp_c = EXCLUDED.avg_ambient_temp_c,
            avg_hvac_power_watts = EXCLUDED.avg_hvac_power_watts;
    """

    with get_db_cursor(commit=True) as cur:
        execute_batch(cur, upsert_sql, rows, page_size=1000)

    print(f"-- Successfully synced {len(rows):,} trips into PostgreSQL fact_trips_summary.")

def run_silver_to_gold_pipeline(input_path, output_path):
    spark = get_spark_session()

    print(f"\n1. Ingesting Silver lakehouse from: {input_path}")
    if not glob.glob(input_path):
        print(f"ERROR: Input path {input_path} does not exist. Please run the Silver ingestion first.")
        sys.exit(1)
    else:
        silver_df = spark.read.parquet(input_path)
        
    # Extract batch_week directly from the input_path argument
    batch_week = input_path.split("=")[-1] if "=" in input_path else "UNKNOWN"
    silver_df = silver_df.withColumn("batch_week", F.lit(batch_week))

    print("2. Integrating physical sensor streams (Riemann sums & idling detection)...")

    trips_base = compute_trip_physics(silver_df)

    print("3. Matching SCD Type 2 tariffs and calculating ESG benchmarks...")
    pricing_df = get_scd2_energy_pricing_df(spark)
    vehicle_df = get_dim_vehicle_df(spark)
    gold_df = enrich_with_scd2_costing_and_esg(trips_base, pricing_df, vehicle_df)

    print(f"4. Writing Gold Lakehouse Parquet to: {output_path}")
    gold_df.write.mode("overwrite").partitionBy("batch_week").parquet(output_path)

    sync_gold_to_postgres(gold_df.drop("batch_week"))
    

    total_trips = gold_df.count()
    print(f"\n-- Gold Ingestion Complete: {total_trips:,} trips materialized --")

    #print("\n--- Powertrain Distribution in Gold Dataset ---")
    #gold_df.groupBy("engine_type").count().show()

    #print("\n--- Sample fact_trips_summary (Financial & Environmental Bottom Line) ---")
    #gold_df.select(
    #    "trip_id", "veh_id", "engine_type", "season_category", 
    #    "distance_km", "duration_minutes", "total_cost_usd", "co2_emissions_kg", "co2_avoided_kg"
    #).show(5, truncate=False)


    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Silver to Gold Pipeline")
    parser.add_argument("--input-path", type=str, default="data/silver/telemetry_enriched")
    parser.add_argument("--output-path", type=str, default="data/gold/fact_trips_summary")
    args = parser.parse_args()

    run_silver_to_gold_pipeline(args.input_path, args.output_path)