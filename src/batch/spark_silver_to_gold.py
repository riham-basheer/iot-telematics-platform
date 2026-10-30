import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Automotive Constants for Gasoline Stoichiometry
GASOLINE_AIR_FUEL_RATIO = 14.7
GASOLINE_DENSITY_G_L = 745.0


def get_spark_session(app_name="VED_Silver_to_Gold"):
    return SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()


def compute_trip_physics(df):
    """
    Computes kinematics and trapezoidal Riemann sum integrals:
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


    # Instantaneous Acceleration
    delta_v_ms = (F.col("vehicle_speed_kmh") - F.coalesce(prev_speed, F.col("vehicle_speed_kmh"))) * (1000.0 / 3600.0)
    accel = F.when(delta_t > 0.0, delta_v_ms / delta_t).otherwise(0.0)

    # 3. Fuel Rate Resolution: Use direct PID if present, else fallback to stoichiometric MAF
    maf_derived_fuel = (F.col("maf_g_sec") * 3600.0) / (GASOLINE_AIR_FUEL_RATIO * GASOLINE_DENSITY_G_L)
    effective_fuel_rate = F.coalesce(F.col("fuel_rate_l_hr"), maf_derived_fuel)

    prev_fuel_rate = F.lag(effective_fuel_rate).over(trip_window)
    avg_fuel_rate = (effective_fuel_rate + F.coalesce(prev_fuel_rate, effective_fuel_rate)) / 2.0
    step_fuel_l = F.when(
        effective_fuel_rate.isNotNull() & prev_fuel_rate.isNotNull(),
        avg_fuel_rate * (delta_t / 3600.0)
    ).otherwise(None)

    # 4. Electric Traction Energy: P = V * I (Watts)
    # In VED, negative current = discharge (consumed).
    # Invert sign so net energy consumption is positive::
    curr_batt_power = -1.0 * (F.col("hv_battery_voltage_v") * F.col("hv_battery_current_a"))
    prev_batt_power = F.lag(curr_batt_power).over(trip_window)
    avg_batt_power = (curr_batt_power + F.coalesce(prev_batt_power, curr_batt_power)) / 2.0

    step_elec_kwh = F.when(
        curr_batt_power.isNotNull() & prev_batt_power.isNotNull(),
        avg_batt_power * (delta_t / 3.6e6)
    ).otherwise(None)

    # 5. HVAC Auxiliary Energy (kWh)
    prev_hvac_power = F.lag("hvac_power_watts").over(trip_window)
    avg_hvac_power = (F.col("hvac_power_watts") + F.coalesce(prev_hvac_power, F.col("hvac_power_watts"))) / 2.0

    step_hvac_kwh = F.when(
        F.col("hvac_power_watts").isNotNull() & prev_hvac_power.isNotNull(),
        avg_hvac_power * (delta_t / 3.6e6)
    ).otherwise(None) # to leave the ICE vehicles as null

    # Append step calculations
    df_steps = df.withColumn("delta_t_s", delta_t) \
                 .withColumn("step_dist_km", step_distance_km) \
                 .withColumn("acceleration_ms2", accel) \
                 .withColumn("step_fuel_l", step_fuel_l) \
                 .withColumn("step_elec_kwh", step_elec_kwh) \
                 .withColumn("step_hvac_kwh", step_hvac_kwh)

    # 6. Trip Aggregations (Gold Rollup)
    trip_summary = df_steps.groupBy("vehid", "trip").agg(
        F.min("event_time").alias("trip_start_time"),
        F.max("event_time").alias("trip_end_time"),
        F.round(F.sum("delta_t_s"), 2).alias("duration_sec"),
        F.round(F.sum("step_dist_km"), 3).alias("total_distance_km"),
        F.round(F.max("vehicle_speed_kmh"), 1).alias("max_speed_kmh"),
        F.round(F.avg("vehicle_speed_kmh"), 1).alias("avg_speed_kmh"),
        F.round(F.max("acceleration_ms2"), 2).alias("max_acceleration_ms2"),
        F.round(F.min("acceleration_ms2"), 2).alias("max_deceleration_ms2"),
        F.round(F.sum("step_fuel_l"), 4).alias("total_fuel_liters"),
        F.round(F.sum("step_elec_kwh"), 4).alias("total_electric_kwh"),
        F.round(F.sum("step_hvac_kwh"), 4).alias("total_hvac_kwh"),
        F.round(F.avg("ambient_temp_c"), 1).alias("avg_ambient_temp_c"),
        F.round(F.max("precipitation_mm"), 1).alias("max_precipitation_mm")
    )

    trip_summary = trip_summary.withColumn(
        "trip_id",
        F.concat(F.col("vehid"), F.lit("_"), F.col("trip"), F.lit("_"), F.date_format("trip_start_time", "yyyyMMdd"))
    )

    return trip_summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Silver to Gold Physics Engine")
    parser.add_argument("--input-path", type=str, default="data/silver/telemetry_enriched")
    parser.add_argument("--output-path", type=str, default="data/gold/fact_trips_summary")
    args = parser.parse_args()


    spark = get_spark_session()
    print(f"\n1. Ingesting Silver Lakehouse from: {args.input_path}")
    silver_df = spark.read.parquet(args.input_path)

    print("2. Integrating physical sensor streams (Riemann sums with MAF fallback)...")
    trips_gold = compute_trip_physics(silver_df)

    print(f"Total Unique Trips Extracted: {trips_gold.count()}")

    print("\n--- Sample ICE Trips (Fuel Liters Populated) ---")
    trips_gold.filter(F.col("total_fuel_liters").isNotNull()) \
              .select("trip_id", "total_distance_km", "duration_sec", "total_fuel_liters", "total_electric_kwh") \
              .show(3, truncate=False)

    print("--- Sample EV Trips (Electric kWh Populated) ---")
    trips_gold.filter(F.col("total_electric_kwh").isNotNull()) \
              .select("trip_id", "total_distance_km", "duration_sec", "total_fuel_liters", "total_electric_kwh", "total_hvac_kwh") \
              .show(3, truncate=False)


    spark.stop()