import os
import sys
import argparse

import warnings
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, LongType, TimestampType)

# Geographic bounding box for Ann Arbor, MI study area
LAT_MIN, LAT_MAX = 42.0, 42.5
LON_MIN, LON_MAX = -84.0, -83.5

VED_BASE_EPOCH = "2017-11-01 00:00:00"  # DayNum 1.0 = 2017-11-01 00:00:00 UTC

def get_spark_session(app_name="VED_Bronze_to_Silver"):
    """
    Create and return a SparkSession optimized for lakehouse batch processing.
    """
    return SparkSession.builder.appName(app_name) \
        .master("local[*]") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .getOrCreate()


def clean_column_headers(df):
    """
    Cleans raw VED CSV headers by removing whitespace, special characters, and converting to lowercase.
    """
    for col in df.columns:          
        clean_col = col.strip().replace(" ", "_") \
            .replace("[", "_").replace("]", "") \
            .replace("(", "_").replace(")", "") \
            .replace("/", "_per_").replace("%", "pct") \
            .lower()
        df = df.withColumnRenamed(col, clean_col)
    return df




def transform_bronze_telemetry(df):
    """
    Cleans raw bronze telemetry data by doing these things:
        (1) Clean column headers
        (2) Map coordinates, trip, and elapsed time columns
        (3) Derive real UTC event_time with sub-second / second precision
        (4) Filter GPS bounding box (Ann Arbor study area)
        (5) Filter Physical Outliers (glitched negative speed or extreme speed spikes > 200 km/h)
        (6) Deduplicate only identical sensor packets for the same vehicle, trip, and millisecond
        (7) Compute combined HVAC power draw
    """

    #(1) Clean column headers
    df = clean_column_headers(df)

    #(2) Map coordinates, trip, and elapsed time columns
    lat_col = "latitude_deg" if "latitude_deg" in df.columns else "latitude"
    lon_col = "longitude_deg" if "longitude_deg" in df.columns else "longitude"
    speed_col = [c for c in df.columns if "speed" in c][0]
    ms_col = [c for c in df.columns if "timestamp" in c or "ms" in c][0]

    # Cast columns to appropriate types
    df = df.withColumn("daynum", F.col("daynum").cast(DoubleType())) \
           .withColumn("vehid", F.col("vehid").cast(IntegerType())) \
           .withColumn("trip", F.col("trip").cast(LongType())) \
           .withColumn("timestamp_ms", F.coalesce(F.col(ms_col).cast(DoubleType()), F.lit(0.0))) \
           .withColumn("latitude", F.col(lat_col).cast(DoubleType())) \
           .withColumn("longitude", F.col(lon_col).cast(DoubleType())) \
           .withColumn("vehicle_speed_kmh", F.col(speed_col).cast(DoubleType()))

    #(3) Derive real UTC event_time with sub-second / second precision
    base_epoch = F.unix_timestamp(F.lit(VED_BASE_EPOCH))
    total_epoch_seconds = base_epoch + ((F.col("daynum") - 1.0) * 86400.0) + (F.col("timestamp_ms") / 1000.0)
    df = df.withColumn("event_time", total_epoch_seconds.cast(TimestampType()))

    # (4) Filter GPS bounding box (Ann Arbor study area)
    df_valid_gps = df.filter(
        (F.col("latitude") >= LAT_MIN) & (F.col("latitude") <= LAT_MAX) &
        (F.col("longitude") >= LON_MIN) & (F.col("longitude") <= LON_MAX)
    )   

    # (5) Filter Physical Outliers (glitched negative speed or extreme speed spikes > 200 km/h)
    df_valid_physics = df_valid_gps.filter(
        (F.col("vehicle_speed_kmh") >= 0.0) & (F.col("vehicle_speed_kmh") <= 200.0)
    )   

    # (6) Deduplicate only identical sensor packets for the same vehicle, trip, and millisecond
    df_dedup = df_valid_physics.dropDuplicates(["vehid", "trip", "timestamp_ms"])

    # safely casting physical signals (preserving all the nulls)
    def safe_cast(name, dtype):
        # Convert string and IEEE NaN into true SQL NULL for all physical sensors
        if name not in df.columns:
            return F.lit(None).cast(dtype)
        c = F.col(name).cast(dtype)
        return F.when(c.isNull() | F.isnan(c), F.lit(None).cast(dtype)).otherwise(c)

    df_final = df_dedup.withColumn("engine_rpm", safe_cast("engine_rpm_rpm", DoubleType())) \
                .withColumn("fuel_rate_l_hr", safe_cast("fuel_rate_l_per_hr", DoubleType())) \
                .withColumn("maf_g_sec", safe_cast("maf_g_per_sec", DoubleType())) \
                .withColumn("hv_battery_voltage_v", safe_cast("hv_battery_voltage_v", DoubleType())) \
                .withColumn("hv_battery_current_a", safe_cast("hv_battery_current_a", DoubleType())) \
                .withColumn("hv_battery_soc_pct", safe_cast("hv_battery_soc_pct", DoubleType())) \
                .withColumn("ac_power_watts", safe_cast("air_conditioning_power_watts", DoubleType())) \
                .withColumn("heater_power_watts", safe_cast("heater_power_watts", DoubleType()))

    # (7) Compute combined HVAC power draw: NULL for ICE, numeric sum for EV/HEV
    has_ac = F.col("ac_power_watts").isNotNull() & ~F.isnan(F.col("ac_power_watts"))
    has_heater = F.col("heater_power_watts").isNotNull() & ~F.isnan(F.col("heater_power_watts"))

    df_final = df_final.withColumn(
        "hvac_power_watts",
        F.when(
            ~has_ac & ~has_heater,
            F.lit(None).cast(DoubleType())
        ).otherwise(
            F.coalesce(F.nanvl(F.col("ac_power_watts"), F.lit(0.0)), F.lit(0.0)) +
            F.coalesce(F.nanvl(F.col("heater_power_watts"), F.lit(0.0)), F.lit(0.0))
        )
    )

    # truncating hour for the downstream weather alignment
    df_final = df_final.withColumn("event_hour", F.date_trunc("hour", F.col("event_time")))

    return df_final


def join_offline_weather(df_telemetry, parquet_weather_path, spark):
    """
    Join telemetry data with offline weather data (parquet) based on event_hour
    eliminating the need for external API calls during batch processing and backfilling.
    """
    if not os.path.exists(parquet_weather_path):
        warnings.warn(f"Offline Weather Parquet file not found at {parquet_weather_path}. Continuing without weather...")
        return df_telemetry.withColumn("ambient_temp_c", F.lit(None).cast(DoubleType())) \
            .withColumn("precipitation_mm", F.lit(None).cast(DoubleType())) \
            .withColumn("wind_speed_kmh", F.lit(None).cast(DoubleType()))

    print(f"-- Joining telemetry with offline weather data from {parquet_weather_path} ---")
    df_weather = spark.read.parquet(parquet_weather_path)
    df_weather = clean_column_headers(df_weather)

    time_col = "time" if "time" in df_weather.columns else df_weather.columns[0]
    df_weather = df_weather.withColumn("weather_hour", F.date_trunc("hour", F.to_timestamp(F.col(time_col), "yyyy-MM-dd HH:mm:ss")))

    temp_col = [c for c in df_weather.columns if "temp" in c][0]
    precip_col = [c for c in df_weather.columns if "precip" in c][0]
    wind_col = [c for c in df_weather.columns if "wind" in c][0]

    weather_lookup = df_weather.select(
        F.col("weather_hour").alias("event_hour"),
        F.col(temp_col).cast(DoubleType()).alias("ambient_temp_c"),
        F.col(precip_col).cast(DoubleType()).alias("precipitation_mm"),
        F.col(wind_col).cast(DoubleType()).alias("wind_speed_kmh")
    ).dropDuplicates(["event_hour"])

    df_joined = df_telemetry.join(F.broadcast(weather_lookup), on="event_hour", how="left")
    return df_joined



def run_bronze_to_silver_pipeline(input_path, output_path, parquet_weather_path=None):
    """
    Run the Bronze to Silver telemetry pipeline:
        1. Read raw bronze CSV
        2. Transform to silver format
        3. Optionally join with offline weather data
        4. Write to Parquet
    """
    spark = get_spark_session()

    if not os.path.exists(input_path):
        print(f"ERROR: Input path {input_path} does not exist. Please provide a valid path.")
        spark.stop()
        sys.exit(1)
    print(f"\n1. Ingesting raw telemetry from {input_path}")
    df_bronze = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)
    
    print("2. Executing cleaning and feature engineering...")
    df_silver = transform_bronze_telemetry(df_bronze)

    print("3. Joining local weather cache...")
    df_silver = join_offline_weather(df_silver, parquet_weather_path, spark)

    week_id = os.path.basename(input_path).replace(".csv", "")
    df_silver = df_silver.withColumn("year", F.year(F.col("event_time"))) \
                         .withColumn("month", F.format_string("%02d", F.month("event_time"))) \
                         .withColumn("batch_week", F.lit(week_id))

    print(f"4. Writing transformed silver parquet partitioned by year/month to: {output_path}")
    # Change mode("overwrite") -> mode("append")
    df_silver.write.mode("overwrite").partitionBy("year", "month", "batch_week").parquet(output_path)

    print(f"\n--- YAY! Bronze to Silver Transformation is complete. Total rows: {df_silver.count()} ----")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BRONZE to SILVER Pipeline for VED dataset")
    parser.add_argument("--input-path", type=str, default="data/raw/VED_171101_week.csv", \
                         help="Path to the raw bronze CSV file.")
    parser.add_argument("--output-path", type=str, default="data/silver/telemetry_enriched", \
                        help="Path to save the transformed silver Parquet file.")
    parser.add_argument("--weather-cache", type=str, \
                        default="data/bronze/weather/historical_bulk/weather_ann_arbor_2017_2018.parquet", \
                        help="Path to the Parquet weather data file.")

    args = parser.parse_args()

    run_bronze_to_silver_pipeline(args.input_path, args.output_path, args.weather_cache)