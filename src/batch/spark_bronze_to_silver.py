import os
import sys
import argparse
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
        return F.col(name).cast(dtype) if name in df.columns else F.lit(None).cast(dtype)

    df_final = df_dedup.withColumn("engine_rpm", safe_cast("engine_rpm_rpm", DoubleType())) \
                .withColumn("fuel_rate_l_hr", safe_cast("fuel_rate_l_per_hr", DoubleType())) \
                .withColumn("maf_g_sec", safe_cast("maf_g_per_sec", DoubleType())) \
                .withColumn("hv_battery_voltage_v", safe_cast("hv_battery_voltage_v", DoubleType())) \
                .withColumn("hv_battery_current_a", safe_cast("hv_battery_current_a", DoubleType())) \
                .withColumn("hv_battery_soc_pct", safe_cast("hv_battery_soc_pct", DoubleType())) \
                .withColumn("ac_power_watts", safe_cast("air_conditioning_power_watts", DoubleType())) \
                .withColumn("heater_power_watts", safe_cast("heater_power_watts", DoubleType()))

    # (7) Compute combined HVAC power draw
    df_final = df_final.withColumn("hvac_power_watts", 
        F.coalesce(F.col("ac_power_watts"), F.lit(0.0)) + F.coalesce(F.col("heater_power_watts"), F.lit(0.0)))

    # truncating hour for the downstream weather alignment
    df_final = df_final.withColumn("event_hour", F.date_trunc("hour", F.col("event_time")))

    return df_final


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BRONZE to SILVER telemetry Pipeline for VED dataset")
    parser.add_argument("--input_path", type=str, default="data/raw/VED_171101_week.csv", help="Path to the raw bronze CSV file.")
    #parser.add_argument("--output_path", type=str, default="data/silver/", help="Path to save the transformed silver Parquet file.")
    args = parser.parse_args()

    spark = get_spark_session()

    print(f"\n--- Reading telemetry from {args.input_path} ---")
    if os.path.exists(args.input_path):
        df_bronze = spark.read.option("header", "true").option("inferSchema", "true").csv(args.input_path)
        df_silver = transform_bronze_telemetry(df_bronze)
        #print(f"\n--- Writing transformed silver telemetry to {args.output_path} ---")
        #df_silver.write.mode("overwrite").parquet(args.output_path)

        print(f"\n--- YAY! Bronze to Silver Transformation is complete. Total rows: {df_silver.count()} ----")
    else:
        print(f"\n--- ERROR: Input path {args.input_path} does not exist. Please provide a valid path. ---")