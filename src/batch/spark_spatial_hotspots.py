import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.db_connection import get_db_cursor

# Constants matching spark_silver_to_gold.py
GASOLINE_AIR_FUEL_RATIO = 14.7
GASOLINE_DENSITY_G_L = 745.0
AVG_GAS_PRICE_PER_LITER = 0.778  # Average EIA Michigan tariff ($/L)


def get_spark_session():
    return (
        SparkSession.builder
        .appName("VED_Spatial_Hotspots")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def build_spatial_hotspots():
    spark = get_spark_session()
    silver_path = str(PROJECT_ROOT / "data/silver/telemetry_enriched")
    gold_out_path = str(PROJECT_ROOT / "data/gold/fact_spatial_hotspots")

    print(f"1. Reading Silver Lakehouse from: {silver_path}")
    df = spark.read.parquet(silver_path)

    # 2. Calculate elapsed time (delta_t) between consecutive pings in a trip
    trip_window = Window.partitionBy("vehid", "trip").orderBy("timestamp_ms")
    prev_ms = F.lag("timestamp_ms").over(trip_window)
    delta_t_raw = (F.col("timestamp_ms") - prev_ms) / 1000.0
    delta_t = F.when((delta_t_raw > 0.0) & (delta_t_raw <= 30.0), delta_t_raw).otherwise(0.0)

    # 3. Calculate instantaneous fuel rate (with MAF stoichiometric fallback)
    maf_derived_fuel = (F.col("maf_g_sec") * 3600.0) / (GASOLINE_AIR_FUEL_RATIO * GASOLINE_DENSITY_G_L)
    effective_fuel_rate = F.coalesce(F.col("fuel_rate_l_hr"), maf_derived_fuel, F.lit(0.0))
    step_fuel_liters = effective_fuel_rate * (delta_t / 3600.0)

    # 4. Filter strictly for stationary idling events (speed == 0 with active engine)
    is_idling = (F.col("vehicle_speed_kmh") == 0.0) & (
        (F.coalesce(F.col("engine_rpm"), F.lit(0.0)) > 0.0)
        | (F.coalesce(F.col("fuel_rate_l_hr"), F.lit(0.0)) > 0.0)
        | (F.coalesce(F.col("maf_g_sec"), F.lit(0.0)) > 0.0)
        | (F.coalesce(F.abs(F.col("hv_battery_current_a")), F.lit(0.0)) > 0.1)
    )

    idle_df = (
        df.withColumn("delta_t_s", delta_t)
        .withColumn("step_fuel_l", step_fuel_liters)
        .filter(is_idling & (F.col("delta_t_s") > 0.0))
    )

    # 5. Assign Seasonal Category (matching our corrected meteorological seasons)
    year = F.year("event_time")
    month = F.month("event_time")
    season_cat = (
        F.when((year == 2017) & (month == 11), F.lit("FALL_2017"))
        .when(((year == 2017) & (month == 12)) | ((year == 2018) & (month.isin(1, 2))), F.lit("WINTER_2018"))
        .when((year == 2018) & (month.isin(3, 4, 5)), F.lit("SPRING_2018"))
        .when((year == 2018) & (month.isin(6, 7, 8)), F.lit("SUMMER_2018"))
        .otherwise(F.lit("FALL_LIVE"))
    )

    # 6. Snap GPS coordinates to 3 decimal places (~110m x 82m intersection grid)
    grid_df = (
        idle_df.withColumn("cluster_latitude", F.round(F.col("latitude"), 3))
        .withColumn("cluster_longitude", F.round(F.col("longitude"), 3))
        .withColumn("season_category", season_cat)
    )

    print("2. Aggregating idling clusters by 3-decimal GPS grid and season...")
    hotspots_df = (
        grid_df.groupBy("cluster_latitude", "cluster_longitude", "season_category")
        .agg(
            F.count("*").cast("int").alias("total_idling_events"),
            F.round(F.sum("delta_t_s") / 3600.0, 2).alias("total_idling_hours"),
            F.round(F.sum("step_fuel_l"), 2).alias("wasted_fuel_liters"),
        )
        .withColumn(
            "wasted_cost_usd",
            F.round(F.col("wasted_fuel_liters") * F.lit(AVG_GAS_PRICE_PER_LITER), 2),
        )
        # Filter out random one-off stops (< 0.05 hours / 3 minutes cumulative idle)
        .filter(F.col("total_idling_hours") >= 0.05)
    )

    # 7. Save to Gold Parquet
    print(f"3. Writing Gold Parquet to: {gold_out_path}")
    hotspots_df.write.mode("overwrite").parquet(gold_out_path)

    # 8. Collect and load into PostgreSQL fact_spatial_hotspots
    rows = [
        (
            float(r["cluster_latitude"]),
            float(r["cluster_longitude"]),
            str(r["season_category"]),
            int(r["total_idling_events"]),
            float(r["total_idling_hours"]),
            float(r["wasted_fuel_liters"]),
            float(r["wasted_cost_usd"]),
        )
        for r in hotspots_df.collect()
    ]

    with get_db_cursor(commit=True) as cur:
        cur.execute("TRUNCATE TABLE fact_spatial_hotspots RESTART IDENTITY;")
        insert_sql = """
            INSERT INTO fact_spatial_hotspots (
                cluster_latitude, cluster_longitude, season_category,
                total_idling_events, total_idling_hours, wasted_fuel_liters, wasted_cost_usd
            ) VALUES (%s, %s, %s, %s, %s, %s, %s);
        """
        cur.executemany(insert_sql, rows)

    print(f"[✓] Successfully populated {len(rows):,} spatial hotspot records into PostgreSQL!")
    spark.stop()


if __name__ == "__main__":
    build_spatial_hotspots()
