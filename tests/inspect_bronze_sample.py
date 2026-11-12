import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, LongType

spark = SparkSession.builder.appName("Inspect_Bronze_Sample") \
    .master("local[*]") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

raw_csv = "data/raw/VED_171101_week.csv"
print(f"\n--- Loading raw sample from {raw_csv} ---")
df = spark.read.option("header", "true").option("inferSchema", "true").csv(raw_csv).limit(10000)
raw_count = df.count()

# 1. Clean column headers
for col in df.columns:
    clean_col = col.strip() \
        .replace(" ", "_") \
        .replace("[", "_") \
        .replace("]", "") \
        .replace("(", "_") \
        .replace(")", "") \
        .replace("/", "_per_") \
        .replace("%", "pct") \
        .lower()
    df = df.withColumnRenamed(col, clean_col)

# 2. Map coordinates, trip, and elapsed time columns
lat_col = "latitude_deg" if "latitude_deg" in df.columns else "latitude"
lon_col = "longitude_deg" if "longitude_deg" in df.columns else "longitude"
speed_col = [c for c in df.columns if "speed" in c][0]
ms_col = [c for c in df.columns if "timestamp" in c or "ms" in c][0]

df = df.withColumn("daynum", F.col("daynum").cast(DoubleType())) \
       .withColumn("vehid", F.col("vehid").cast(IntegerType())) \
       .withColumn("trip", F.col("trip").cast(LongType())) \
       .withColumn("timestamp_ms", F.coalesce(F.col(ms_col).cast(DoubleType()), F.lit(0.0))) \
       .withColumn("latitude", F.col(lat_col).cast(DoubleType())) \
       .withColumn("longitude", F.col(lon_col).cast(DoubleType())) \
       .withColumn("speed_kmh", F.col(speed_col).cast(DoubleType()))

# 3. Derive real UTC event_time with sub-second / second precision
# DayNum 1.0 = 2017-11-01 00:00:00 UTC + elapsed seconds from timestamp_ms
base_epoch = F.unix_timestamp(F.lit("2017-11-01 00:00:00"))
total_epoch_seconds = base_epoch + ((F.col("daynum") - 1.0) * 86400.0) + (F.col("timestamp_ms") / 1000.0)
df = df.withColumn("event_time", total_epoch_seconds.cast("timestamp"))

# 4. Filter GPS bounding box (Ann Arbor study area)
df_valid_gps = df.filter(
    (F.col("latitude") >= 42.0) & (F.col("latitude") <= 42.5) &
    (F.col("longitude") >= -84.0) & (F.col("longitude") <= -83.5)
)
gps_dropped = raw_count - df_valid_gps.count()

# 5. Filter Physical Outliers (glitched negative speed or extreme speed spikes > 200 km/h)
df_valid_physics = df_valid_gps.filter(
    (F.col("speed_kmh") >= 0.0) & (F.col("speed_kmh") <= 200.0)
)
physics_dropped = df_valid_gps.count() - df_valid_physics.count()

# 6. Deduplicate only identical sensor packets for the same vehicle, trip, and millisecond
df_clean = df_valid_physics.dropDuplicates(["vehid", "trip", "timestamp_ms"])
dedup_dropped = df_valid_physics.count() - df_clean.count()

print("\n--- Corrected Cleaning Audit Results ---")
print(f"Total Raw Rows Sampled:         {raw_count}")
print(f"Out-of-Bounds GPS Rows Dropped: {gps_dropped}")
print(f"Physical Glitches Dropped:      {physics_dropped}")
print(f"Duplicate Sensor Pings Dropped: {dedup_dropped}")
print(f"Valid Clean Silver Rows:        {df_clean.count()}")

print("\n--- Continuous 1 Hz Trajectory Sample (Trip 706) ---")
df_clean.filter(F.col("trip") == 706) \
    .select("vehid", "trip", "timestamp_ms", "event_time", "speed_kmh", "latitude", "longitude") \
    .orderBy("timestamp_ms") \
    .show(8, truncate=False)

spark.stop()
