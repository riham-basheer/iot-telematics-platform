from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[*]").getOrCreate()
df = spark.read.parquet("data/silver/telemetry_enriched")

print("\n--- ICE Vehicles (Battery SOC is null -> Electric HVAC is null) ---")
df.filter(F.col("hv_battery_soc_pct").isNull()) \
  .select("vehid", "trip", "event_time", "vehicle_speed_kmh", "hv_battery_soc_pct", "hvac_power_watts") \
  .show(3)

print("--- EV / HEV Vehicles (Battery SOC is numeric -> Electric HVAC is numeric) ---")
df.filter(F.col("hv_battery_soc_pct").isNotNull()) \
  .select("vehid", "trip", "event_time", "hv_battery_soc_pct", "hvac_power_watts") \
  .show(3)

spark.stop()
