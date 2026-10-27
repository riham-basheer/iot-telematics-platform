import os
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Ann Arbor: MI Coordinates and Historical Window (for only the 50 Weeks)
LATITUDE = 42.2713
LONGITUDE = -83.7255
START_DATE = "2017-11-01"
END_DATE = "2018-06-13"

OUTPUT_DIR = "data/bronze/weather/historical_bulk"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "weather_ann_arbor_2017_2018.parquet")

def fetch_and_cache_historical_weather():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "start_date": START_DATE,
        "end_date": END_DATE,
        "hourly": "temperature_2m,precipitation,wind_speed_10m",
        "timezone": "UTC"
    }

    print(f"Requesting bulk historical weather for Ann Arbor ({START_DATE} to {END_DATE})...")
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    payload = response.json()

    hourly = payload.get("hourly", {})
    if not hourly or "time" not in hourly:
        raise ValueError("Invalid Open-Meteo response structure")

    # Construct tabular structure with the 4 cols needed later for the bronze table
    df = pd.DataFrame({
        "weather_time": pd.to_datetime(hourly["time"], utc=True),
        "temperature_c": pd.to_numeric(hourly.get("temperature_2m", []), errors="coerce"),
        "precipitation_mm": pd.to_numeric(hourly.get("precipitation", []), errors="coerce"),
        "wind_speed_kmh": pd.to_numeric(hourly.get("wind_speed_10m", []), errors="coerce"),
    })

    print(f" Downloaded {len(df)} hourly weather records.")

    # Convert and save as Parquet
    table = pa.Table.from_pandas(df)
    pq.write_table(table, OUTPUT_FILE, compression="snappy")

    file_size_kb = os.path.getsize(OUTPUT_FILE) / 1024
    print(f" Cached bronze Parquet to: {OUTPUT_FILE} ({file_size_kb:.2f} KB)")

if __name__ == "__main__":
    fetch_and_cache_historical_weather()
