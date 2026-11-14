import json
import os
import subprocess
from datetime import datetime, timedelta
from airflow import DAG  
from airflow.operators.python import PythonOperator  
import pandas as pd
import requests

PROJECT_ROOT = (
    "/opt/airflow"
    if os.path.exists("/opt/airflow/src")
    else os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
)

LATITUDE = 42.2713
LONGITUDE = -83.7255
SIMULATION_DATE = "2018-10-24"
PARQUET_WEATHER_FILE = os.path.join(
    PROJECT_ROOT,
    "data/bronze/weather/historical_bulk/weather_ann_arbor_2017_2018.parquet",
)


def fetch_hourly_weather(**context):
  """Fetches hourly Open-Meteo weather for Ann Arbor."""

  logical_date = context.get("ds") or SIMULATION_DATE
  target_date = SIMULATION_DATE if logical_date > "2018-12-31" else logical_date

  dt = datetime.strptime(target_date, "%Y-%m-%d")
  out_dir = os.path.join(
      PROJECT_ROOT,
      f"data/bronze/weather/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}",
  )
  os.makedirs(out_dir, exist_ok=True)
  out_file = os.path.join(out_dir, f"weather_{target_date}.json")

  url = "https://archive-api.open-meteo.com/v1/archive"
  params = {
      "latitude": LATITUDE,
      "longitude": LONGITUDE,
      "start_date": target_date,
      "end_date": target_date,
      "hourly": "temperature_2m,precipitation,wind_speed_10m",
      "timezone": "UTC",
  }

  resp = requests.get(url, params=params, timeout=30)
  resp.raise_for_status()
  payload = resp.json()

  with open(out_file, "w") as f:
    json.dump(payload, f, indent=2)

  print(f"[Done] Saved hourly weather JSON to {out_file}")
  return out_file


def validate_weather_schema(**context):
  """Enforces physical temperature bounds (-40°C <= T <= 50°C) and non-null arrays."""
  ti = context["ti"]
  json_path = ti.xcom_pull(task_ids="task_fetch_hourly_weather")
  if not json_path or not os.path.exists(json_path):
    raise FileNotFoundError(f"Weather payload missing at: {json_path}")

  with open(json_path, "r") as f:
    payload = json.load(f)

  hourly = payload.get("hourly", {})
  temps = hourly.get("temperature_2m", [])
  if not temps:
    raise ValueError("Validation failed: 'temperature_2m' array is empty.")

  for t in temps:
    if t is None or not (-40.0 <= float(t) <= 50.0):
      raise ValueError(
          f"Validation failed: temperature {t}°C outside [-40, 50] bounds."
      )

  print(
      "[Done] Weather schema & boundary validation passed ("
      f"{len(temps)} hourly records)."
  )


def update_weather_parquet_cache(**context):
  """Appends validated hourly records directly to the Bronze weather Parquet file."""
  ti = context["ti"]
  json_path = ti.xcom_pull(task_ids="task_fetch_hourly_weather")

  with open(json_path, "r") as f:
    payload = json.load(f)

  hourly = payload.get("hourly", {})
  df_new = pd.DataFrame({
      "weather_time": pd.to_datetime(hourly["time"], utc=True),
      "temperature_c": pd.to_numeric(
          hourly.get("temperature_2m", []), errors="coerce"
      ),
      "precipitation_mm": pd.to_numeric(
          hourly.get("precipitation", []), errors="coerce"
      ),
      "wind_speed_kmh": pd.to_numeric(
          hourly.get("wind_speed_10m", []), errors="coerce"
      ),
  })

  os.makedirs(os.path.dirname(PARQUET_WEATHER_FILE), exist_ok=True)

  if os.path.exists(PARQUET_WEATHER_FILE):
    df_existing = pd.read_parquet(PARQUET_WEATHER_FILE)
    df_combined = pd.concat([df_existing, df_new], ignore_index=True)
    df_combined = df_combined.drop_duplicates(subset=["weather_time"]).sort_values(
        "weather_time"
    )
  else:
    df_combined = df_new

  df_combined.to_parquet(
      PARQUET_WEATHER_FILE, engine="pyarrow", compression="snappy"
  )
  print(
      f"[Done] Successfully merged {len(df_new)} hourly weather records into"
      f" {PARQUET_WEATHER_FILE}"
  )


def sync_charging_stations():
  """Idempotently syncs NREL charging stations into PostgreSQL dim_charging_stations."""
  env = os.environ.copy()
  env["PYTHONPATH"] = PROJECT_ROOT
  if os.path.exists("/opt/airflow"):
    env["DB_HOST"] = "postgres"
    env["DB_NAME"] = "telematics_dwh"

  script_path = os.path.join(PROJECT_ROOT, "db/fetch_nrel_stations.py")
  subprocess.run(["python3", script_path], env=env, check=True)


default_args = {
    "owner": "riham-elsayed",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="dag_external_ingestion_hourly",
    default_args=default_args,
    description=(
        "Hourly weather polling, validation, Parquet cache sync, and NREL"
        " station sync"
    ),
    schedule="@hourly",
    start_date=datetime(2018, 10, 24),
    catchup=False,
    tags=["telematics", "ingestion", "weather", "nrel", "hourly"],
) as dag:

  task_fetch_hourly_weather = PythonOperator(
      task_id="task_fetch_hourly_weather",
      python_callable=fetch_hourly_weather,
  )

  task_validate_weather_schema = PythonOperator(
      task_id="task_validate_weather_schema",
      python_callable=validate_weather_schema,
  )

  task_update_weather_cache = PythonOperator(
      task_id="task_update_weather_cache",
      python_callable=update_weather_parquet_cache,
  )

  task_sync_charging_stations = PythonOperator(
      task_id="task_sync_charging_stations",
      python_callable=sync_charging_stations,
  )

  (
      task_fetch_hourly_weather
      >> task_validate_weather_schema
      >> task_update_weather_cache
      >> task_sync_charging_stations
  )