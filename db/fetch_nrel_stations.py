import os
import sys
import requests
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv

# Load local environment variables from .env 
load_dotenv()

# Add project root to sys.path for module imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.utils.db_connection import get_db_connection

# API key defaults to DEMO_KEY if not explicitly set in environment
NREL_API_KEY = os.getenv("NREL_API_KEY", "DEMO_KEY")

# Official National Laboratory of the Rockies (NLR) Nearest Stations API
# this API is used to fetch live electric charging stations within a specified radius of a given latitude/longitude
# There is also a "Nearby" API endpoint, but the Nearest API is preferred for bulk retrieval of stations within a radius that's why I used it.
NLR_NEAREST_API_URL = "https://developer.nlr.gov/api/alt-fuel-stations/v1/nearest.json"

# Centroid of Ann Arbor, MI matching the VED telematics bounding box
ANN_ARBOR_LAT = 42.2713
ANN_ARBOR_LON = -83.7255
SEARCH_RADIUS_MILES = 15.0


def fetch_charging_stations(api_key: str, lat: float, lon: float, radius: float) -> list[dict]:
    """
    Queries the live NLR Alternative Fuel Stations Nearest API for electric stations.
    
    Args:
        api_key: Developer API key (you have to put your key in the .env file).
        lat: Latitude of search center.
        lon: Longitude of search center.
        radius: Radial search distance in miles.
        
    Returns:
        List of raw station dictionaries returned by the API.
    """
    params = {
        "api_key": api_key,
        "fuel_type": "ELEC",
        "latitude": lat,
        "longitude": lon,
        "radius": radius,
        "status": "E",       # Operational/Available stations only - there's 3 options: E=Available, P=Planned, T=Temporarily Out of Service
        "limit": "all"       # Retrieve all matching stations within radius
    }
    headers = {
        "User-Agent": "TelematicsPlatform/1.0 (University Research Fleet Project)"
    }

    print(f"Querying NLR API at {NLR_NEAREST_API_URL} (radius={radius} miles)...")
    response = requests.get(NLR_NEAREST_API_URL, params=params, headers=headers, timeout=20)
    response.raise_for_status()

    payload = response.json()
    stations = payload.get("fuel_stations", [])
    total_found = payload.get("total_results", len(stations))
    print(f"Retrieved {len(stations)} live stations.")
    return stations


def parse_station_records(stations: list[dict]) -> list[tuple]:
    """
    Extracts and validates database fields from raw API station JSON payloads.
    
    Target Schema (dim_charging_stations):
        - station_id: int
        - station_name: varchar(150)
        - latitude: numeric(9, 6)
        - longitude: numeric(9, 6)
        - l2_ports_count: int
        - dc_fast_ports_count: int
        - access_type: varchar(30)
    """
    records = []
    for s in stations:
        # Handle cases where port counts are null/None in API payload
        l2_ports = s.get("ev_level2_evse_num")
        dc_ports = s.get("ev_dc_fast_num")

        record = (
            int(s["id"]),
            str(s.get("station_name", "Unknown Station"))[:150],
            float(s["latitude"]),
            float(s["longitude"]),
            int(l2_ports) if l2_ports is not None else 0,
            int(dc_ports) if dc_ports is not None else 0,
            str(s.get("access_code", "public"))[:30]
        )
        records.append(record)

    return records


def upsert_stations_to_db(records: list[tuple]) -> None:
    """
    Inserts or updates parsed station records into dim_charging_stations idempotently
    """
    if not records:
        print("No station records to upsert.")
        return

    # Idempotent upsert logic based on primary key `station_id`
    upsert_sql = """
        INSERT INTO dim_charging_stations (
            station_id,
            station_name,
            latitude,
            longitude,
            l2_ports_count,
            dc_fast_ports_count,
            access_type
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (station_id) DO UPDATE SET
            station_name = EXCLUDED.station_name,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            l2_ports_count = EXCLUDED.l2_ports_count,
            dc_fast_ports_count = EXCLUDED.dc_fast_ports_count,
            access_type = EXCLUDED.access_type;
    """

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            execute_batch(cur, upsert_sql, records, page_size=100)
        conn.commit()
        print(f"Database updated: Successfully upserted {len(records)} stations into dim_charging_stations.")
    except Exception as e:
        conn.rollback()
        print(f"Database upsert failed: {e}")
        raise
    finally:
        conn.close()


def fetch_nrel_stations():
    try:
        stations = fetch_charging_stations(
            api_key=NREL_API_KEY,
            lat=ANN_ARBOR_LAT,
            lon=ANN_ARBOR_LON,
            radius=SEARCH_RADIUS_MILES
        )
        records = parse_station_records(stations)
        upsert_stations_to_db(records)
    except Exception as err:
        print(f"Ingestion failed: {err}")
        sys.exit(1)


if __name__ == "__main__":
    fetch_nrel_stations()