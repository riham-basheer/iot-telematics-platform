import os
import sys
from pathlib import Path
import requests

# Resolve root directory for imports
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.utils.db_connection import get_db_cursor

EIA_API_KEY = os.getenv("EIA_API_KEY", "").strip()
BASE_URL = "https://api.eia.gov/v2/petroleum/pri/gnd"

LITERS_PER_GALLON = 3.78541
GASOLINE_CO2_FACTOR = 2.3100     # EPA benchmark: 2.31 kg CO2 / Liter
ELECTRICITY_PRICE = 0.1120       # Commercial grid benchmark: $0.112 / kWh
ELECTRICITY_CO2_FACTOR = 0.5400  # Michigan grid factor: 0.54 kg CO2 / kWh


def discover_eia_facet_ids():
    duo_url = f"{BASE_URL}/facet/duoarea"
    res = requests.get(duo_url, params={"api_key": EIA_API_KEY}, timeout=15)
    res.raise_for_status()
    areas = res.json().get("response", {}).get("facets", [])

    selected_area = next((a for a in areas if "michigan" in a.get("name", "").lower()), None)
    if not selected_area:
        selected_area = next((a for a in areas if "padd 2" in a.get("name", "").lower() or "midwest" in a.get("name", "").lower()), areas[0])

    prod_url = f"{BASE_URL}/facet/product"
    res = requests.get(prod_url, params={"api_key": EIA_API_KEY}, timeout=15)
    res.raise_for_status()
    products = res.json().get("response", {}).get("facets", [])
    selected_product = next((p for p in products if "regular" in p.get("name", "").lower() and "gasoline" in p.get("name", "").lower()), products[0])

    return selected_area["id"], selected_product["id"]


def fetch_real_gas_prices(area_id, product_id):
    params = {
        "api_key": EIA_API_KEY,
        "frequency": "weekly",
        "data[0]": "value",
        "facets[duoarea][]": area_id,
        "facets[product][]": product_id,
        "start": "2017-11-01",
        "end": "2018-06-30",
        "sort[0][column]": "period",
        "sort[0][direction]": "asc",
        "offset": 0,
        "length": 100
    }
    res = requests.get(f"{BASE_URL}/data/", params=params, timeout=15)
    res.raise_for_status()
    return res.json().get("response", {}).get("data", [])


def build_scd2_records(records):
    scd2_entries = []
    for i, row in enumerate(records):
        price_gal = float(row["value"])
        price_liter = round(price_gal / LITERS_PER_GALLON, 4)
        valid_from = "2017-11-01 00:00:00+00" if i == 0 else f"{row['period']} 00:00:00+00"
        valid_to = f"{records[i + 1]['period']} 00:00:00+00" if i + 1 < len(records) else None
        
        scd2_entries.append((
            "Gasoline_Regular",
            price_liter,
            GASOLINE_CO2_FACTOR,
            valid_from,
            valid_to,
            valid_to is None
        ))

    # Append commercial electricity benchmark
    scd2_entries.append((
        "Electricity_Commercial",
        ELECTRICITY_PRICE,
        ELECTRICITY_CO2_FACTOR,
        "2017-11-01 00:00:00+00",
        None,
        True
    ))
    return scd2_entries


def seed_pricing_table():
    if not EIA_API_KEY:
        print("ERROR: EIA_API_KEY is not defined in .env.")
        sys.exit(1)

    area_id, product_id = discover_eia_facet_ids()
    records = fetch_real_gas_prices(area_id, product_id)
    scd2_data = build_scd2_records(records)

    with get_db_cursor(commit=True) as cur:
        cur.execute("TRUNCATE TABLE dim_energy_pricing_history RESTART IDENTITY;")
        insert_sql = """
            INSERT INTO dim_energy_pricing_history 
            (energy_type, price_per_unit, co2_kg_per_unit, valid_from, valid_to, is_current)
            VALUES (%s, %s, %s, %s, %s, %s);
        """
        cur.executemany(insert_sql, scd2_data)

    print(f"-- Successfully populated {len(scd2_data)} records into dim_energy_pricing_history via db_connection.")


if __name__ == "__main__":
    seed_pricing_table()