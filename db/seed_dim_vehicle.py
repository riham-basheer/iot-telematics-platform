import os
import sys
import re
import glob
from pathlib import Path
import pandas as pd

# Resolve root directory for imports
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.utils.db_connection import get_db_cursor

LBS_TO_KG = 0.453592
DEFAULT_WEIGHT_LBS = 3500.0


def parse_weight_kg(val):
    if pd.isna(val) or str(val).strip().upper() in ["NO DATA", "NONE", "NULL", ""]:
        return round(DEFAULT_WEIGHT_LBS * LBS_TO_KG, 2)
    try:
        cleaned = re.sub(r"[^\d.]", "", str(val))
        return round(float(cleaned) * LBS_TO_KG, 2)
    except Exception:
        return round(DEFAULT_WEIGHT_LBS * LBS_TO_KG, 2)


def parse_displacement(val):
    if pd.isna(val) or str(val).strip().upper() in ["NO DATA", "NONE", "NULL", "ELECTRIC", ""]:
        return None
    try:
        match = re.search(r"(\d+(\.\d+)?)\s*L", str(val).strip().upper())
        return float(match.group(1)) if match else None
    except Exception:
        return None


def clean_str(val, default="Unknown"):
    if pd.isna(val):
        return default
    s = str(val).strip()
    return default if s.upper() in ["NO DATA", "NONE", "NULL", ""] else s


def seed_vehicles():
    ice_files = glob.glob("**/VED_Static_Data_ICE&HEV*", recursive=True)
    ev_files = glob.glob("**/VED_Static_Data_PHEV&EV*", recursive=True)

    if not ice_files or not ev_files:
        print("ERROR: Static metadata Excel files not found under data/raw/")
        return

    df_ice = pd.read_excel(ice_files[0]) if ice_files[0].endswith((".xls", ".xlsx")) else pd.read_csv(ice_files[0])
    df_ev = pd.read_excel(ev_files[0]) if ev_files[0].endswith((".xls", ".xlsx")) else pd.read_csv(ev_files[0])

    vehicles = []
    for _, r in df_ice.iterrows():
        try:
            vehicles.append((
                int(r["VehId"]),
                clean_str(r.get("Vehicle Class"), "Passenger Car"),
                clean_str(r.get("Vehicle Type"), "ICE"),
                parse_displacement(r.get("Engine Configuration & Displacement")),
                parse_weight_kg(r.get("Generalized_Weight"))
            ))
        except (ValueError, TypeError):
            continue

    for _, r in df_ev.iterrows():
        try:
            vehicles.append((
                int(r["VehId"]),
                clean_str(r.get("Vehicle Class"), "Passenger Car"),
                clean_str(r.get("EngineType", r.get("Vehicle Type")), "EV"),
                parse_displacement(r.get("Engine Configuration & Displacement")),
                parse_weight_kg(r.get("Generalized_Weight"))
            ))
        except (ValueError, TypeError):
            continue

    with get_db_cursor(commit=True) as cur:
        cur.execute("TRUNCATE TABLE dim_vehicle CASCADE;")
        insert_sql = """
            INSERT INTO dim_vehicle (veh_id, vehicle_class, engine_type, engine_displacement_l, curb_weight_kg)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (veh_id) DO UPDATE SET 
                vehicle_class = EXCLUDED.vehicle_class,
                engine_type = EXCLUDED.engine_type,
                engine_displacement_l = EXCLUDED.engine_displacement_l,
                curb_weight_kg = EXCLUDED.curb_weight_kg;
        """
        cur.executemany(insert_sql, vehicles)

    print(f"-- Successfully populated {len(vehicles)} vehicles into dim_vehicle via db_connection.")


if __name__ == "__main__":
    seed_vehicles()