import os
import sys
import glob
import pytest
import pandas as pd
from pathlib import Path

# Add project root to sys.path to enable src module imports
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.db_connection import get_db_connection


@pytest.fixture(scope="module")
def pg_conn():
    """Provides a PostgreSQL connection and tears it down after tests finish."""
    conn = get_db_connection()
    yield conn
    conn.close()


# ==============================================================================
# 1. Pipeline Watermark & Batch Lineage Audits
# ==============================================================================

def test_watermark_batches_processed(pg_conn):
    """
    Asserts that the 50-week historical baseline was successfully processed
    and recorded into the etl_processed_batches watermark table.
    """
    with pg_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM etl_processed_batches WHERE status = 'SUCCESS';")
        weeks_processed = cur.fetchone()[0]

    assert weeks_processed >= 50, (
        f"Expected at least 50 successfully processed weeks in watermark, found {weeks_processed}"
    )


def test_watermark_no_failed_batches(pg_conn):
    """Asserts that there are no stalled or failed batches in the audit ledger."""
    with pg_conn.cursor() as cur:
        cur.execute("SELECT file_name FROM etl_processed_batches WHERE status != 'SUCCESS';")
        failed_batches = cur.fetchall()

    assert len(failed_batches) == 0, f"Detected failed batches in watermark: {failed_batches}"


# ==============================================================================
# 2. Fact Table Integrity & Idempotency Audits (fact_trips_summary)
# ==============================================================================

def test_primary_key_uniqueness(pg_conn):
    """Asserts that trip_id is strictly unique and contains zero duplicates."""
    with pg_conn.cursor() as cur:
        cur.execute("""
            SELECT 
                COUNT(*) AS total_rows,
                COUNT(DISTINCT trip_id) AS distinct_trips
            FROM fact_trips_summary;
        """)
        total_rows, distinct_trips = cur.fetchone()

    assert total_rows > 0, "fact_trips_summary table is empty!"
    assert total_rows == distinct_trips, (
        f"Duplicate trip_ids detected! Total rows: {total_rows}, Distinct: {distinct_trips}"
    )


def test_no_null_critical_keys(pg_conn):
    """Asserts that primary keys and vehicle identifiers are non-null."""
    with pg_conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) 
            FROM fact_trips_summary 
            WHERE trip_id IS NULL 
               OR veh_id IS NULL 
               OR trip_start_time IS NULL 
               OR trip_end_time IS NULL;
        """)
        null_count = cur.fetchone()[0]

    assert null_count == 0, f"Found {null_count} records with NULL critical keys."


# ==============================================================================
# 3. Kinematic, Physical & ESG Mathematical Invariants
# ==============================================================================

def test_kinematic_and_duration_invariants(pg_conn):
    """
    Asserts physical reality constraints:
    - Trip duration must be strictly positive (> 0 mins).
    - Distance must be non-negative (>= 0 km).
    - Idling duration must be non-negative and <= total trip duration.
    """
    with pg_conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) 
            FROM fact_trips_summary 
            WHERE duration_minutes <= 0.0
               OR distance_km < 0.0
               OR idling_duration_minutes < 0.0
               OR idling_duration_minutes > (duration_minutes + 0.1);
        """)
        invalid_trips = cur.fetchone()[0]

    assert invalid_trips == 0, f"Detected {invalid_trips} trips violating kinematic/time invariants."


def test_financial_and_esg_invariants(pg_conn):
    """
    Asserts economic and environmental bounds:
    - Fuel consumption >= 0.
    - Total cost (USD) >= 0.
    - CO2 emissions (kg) >= 0.
    - Avoided CO2 emissions (kg) >= 0.
    - Net negative energy_consumed_kwh is only permitted on HEV/PHEV/EV (regenerative braking).
    """
    with pg_conn.cursor() as cur:
        # Check that financial and emission metrics are non-negative
        cur.execute("""
            SELECT COUNT(*) 
            FROM fact_trips_summary 
            WHERE fuel_consumed_liters < 0.0
               OR total_cost_usd < 0.0
               OR co2_emissions_kg < 0.0
               OR co2_avoided_kg < 0.0;
        """)
        violating_rows = cur.fetchone()[0]

        # Check that ICE vehicles never have negative energy consumption
        cur.execute("""
            SELECT COUNT(*) 
            FROM fact_trips_summary 
            WHERE engine_type = 'ICE' AND energy_consumed_kwh < 0.0;
        """)
        invalid_ice_energy = cur.fetchone()[0]

    assert violating_rows == 0, f"Detected {violating_rows} trips with negative cost or emission metrics."
    assert invalid_ice_energy == 0, f"Detected {invalid_ice_energy} ICE trips with negative energy consumption."

def test_ice_powertrain_esg_boundary(pg_conn):
    """
    Combustion vehicles (ICE) do not generate avoided carbon displacement credits
    against the ICE baseline (co2_avoided_kg must equal 0.0).
    """
    with pg_conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) 
            FROM fact_trips_summary 
            WHERE engine_type = 'ICE' AND co2_avoided_kg > 0.0;
        """)
        invalid_ice_credits = cur.fetchone()[0]

    assert invalid_ice_credits == 0, (
        f"Found {invalid_ice_credits} ICE trips claiming non-zero avoided CO2 credits!"
    )


# ==============================================================================
# 4. Dimensional Integrity & Star Schema Consistency
# ==============================================================================

def test_referential_integrity_dim_vehicle(pg_conn):
    """Asserts that all trips link to an authoritative registered vehicle in dim_vehicle."""
    with pg_conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*)
            FROM fact_trips_summary f
            LEFT JOIN dim_vehicle v ON f.veh_id = v.veh_id
            WHERE v.veh_id IS NULL;
        """)
        unmatched_vehicles = cur.fetchone()[0]

    assert unmatched_vehicles == 0, f"Found {unmatched_vehicles} trips with unmapped veh_id values."


def test_powertrain_taxonomy_distribution(pg_conn):
    """Asserts that all fleet powertrain archetypes are populated."""
    with pg_conn.cursor() as cur:
        cur.execute("SELECT DISTINCT engine_type FROM fact_trips_summary;")
        powertrains = {row[0] for row in cur.fetchall()}

    for expected in ["ICE", "HEV", "PHEV", "EV"]:
        assert expected in powertrains, f"Missing expected powertrain type in Gold layer: {expected}"


def test_scd2_tariff_matching(pg_conn):
    """Asserts that trips with energy/fuel usage were successfully joined to SCD2 pricing tariffs."""
    with pg_conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) 
            FROM fact_trips_summary 
            WHERE (fuel_consumed_liters > 0.0 OR energy_consumed_kwh > 0.0)
              AND total_cost_usd IS NULL;
        """)
        unmatched_costs = cur.fetchone()[0]

    assert unmatched_costs == 0, f"Found {unmatched_costs} active trips unpriced by SCD2 pricing tariffs."


# ==============================================================================
# 5. Lakehouse Parquet Storage Audit
# ==============================================================================

def test_gold_parquet_storage_exists():
    """Asserts that physical columnar Parquet partitions exist on disk."""
    gold_dir = os.path.join(str(PROJECT_ROOT), "data/gold/fact_trips_summary")
    parquet_files = glob.glob(os.path.join(gold_dir, "**", "*.parquet"), recursive=True)

    assert os.path.exists(gold_dir), f"Directory {gold_dir} does not exist!"
    assert len(parquet_files) > 0, f"No Parquet partition files found in {gold_dir}!"


# ==============================================================================
# 6. Spatial Infrastructure & Page 3 Serving Audits
# ==============================================================================

def test_spatial_hotspots_and_chargers_integrity(pg_conn):
    """
    Asserts Page 3 serving tables (fact_spatial_hotspots & dim_charging_stations)
    are populated and obey Ann Arbor GPS bounds and non-negative metric invariants.
    """
    with pg_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM fact_spatial_hotspots;")
        hotspot_count = cur.fetchone()[0]

        cur.execute("""
            SELECT COUNT(*)
            FROM fact_spatial_hotspots
            WHERE cluster_latitude < 42.0 OR cluster_latitude > 42.5
               OR cluster_longitude < -84.0 OR cluster_longitude > -83.5
               OR total_idling_events <= 0
               OR total_idling_hours <= 0.0
               OR wasted_fuel_liters < 0.0
               OR wasted_cost_usd < 0.0;
        """)
        invalid_hotspots = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM dim_charging_stations;")
        station_count = cur.fetchone()[0]

    assert hotspot_count > 0, "fact_spatial_hotspots table is empty!"
    assert invalid_hotspots == 0, f"Found {invalid_hotspots} spatial hotspots violating GPS or metric bounds."
    assert station_count > 0, "dim_charging_stations table is empty!"


def test_spatial_gold_parquet_exists():
    """Asserts that Gold Parquet files exist on disk for fact_spatial_hotspots."""
    spatial_dir = os.path.join(str(PROJECT_ROOT), "data/gold/fact_spatial_hotspots")
    parquet_files = glob.glob(os.path.join(spatial_dir, "**", "*.parquet"), recursive=True)

    assert os.path.exists(spatial_dir), f"Directory {spatial_dir} does not exist!"
    assert len(parquet_files) > 0, f"No Parquet files found in {spatial_dir}!"