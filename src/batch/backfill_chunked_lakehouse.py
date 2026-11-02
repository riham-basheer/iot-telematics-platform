import os
import glob
import subprocess
import psycopg2
from dotenv import find_dotenv, load_dotenv
import argparse

load_dotenv(find_dotenv())

RAW_DIR = "data/raw"
# Sort all CSV files chronologically and slice the first 50 files for the historical backfill
# This reserves the final files (e.g., Week 51/52) for the live streaming cutover simulation
ALL_FILES = sorted(glob.glob(os.path.join(RAW_DIR, "VED_*_week.csv")))[:50]

def get_pg_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        dbname=os.getenv("DB_NAME", "telematics_dwh"),
        user=os.getenv("DB_USER", "airflow"),
        password=os.getenv("DB_PASSWORD", "airflow"),
        port=os.getenv("DB_PORT", "5432")
    )


def get_processed_files():
    """Retrieve the list of successfully processed CSVs from PostgreSQL watermark."""
    with get_pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT file_name FROM etl_processed_batches WHERE status = 'SUCCESS';")
            return {row[0] for row in cur.fetchall()}


def get_total_gold_trip_count():
    """Query the Gold table to get the total number of trips currently materialized."""
    with get_pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM fact_trips_summary;")
            return cur.fetchone()[0]

def mark_file_processed(filename, rows_ingested, trips_generated):
    """Log a successful batch completion to prevent reprocessing"""
    with get_pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO etl_processed_batches (file_name, batch_chunk_id, rows_ingested, trips_generated, status)
                VALUES (%s, 'BACKFILL', 0, 0, 'SUCCESS')
                ON CONFLICT (file_name) DO UPDATE SET
                    rows_ingested = EXCLUDED.rows_ingested,
                    treips_generated = EXCLUDED.trips_generated,
                    status = 'SUCCESS', processed_at = CURRENT_TIMESTAMP;
            """, (filename, rows_ingested, trips_generated))



def execute_pipeline(file_path):
    """Executes the Medallion pipeline (Bronze->Silver->Gold->PG) on a single file"""
    env = os.environ.copy()
    env["PYTHONPATH"] = "."

    #1. get baseline trip count before processing
    baseline_trip_count = get_total_gold_trip_count()

    #2. Calculate raw rows ingested from the CSV file (excluding headers)
    with open(file_path, 'r') as f:
        rows_ingested = sum(1 for line in f) - 1  # Subtract 1 for the header row

    print(f"\n[RUNNING BRONZE -> SILVER] {file_path}")
    subprocess.run(["python3", "src/batch/spark_bronze_to_silver.py", "--input-path", file_path], env=env, check=True)
    
    print(f"[RUNNING SILVER -> GOLD & UPSERT] {file_path}")
    subprocess.run(["python3", "src/batch/spark_silver_to_gold.py", "--input-path", "data/silver/telemetry_enriched"], env=env, check=True)

    #3. query gold layer after processing to get the new trip count
    new_trip_count = get_total_gold_trip_count()
    trips_generated = new_trip_count - baseline_trip_count

    return rows_ingested, trips_generated

def run_chunked_backfill(chunk_size=3):
    processed = get_processed_files()
    pending = [f for f in ALL_FILES if os.path.basename(f) not in processed]
    
    if not pending:
        print("Lakehouse is fully up to date! No pending weeks to process.)")
        return

    chunk = pending[:chunk_size]
    print(f"\n-- Initiating backfill for {len(chunk)} files (Total remaining: {len(pending)})...\n")
    
    for file_path in chunk:
        filename = os.path.basename(file_path)
        try:
            rows_ingested, trips_generated = execute_pipeline(file_path)
            mark_file_processed(filename, rows_ingested, trips_generated)
            print(f"--- SUCCESSFULLY INGESTED AND UPSERTED: {filename} ({rows_ingested} raw rows -> {trips_generated} gold trips)")
        except subprocess.CalledProcessError as e:
            print(f"--- PIPELINE FAILED ON: {filename}. Stopping chunk run to preserve state.")
            break

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-size", type=int, default=3, help="Number of CSV files to process in this run")
    args = parser.parse_args()
    
    run_chunked_backfill(chunk_size=args.batch_size)
