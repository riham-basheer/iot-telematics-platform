#!/usr/bin/env bash
set -euo pipefail

echo "=================================================================="
echo "      IOT Fleet Telematics Lakehouse — Automated Bootstrap "
echo "=================================================================="

# 1. Ensure .env exists (copy from .env.example if missing) & set AIRFLOW_UID
if [ ! -f .env ]; then
    if [ -f .env.example ]; then
        cp .env.example .env
        echo "[Done] Created .env from .env.example template."
    else
        touch .env
    fi
fi

if ! grep -q "^AIRFLOW_UID=" .env; then
    echo "AIRFLOW_UID=$(id -u)" >> .env
else
    sed -i "s/^AIRFLOW_UID=.*/AIRFLOW_UID=$(id -u)/" .env
fi
echo "[Done] Configured AIRFLOW_UID=$(id -u) in .env"

# 2. Start PostgreSQL & Redis services
echo "[1/6] Starting Docker Compose infrastructure..."
docker compose up -d postgres redis
echo "Waiting for PostgreSQL container (telematics-postgres) to become healthy..."
until docker exec telematics-postgres pg_isready -U airflow > /dev/null 2>&1; do
    sleep 2
done
echo "[Done] PostgreSQL is online."

# 3. Create telematics_dwh database and apply Star Schema DDL
echo "[2/6] Initializing Star Schema in PostgreSQL (telematics_dwh)..."
docker exec -i telematics-postgres psql -U airflow -d airflow -tc \
    "SELECT 1 FROM pg_database WHERE datname = 'telematics_dwh'" | grep -q 1 || \
    docker exec -i telematics-postgres psql -U airflow -d airflow -c "CREATE DATABASE telematics_dwh;"

docker exec -i telematics-postgres psql -U airflow -d telematics_dwh < db/init_schema.sql > /dev/null
docker exec -i telematics-postgres psql -U airflow -d airflow < db/init_schema.sql > /dev/null
echo "[Done] Star Schema tables and indexes verified."

# 4. Load Warehouse Snapshot (if DB is empty) OR Run Dimension Seeders
TRIP_COUNT=$(docker exec -i telematics-postgres psql -U airflow -d telematics_dwh -tc "SELECT COUNT(*) FROM fact_trips_summary;" | tr -d '[:space:]')

if [ "$TRIP_COUNT" = "0" ] && [ -f "db/telematics_dwh_snapshot.sql.gz" ]; then
    echo "[3/6] Loading pre-populated 50-week warehouse data from db/telematics_dwh_snapshot.sql.gz..."
    gunzip -c db/telematics_dwh_snapshot.sql.gz | docker exec -i telematics-postgres psql -U airflow -d telematics_dwh > /dev/null
    echo "[Done] Warehouse data loaded (trips, spatial hotspots, vehicles, tariffs, and stations)."
else
    echo "[3/6] Seeding Dimension Tables..."
    export PYTHONPATH=.
    if [ -f db/seed_dim_vehicle.py ]; then
        python3 db/seed_dim_vehicle.py
    fi
    if [ -f db/seed_pricing_history.py ]; then
        python3 db/seed_pricing_history.py || echo "[Warn] EIA API timed out — using existing dim_energy_pricing_history records."
    fi
    if [ -f db/fetch_nrel_stations.py ]; then
        python3 db/fetch_nrel_stations.py || echo "[Warn] NREL API timed out — using existing dim_charging_stations records."
    fi
    echo "[Done] Dimension tables seeded (dim_vehicle, dim_energy_pricing_history, dim_charging_stations)."
fi

# 5. Ensure Historical Weather Cache Exists
echo "[4/6] Checking Bronze Historical Weather Parquet Cache..."
WEATHER_CACHE="data/bronze/weather/historical_bulk/weather_ann_arbor_2017_2018.parquet"
if [ ! -f "$WEATHER_CACHE" ]; then
    python3 src/batch/fetch_historical_weather_bulk.py
else
    echo "[Done] Cached weather Parquet found at $WEATHER_CACHE"
fi


# 6. Audit PostgreSQL Gold Serving Layer and Populate Spatial Hotspots if Needed
echo "[5/6] Auditing PostgreSQL Gold Serving Layer..."
HOTSPOT_COUNT=$(docker exec -i telematics-postgres psql -U airflow -d telematics_dwh -tc "SELECT COUNT(*) FROM fact_spatial_hotspots;" | tr -d '[:space:]')
if [ "$HOTSPOT_COUNT" = "0" ] && [ -d "data/silver/telemetry_enriched" ]; then
    echo "Populating Gold spatial hotspots for Page 3..."
    python3 src/batch/spark_spatial_hotspots.py
fi

docker exec -i telematics-postgres psql -U airflow -d telematics_dwh -c "
SELECT 
    (SELECT COUNT(*) FROM etl_processed_batches WHERE status = 'SUCCESS') AS weeks_processed,
    (SELECT COUNT(*) FROM dim_vehicle) AS fleet_vehicles,
    (SELECT COUNT(*) FROM dim_charging_stations) AS nrel_stations,
    (SELECT COUNT(*) FROM fact_trips_summary) AS gold_trips_loaded,
    (SELECT COUNT(*) FROM fact_spatial_hotspots) AS spatial_hotspots;
"

# Start remaining Airflow services in background
docker compose up -d

# 7. Launch Streamlit Executive Analytics Dashboard
echo "[6/6] Launching Streamlit Executive Analytics Platform on http://localhost:8501 ..."
streamlit run streamlit_app/Home.py
