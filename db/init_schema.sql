-- ============================================================================
-- 1. Ingestion State & Audit Layer
-- ============================================================================
CREATE TABLE IF NOT EXISTS etl_processed_batches (
    file_name VARCHAR(120) PRIMARY KEY,
    batch_chunk_id VARCHAR(50) NOT NULL,
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    rows_ingested INT NOT NULL,
    trips_generated INT NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'SUCCESS', -- 'SUCCESS', 'FAILED'
    execution_duration_sec NUMERIC(6, 2)
);
CREATE INDEX IF NOT EXISTS idx_etl_batches_status ON etl_processed_batches(status);

-- ============================================================================
-- 2. Operational & Streaming Layer (Speed Layer Tables)
-- ============================================================================
CREATE TABLE IF NOT EXISTS current_fleet_status (
    veh_id INT PRIMARY KEY,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    speed_kmh DOUBLE PRECISION NOT NULL,
    engine_rpm DOUBLE PRECISION,
    fuel_rate_l_hr DOUBLE PRECISION,
    battery_soc_pct DOUBLE PRECISION,
    powertrain_type VARCHAR(20) NOT NULL,
    vehicle_state VARCHAR(20) NOT NULL, -- 'MOVING', 'IDLING', 'PARKED'
    last_updated_time TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE TABLE IF NOT EXISTS live_alerts (
    alert_id BIGSERIAL PRIMARY KEY,
    veh_id INT NOT NULL,
    alert_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    alert_type VARCHAR(50) NOT NULL,     -- 'OVERSPEEDING', 'LOW_BATTERY', 'EXCESSIVE_IDLE'
    severity VARCHAR(20) NOT NULL,       -- 'WARNING', 'CRITICAL'
    metric_value DOUBLE PRECISION,
    message TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_live_alerts_ts ON live_alerts(alert_timestamp DESC);

-- ============================================================================
-- 3. Dimension Tables (Annual Temporal Coverage)
-- ============================================================================
CREATE TABLE IF NOT EXISTS dim_vehicle (
    veh_id INT PRIMARY KEY,
    vehicle_class VARCHAR(50),
    engine_type VARCHAR(20) NOT NULL,   -- 'ICE', 'HEV', 'PHEV', 'EV'
    engine_displacement_l NUMERIC(4,2),
    curb_weight_kg NUMERIC(8,2)
);

CREATE TABLE IF NOT EXISTS dim_energy_pricing_history (
    price_id SERIAL PRIMARY KEY,
    energy_type VARCHAR(30) NOT NULL,   -- 'Gasoline_Regular', 'Electricity_Commercial'
    price_per_unit NUMERIC(8, 4) NOT NULL,
    co2_kg_per_unit NUMERIC(8, 4) NOT NULL,
    valid_from TIMESTAMP WITH TIME ZONE NOT NULL,
    valid_to TIMESTAMP WITH TIME ZONE,  -- NULL denotes current active tariff
    is_current BOOLEAN DEFAULT TRUE
);
CREATE INDEX IF NOT EXISTS idx_pricing_scd ON dim_energy_pricing_history(energy_type, valid_from, valid_to);

CREATE TABLE IF NOT EXISTS dim_charging_stations (
    station_id INT PRIMARY KEY,
    station_name VARCHAR(150),
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    l2_ports_count INT DEFAULT 0,
    dc_fast_ports_count INT DEFAULT 0,
    access_type VARCHAR(30)
);

-- ============================================================================
-- 4. Fact Tables (Gold Layer Serving: Full 51 Weeks)
-- ============================================================================
CREATE TABLE IF NOT EXISTS fact_trips_summary (
    trip_id BIGINT PRIMARY KEY,
    veh_id INT NOT NULL REFERENCES dim_vehicle(veh_id),
    engine_type VARCHAR(20) NOT NULL,
    season_category VARCHAR(25) NOT NULL, -- 'FALL_2017', 'WINTER_2018', 'SPRING_2018', 'SUMMER_LIVE'
    trip_start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    trip_end_time TIMESTAMP WITH TIME ZONE NOT NULL,
    duration_minutes NUMERIC(8, 2) NOT NULL,
    distance_km NUMERIC(8, 2) NOT NULL,
    avg_speed_kmh NUMERIC(6, 2) NOT NULL,
    idling_duration_minutes NUMERIC(8, 2) NOT NULL,
    fuel_consumed_liters NUMERIC(8, 3) DEFAULT 0,
    energy_consumed_kwh NUMERIC(8, 3) DEFAULT 0,
    total_cost_usd NUMERIC(8, 2) NOT NULL,
    co2_emissions_kg NUMERIC(8, 2) NOT NULL,
    co2_avoided_kg NUMERIC(8, 2) DEFAULT 0,
    avg_ambient_temp_c NUMERIC(5, 2),
    avg_hvac_power_watts NUMERIC(8, 2)
);
CREATE INDEX IF NOT EXISTS idx_fact_trips_start ON fact_trips_summary(trip_start_time);
CREATE INDEX IF NOT EXISTS idx_fact_trips_engine ON fact_trips_summary(engine_type);
CREATE INDEX IF NOT EXISTS idx_fact_trips_season ON fact_trips_summary(season_category);

CREATE TABLE IF NOT EXISTS fact_spatial_hotspots (
    hotspot_id SERIAL PRIMARY KEY,
    cluster_latitude DOUBLE PRECISION NOT NULL,
    cluster_longitude DOUBLE PRECISION NOT NULL,
    season_category VARCHAR(25) DEFAULT 'ALL_TIME',
    total_idling_events INT NOT NULL,
    total_idling_hours NUMERIC(8, 2) NOT NULL,
    wasted_fuel_liters NUMERIC(8, 2) NOT NULL,
    wasted_cost_usd NUMERIC(8, 2) NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_hotspots_coords ON fact_spatial_hotspots(cluster_latitude, cluster_longitude);
CREATE INDEX IF NOT EXISTS idx_hotspots_season ON fact_spatial_hotspots(season_category);