# IoT Fleet Telematics Lakehouse

[![Live Demo](https://img.shields.io/badge/Live_Demo-Streamlit-FF4B4B?style=flat-square&logo=streamlit&logoColor=white)](https://iot-telematics-platform.streamlit.app/)
[![Release](https://img.shields.io/badge/Release-v1.0.0_Live-10B981?style=flat-square)](#release-roadmap)
[![PySpark](https://img.shields.io/badge/PySpark-Medallion-E25A1C?style=flat-square&logo=apachespark&logoColor=white)](src/batch)
[![Airflow](https://img.shields.io/badge/Airflow-2.11-017CEE?style=flat-square&logo=apacheairflow&logoColor=white)](dags)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Star_Schema-4169E1?style=flat-square&logo=postgresql&logoColor=white)](db/init_schema.sql)
[![Kafka](https://img.shields.io/badge/Kafka-Phase_2_WIP-231F20?style=flat-square&logo=apachekafka&logoColor=white)](#release-roadmap)
[![Tests](https://img.shields.io/badge/PyTest-18%2F18_Passing-22C55E?style=flat-square&logo=pytest&logoColor=white)](tests)

An end-to-end **IoT Big Data Lakehouse and Hybrid Lambda Platform** processing **26M+ 1 Hz CAN-bus/OBD-II and GPS sensor pings (~2.8 GB)** from the University of Michigan **Vehicular Energy Dataset (VED)** across **384 vehicles** in Ann Arbor, MI. The pipeline fuses raw vehicular IoT telemetry with hourly airport weather (**Open-Meteo**), weekly utility tariffs (**U.S. EIA**), and public EV charger registries (**NREL**) to deliver audited TCO, idling waste, winter HVAC battery sensitivity, and CapEx siting analytics.

**[Launch Live Streamlit Dashboard](https://iot-telematics-platform.streamlit.app/)**  
<sub>*Hosted on Streamlit Cloud free tier. If hibernating after 24-48h of inactivity, click **"Yes, get this app back up!"** and wait ~30 seconds for the embedded SQLite serving cache to wake.*</sub>

## Demo Walkthrough

[PASTE_YOUR_DEMO_VIDEO_URL_HERE](https://github.com/user-attachments/assets/c882405a-9010-44ef-8bd9-14e048623379)

## Production Metrics (50-Week Baseline)

| Metric | Production Output | Technical Scope |
| :--- | :--- | :--- |
| **IoT Telemetry Ingested** | **26M+ rows (1 Hz)** | 50 weekly CSV files (~2.8 GB raw) compressed to partitioned Parquet |
| **Fleet Inventory** | **384 Vehicles** | 264 ICE, 93 HEV, 24 PHEV, 3 EV (`dim_vehicle`) |
| **Reconstructed Trips** | **31,271 Trips** | 161,496 km across 364 active days (`fact_trips_summary`) |
| **Audited Energy Spend** | **$7,215.28 USD** | 10,195.7 L fuel + 2,397.1 kWh electricity via SCD Type 2 tariffs |
| **ESG Impact** | **5.20 MT CO2 Avoided** | Scope 1 + Scope 2 emissions vs. ICE baseline (0.19635 kg/km) |
| **Spatial Planning** | **3,336 Hotspots** | ~110m x 82m idling cells cross-referenced with 200 NREL stations |
| **Automated QA Gates** | **18 / 18 Passed** | 5 PySpark physics unit tests + 13 E2E PostgreSQL audit assertions |

## Architecture (Hybrid Batch + Streaming)

Designed with **Storage Interface Decoupling** so the historical Batch Medallion Lakehouse (`v1.0.0`) and the real-time Kafka Speed Layer (`v2.0.0`) converge on the same Bronze Parquet contract and PostgreSQL Serving Warehouse.

```text
DATA SOURCES: VED 1 Hz IoT Telemetry (51 Weeks) | Open-Meteo (KARB) | U.S. EIA | NREL AFDC
                         |                                        |
         [PHASE 1: LIVE (v1.0.0)]                  [PHASE 2: IN PROGRESS (v2.0.0)]
                         v                                        v
+-----------------------------------------+    +-------------------------------------+
| PYSPARK MEDALLION LAKEHOUSE (Batch)     |    | LAMBDA SPEED LAYER (Kafka + Avro)   |
|                                         |    |                                     |
| [BRONZE] Hive-Partitioned Parquet Cache |<---+ Micro-Batch Parquet Sink Worker     |
|    |  spark_bronze_to_silver.py         |    |    ^                                |
|    v                                    |    | Kafka + Confluent Schema Registry   |
| [SILVER] Sub-sec UTC Timestamps,        |    | (telemetry_event & pricing .avsc)   |
|          GPS Bounding Box, F.nanvl(),   |    |    |                                |
|          Offline Weather Broadcast Join |    |    v                                |
|    |  spark_silver_to_gold.py           |    | Stateless Safety Rule Engine        |
|    |  spark_spatial_hotspots.py         |    | (Overspeed >100 km/h, SOC <20%,     |
|    v                                    |    |  Excessive Idle >5 min)             |
| [GOLD]   Trapezoidal Riemann Integrals, |    +------------------+------------------+
|          MAF Fallback, SCD-2 Costing,   |                       |
|          ~110m Spatial Idling Hotspots  |                       |
+--------------------+--------------------+                       |
                     |   Airflow 2.11 DAGs + 18 PyTest Gates      |
                     v                                            v
+------------------------------------------------------------------------------------+
| SERVING LAYER: PostgreSQL 8-Table Hybrid Fact Constellation + SQLite Cloud Cache   |
|  • Analytical Facts: fact_trips_summary (31,271) | fact_spatial_hotspots (3,336)   |
|  • Dimensions:       dim_vehicle (384) | dim_energy_pricing_history (35 SCD-2)     |
|                      dim_charging_stations (200 NREL Stations)                     |
|  • Speed & Audit:    current_fleet_status (1:1 Upsert) | live_alerts               |
|                      etl_processed_batches (Watermark Control Plane)               |
+------------------------------------------------------------------------------------+
                                         |
                                         v
+------------------------------------------------------------------------------------+
| STREAMLIT DECISION PORTAL                                                          |
|  • Page 1 [v2.0.0 WIP]: Live Operations (2s Pydeck Tracking & Real-Time Alerts)    |
|  • Page 2 [v1.0.0 Live]: Executive Analytics (TCO $/100km, Idling & Winter HVAC)   |
|  • Page 3 [v1.0.0 Live]: Spatial Infrastructure (Pydeck Heatmap & Haversine CapEx) |
+------------------------------------------------------------------------------------+
```

## Key Engineering Decisions

* **Physics-Grade Signal Integration (`spark_silver_to_gold.py`):** Computes 1 Hz trapezoidal Riemann sums for distance, fuel, HV traction energy (`P = -1.0 * V * I`), and cabin HVAC load with a **30s blackout guard** to clamp tunnel/sensor dropouts. Recovers missing fuel PIDs via Mass Air Flow stoichiometry (`AFR = 14.7`, density `745.0 g/L`).
* **Data Quality & Zero-Shuffle Joins (`spark_bronze_to_silver.py`):** Deduplicates on the integer natural key `(vehid, trip, timestamp_ms)` to prevent floating-point jitter, isolates `IEEE 754 NaN` vs. `SQL NULL` via `F.nanvl()` with powertrain hardware masks, and eliminates network shuffles across 26M+ rows using `F.broadcast(weather_df)`.
* **Point-in-Time Financial Accuracy (SCD Type 2):** Evaluates `valid_from <= trip_start_time < valid_to` against 35 weekly EIA tariff intervals in `dim_energy_pricing_history` to eliminate financial lookahead bias.
* **4-Layer Idempotent Backfill (`backfill_chunked_lakehouse.py`):** Combines dynamic Parquet partition overwrites (`year/month/batch_week`), partition-pruned Gold reads, deterministic 64-bit `trip_id` PostgreSQL `UPSERT`s, and a watermark ledger (`etl_processed_batches`) running each week in an isolated subprocess to prevent JVM OOM leaks.
* **Geospatial CapEx Planner (`3_Spatial_Infrastructure.py`):** Aggregates stationary idling pings (`speed == 0` with active engine) into ~110m grid cells and applies geodesic **Haversine distance** against 200 NREL stations to flag **Charging Deserts (> 2.0 km)** for priority 50 kW DC Fast Charger deployment.

## Release Roadmap

### Release 1 (`v1.0.0`) — Live on `main`
- [x] 50-week PySpark Medallion Lakehouse (`Bronze` -> `Silver` -> `Gold`) and resumable backfill engine
- [x] 8-table PostgreSQL Hybrid Fact Constellation + `1.2 MB` snapshot & `1.5 MB` SQLite cloud fallback
- [x] Memory-tuned Docker Compose stack, custom Airflow 2.11/PySpark image, and 2 production DAGs
- [x] 18/18 passing PyTest unit physics and E2E warehouse quality gates
- [x] Streamlit Portal: Home, Executive TCO/ESG/HVAC Analytics (Page 2), Spatial CapEx Planner (Page 3)

### Release 2 (`v2.0.0`) — In Progress on `develop`
- [ ] Confluent Kafka + Schema Registry containers and Avro contracts (`telemetry_event.avsc`, `pricing_event.avsc`)
- [ ] 1 Hz Week 51 replay producer and live EIA pricing revision emitter (`telemetry_producer.py`)
- [ ] Stateless safety rule engine upserting `current_fleet_status` and `live_alerts` (`stream_consumer.py`)
- [ ] Micro-batch Bronze Parquet sink worker (`storage_sink_worker.py`) and live Page 1 (`1_Live_Operations.py`)

## Repository Structure

```text
├── bootstrap_pipeline.sh                  # One-command setup, DB snapshot restore & launch
├── Dockerfile                             # Custom Airflow 2.11 + OpenJDK + PySpark image
├── docker-compose.yml                     # Memory-tuned PostgreSQL, Redis & Airflow stack
├── db/
│   ├── init_schema.sql                    # 8-table Hybrid Fact Constellation & indexes DDL
│   ├── telematics_dwh_snapshot.sql.gz     # Pre-computed 50-week Postgres snapshot (~1.2 MB)
│   ├── telematics_serving.sqlite          # Embedded serving cache for Streamlit Cloud
│   ├── seed_dim_vehicle.py                # VED static vehicle metadata parser
│   ├── seed_pricing_history.py            # U.S. EIA weekly Michigan SCD-2 tariff seeder
│   └── fetch_nrel_stations.py             # Live NREL AFDC Ann Arbor EV stations extractor
├── src/
│   ├── utils/db_connection.py             # Pooled SQLAlchemy & psycopg2 context manager
│   ├── batch/
│   │   ├── fetch_historical_weather_bulk.py
│   │   ├── spark_bronze_to_silver.py
│   │   ├── spark_silver_to_gold.py
│   │   ├── spark_spatial_hotspots.py
│   │   └── backfill_chunked_lakehouse.py
│   └── streaming/                         # [Phase 2 - v2.0.0] Kafka/Avro speed layer
├── dags/
│   ├── dag_external_ingestion_hourly.py   # Hourly Open-Meteo & NREL sync DAG
│   └── dag_telematics_lakehouse_batch.py  # FileSensor -> Spark Medallion -> PyTest Gate
├── streamlit_app/
│   ├── Home.py
│   └── pages/
│       ├── 2_Executive_Analytics.py
│       └── 3_Spatial_Infrastructure.py
└── tests/
    ├── test_gold_aggregations.py          # 5 PySpark unit physics & invariant tests
    └── test_data_quality_audit.py         # 13 E2E PostgreSQL & Lakehouse audit gates
```

## Quickstart

### Option A: Automated One-Command Bootstrap (Recommended)
Restores the 50-week PostgreSQL warehouse snapshot (`31,271` trips and `3,336` hotspots), verifies services, and launches the stack:
```bash
git clone [https://github.com/riham-basheer/iot-telematics-platform.git](https://github.com/riham-basheer/iot-telematics-platform.git)
cd iot-telematics-platform
bash bootstrap_pipeline.sh
```

### Option B: Run Quality Gates & Dashboard Manually
```bash
docker compose up -d --build
pytest tests/test_gold_aggregations.py tests/test_data_quality_audit.py -v -W ignore
streamlit run streamlit_app/Home.py
```

* **Streamlit Portal:** `http://localhost:8501`
* **Airflow UI:** `http://localhost:8080` (`airflow` / `airflow`)