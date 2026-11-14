import os
from datetime import datetime, timedelta
from airflow import DAG  # type: ignore
from airflow.sensors.filesystem import FileSensor  # type: ignore
from airflow.operators.bash import BashOperator  # type: ignore

PROJECT_ROOT = "/opt/airflow" if os.path.exists("/opt/airflow/src") else os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
WEEK_51_CSV = os.path.join(PROJECT_ROOT, "data/raw/VED_181024_week.csv")
WEEK_51_SILVER_PARTITION = os.path.join(PROJECT_ROOT, "data/silver/telemetry_enriched/*/*/batch_week=VED_181024_week")

default_args = {
    "owner": "riham-elsayed",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Implements the core Lakehouse Medallion Architecture. 
# It monitors for incoming raw telemetry, cleans sensor noise, 
# calculates vehicle physics (energy, fuel, cost), and writes 
# the outputs to the ggold serving layer in PostgreSQL and Parquet storage.
with DAG(
    dag_id="dag_telematics_lakehouse_batch",
    default_args=default_args,
    description="Incremental Medallion Batch Pipeline for Week 51 (2018-10-24)",
    schedule="0 2 * * *",
    start_date=datetime(2018, 10, 24),
    catchup=False,
    tags=["telematics", "spark", "medallion", "batch-pipeline"],
) as dag:
    # Why this? Prevents PySpark from spinning up on empty inputs. 
    # If the file or streaming partition hasn't landed yet, the DAG
    #  waits safely instead of throwing a missing file exception
    task_sensor_bronze_telemetry = FileSensor(
        task_id="task_sensor_bronze_telemetry",
        filepath=WEEK_51_CSV,
        fs_conn_id="fs_default",
        poke_interval=15,
        timeout=300,
        mode="poke",
    )

    task_spark_bronze_to_silver = BashOperator(
        task_id="task_spark_bronze_to_silver",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            f"PYTHONPATH={PROJECT_ROOT} python3 src/batch/spark_bronze_to_silver.py "
            f"--input-path {WEEK_51_CSV}"
        ),
    )

    task_spark_silver_to_gold = BashOperator(
        task_id="task_spark_silver_to_gold",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            f"PYTHONPATH={PROJECT_ROOT} python3 src/batch/spark_silver_to_gold.py "
            f"--input-path '{WEEK_51_SILVER_PARTITION}'"
        ),
    )

    task_data_quality_gate = BashOperator(
        task_id="task_data_quality_gate",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            f"PYTHONPATH={PROJECT_ROOT} pytest tests/test_data_quality_audit.py -v"
        ),
    )

    (
        task_sensor_bronze_telemetry
        >> task_spark_bronze_to_silver
        >> task_spark_silver_to_gold
        >> task_data_quality_gate
    )
