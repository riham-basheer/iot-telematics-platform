FROM apache/airflow:2.11.0

# 1. Install Java JRE at the OS level (required by PySpark engine)
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 2. Install only the libraries required for Spark and test execution in Airflow
USER airflow
RUN pip install --no-cache-dir --default-timeout=100 \
    pyspark>=3.5.0 \
    pyarrow>=14.0.2 \
    pandas>=2.2.0 \
    numpy>=2.0.2 \
    pytest>=7.4.0
