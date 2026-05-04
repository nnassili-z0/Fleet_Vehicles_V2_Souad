# ============================================================
# Dockerfile — Airflow Data Environment
# Base image: Apache Airflow 2.9.1 (Python 3.11)
# ============================================================
FROM apache/airflow:2.9.1-python3.11

# Switch to root to install system-level dependencies
USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to the airflow user for pip installs
USER airflow

# Copy and install Python dependencies
COPY requirements.txt ../requirements.txt
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r ../requirements.txt

# Copy DAGs and plugins into the image
# (can also be mounted as volumes in docker-compose)
COPY --chown=airflow:root ../Airflow/dags/ ${AIRFLOW_HOME}/dags/
COPY --chown=airflow:root ../Airflow/plugins/ ${AIRFLOW_HOME}/plugins/

# Default entrypoint is inherited from the base image
