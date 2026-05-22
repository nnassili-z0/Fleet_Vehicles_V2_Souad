# ============================================================
# Dockerfile for Fleet Vehicles V2 - Airflow Service
# ============================================================
# Multi-stage build for optimized image size
# Based on official Airflow image
# NOTE: Using pre-built image instead of custom build for faster deployment

FROM apache/airflow:2.8.3-python3.11

# ========================================
# 1. SYSTEM DEPENDENCIES
# ========================================

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# ========================================
# 2. PYTHON DEPENDENCIES
# ========================================

USER airflow

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# ========================================
# 3. APPLICATION SETUP
# ========================================

# Copy source code
COPY --chown=airflow:airflow src/ /opt/airflow/src/
COPY --chown=airflow:airflow .env.example /opt/airflow/.env.example

# Create necessary directories
RUN mkdir -p /opt/airflow/data/{bronze,silver,gold,.temp} \
    && mkdir -p /opt/airflow/artifacts/{logs,quality_reports}

# ========================================
# 4. AIRFLOW INITIALIZATION
# ========================================

# Set environment variables
ENV AIRFLOW_HOME=/opt/airflow
ENV PYTHONPATH=/opt/airflow
ENV AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/src/orchestration/dags
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False

# ========================================
# 5. HEALTH CHECK
# ========================================

HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# ========================================
# 6. ENTRY POINT
# ========================================

EXPOSE 8080 5555

CMD ["airflow", "webserver"]