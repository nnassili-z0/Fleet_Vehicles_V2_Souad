# Fleet Vehicles V2 - Medallion Architecture Data Platform

A production-grade data engineering platform implementing the medallion architecture (Bronze → Silver → Gold) for fleet vehicle telematics and analytics. Built with Apache Airflow, Great Expectations, and PostgreSQL.

## Features

- **Medallion Architecture:** Three-layer data organization with progressive transformation
- **Data Quality Framework:** Integrated detection and validation using Great Expectations
- **Star Schema Analytics:** Gold layer optimized for analytics with fact and dimension tables
- **Microservices:** Containerized services (Airflow, PostgreSQL, Great Expectations)
- **Industrial Standards:** SOLID principles, clean architecture, comprehensive naming conventions
- **Observable:** Full logging, quality artifacts, and DAG monitoring
- **Production-Ready:** Scalable design supporting multiple deployment models

## Architecture Overview

```
Source Systems
    ↓
Bronze Layer (Raw Data)
    ├─ autos.csv
    ├─ drivers.csv
    └─ events.csv
    ↓
[Quality Detection & Implementation]
    ↓
Silver Layer (Cleaned Data)
    ├─ silver_vehicle.csv
    ├─ silver_driver.csv
    └─ silver_events.csv
    ↓
[Star Schema Transformation]
    ↓
Gold Layer (Analytics Data)
    ├─ Dimensions: dim_date, dim_time, dim_vehicle, dim_driver
    ├─ Facts: fact_driving_event, fact_maintenance_alert
    └─ Analytics: vehicle_kpi, driver_score
    ↓
Analytics & Reporting
```

## Quick Start

### Prerequisites

| Tool | Minimum version |
|------|----------------|
| Docker | 24.x |
| Docker Compose | v2 |

### Setup

```bash
# Clone and navigate
cd fleet_vehicles_v2

# Start services
docker compose -f docker/docker-compose.yml up -d

# Access Airflow UI
# http://localhost:8080 (admin / admin)
```

---

## Project Structure

```
.
├── Dockerfile                  # Airflow + Python deps image
├── docker-compose.yml          # All services
├── .dockerignore               # Build context exclusions
├── .env.example                # Environment variable template
├── requirements.txt            # Python dependencies
├── dags/                       # Airflow DAG files
│   └── dag_hello_platform.py  # Validation DAG
├── plugins/                    # Airflow custom operators/hooks
├── logs/                       # Airflow task logs (git-ignored)
└── data/
    ├── raw/                    # Source data — READ-ONLY in containers
    └── processed/              # Pipeline outputs
```

---

## Quick Start

### 1 — Configure environment variables

```bash
cp .env.example .env
```

Open `.env` and fill in the required values:

```dotenv
POSTGRES_PASSWORD=changeme_strong_password
AIRFLOW_FERNET_KEY=<generate below>
AIRFLOW_SECRET_KEY=<generate below>
AIRFLOW_ADMIN_PASSWORD=changeme_admin_password
```

Generate the Fernet key and secret key:

```bash
# Fernet key
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# Secret key
python -c "import secrets; print(secrets.token_hex(32))"
```

### 2 — Create required local directories

```bash
mkdir -p dags plugins logs data/raw data/processed
```

### 3 — Build and start all services

```bash
docker compose up --build -d
```

The first run may take a few minutes while Docker pulls base images and installs dependencies. The `airflow-init` service will run DB migrations and create the admin account, then exit automatically.

### 4 — Check service health

```bash
docker compose ps
```

All services should show `healthy` or `running` status.

---

## Services

| Service | Description | Port |
|---------|-------------|------|
| `postgres` | Airflow metadata database | — (internal) |
| `airflow-init` | One-time DB migration + admin user creation | — |
| `airflow-webserver` | Airflow UI | **8080** |
| `airflow-scheduler` | DAG scheduling and task execution | — |

---

## Accessing Airflow

Open your browser at: **http://localhost:8080**

| Field | Value |
|-------|-------|
| Username | `admin` *(or the value of `AIRFLOW_ADMIN_USER` in `.env`)* |
| Password | *(value of `AIRFLOW_ADMIN_PASSWORD` in `.env`)* |

> ⚠️ Never use default credentials in a production environment.

The validation DAG `dag_hello_platform` should appear in the DAG list. Enable it manually or trigger it via the UI to confirm the platform is healthy.

---

## Environment Variables

All configuration is managed through the `.env` file (copied from `.env.example`). **Never commit `.env` to version control.**

| Variable | Description |
|----------|-------------|
| `POSTGRES_USER` | PostgreSQL username |
| `POSTGRES_PASSWORD` | PostgreSQL password |
| `POSTGRES_DB` | PostgreSQL database name |
| `AIRFLOW_FERNET_KEY` | Encryption key for Airflow connections |
| `AIRFLOW_SECRET_KEY` | Flask secret key for the webserver |
| `AIRFLOW_ADMIN_USER` | Airflow admin UI username |
| `AIRFLOW_ADMIN_PASSWORD` | Airflow admin UI password |
| `AIRFLOW_ADMIN_EMAIL` | Airflow admin email |

---

## Useful Commands

```bash
# View live logs for a specific service
docker compose logs -f airflow-scheduler

# Re-build the image after changing Dockerfile or requirements.txt
docker compose up --build -d

# Run a one-off command inside the scheduler container
docker compose exec airflow-scheduler airflow dags list

# Trigger the validation DAG manually from the CLI
docker compose exec airflow-scheduler airflow dags trigger dag_hello_platform

# Open a shell inside the webserver container
docker compose exec airflow-webserver bash
```

---

## Stopping & Cleaning Up

```bash
# Stop all services (keeps volumes)
docker compose down

# Stop and remove all volumes (⚠️ deletes the Postgres database)
docker compose down -v

# Remove the built image as well
docker compose down --rmi local
```

---

## Troubleshooting

**`airflow-webserver` is not healthy after 2 minutes**
- Check logs: `docker compose logs airflow-webserver`
- Ensure `airflow-init` completed successfully: `docker compose ps airflow-init`

**Permission errors on `logs/` or `data/processed/`**
```bash
# Airflow runs as UID 50000 inside the container
sudo chown -R 50000:50000 logs/ data/processed/
```

**Port 8080 already in use**
- Change the host port in `docker-compose.yml`: `"8081:8080"` and access `localhost:8081`.

**Fernet key error on startup**
- Make sure `.env` exists and `AIRFLOW_FERNET_KEY` is set to a valid Fernet key (44 base64 characters ending in `=`).
