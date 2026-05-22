# Fleet Vehicles V2 - Architecture Documentation

## Overview

This document describes the Medallion Architecture implementation for the Fleet Vehicles V2 data platform. The platform implements a three-layer architecture (Bronze, Silver, Gold) with integrated data quality checks and microservices orchestration.

## Architecture Layers

### 1. Bronze Layer - Raw Data Ingestion

**Purpose:** Store raw data as-is from source systems without any transformations.

**Characteristics:**
- Exact replica of source data
- No schema validation or cleaning
- Immutable historical record
- Minimal metadata

**Tables:**
- `bronze_vehicle` - Raw vehicle master data
- `bronze_driver` - Raw driver master data
- `bronze_event` - Raw telematics events

**Data Location:** `/data/bronze/`

**Format:** CSV (can be extended to Parquet, JSON)

### 2. Silver Layer - Cleaned and Validated Data

**Purpose:** Provide a single source of truth with cleaned, standardized, validated data.

**Characteristics:**
- Data quality checks enforced (detection + implementation)
- Schema standardization (snake_case columns, proper types)
- Consistent data formats and encodings
- Audit columns (created_at, updated_at)
- No aggregation - same granularity as Bronze
- Quality artifacts persisted

**Quality Checks Performed:**
1. **Detection Phase:**
   - Schema validation
   - Null constraint checks
   - Data type validation
   - Range validation (e.g., speed 0-350 km/h)
   - Uniqueness checks
   - Entity-specific rules (e.g., timestamp validity, coordinate range)

2. **Implementation Phase:**
   - Type conversion
   - Null handling
   - Outlier removal
   - Standardization
   - Feature engineering for analytical needs

**Tables:**
- `silver_vehicle` - Cleaned vehicle data
- `silver_driver` - Cleaned driver data
- `silver_event` - Cleaned and enriched events

**Data Location:** `/data/silver/`

**Artifacts:** `/artifacts/quality_reports/`

### 3. Gold Layer - Analytical Data Warehouse

**Purpose:** Provide aggregated, denormalized data optimized for analytics and reporting.

**Design Pattern:** Star Schema

#### Dimension Tables (SCD Type 2 ready)

**dim_date**
- date_id (surrogate key)
- date, year, quarter, month, day
- day_of_week, is_weekend, week_of_year
- Covers 7 years (2020-2027)

**dim_time**
- time_id (surrogate key)
- hour, minute, second
- time_period (morning, afternoon, evening, night)
- 15-minute granularity

**dim_vehicle**
- vehicle_key (surrogate key)
- auto_id, vin, make, year, transmission
- owner_id, effective_date, is_current
- Supports slowly changing dimension (SCD Type 2)

**dim_driver**
- driver_key (surrogate key)
- person_id, full_name, gender, age
- date_of_birth, effective_date, is_current
- Supports slowly changing dimension (SCD Type 2)

#### Fact Tables

**fact_driving_event**
- event_key (surrogate key)
- event_id, vehicle_key, driver_key
- date_id, time_id (temporal dimensions)
- Measurements: odometer, vehicle_speed, fuel_level, engine_speed, accelerator_pedal_position
- Flags: is_speeding, is_harsh_braking, is_harsh_acceleration
- Calculated: fuel_consumption_rate

**fact_maintenance_alert**
- alert_key (surrogate key)
- vehicle_key, date_id
- alert_type (HIGH_MILEAGE, FUEL_LEVEL, HARSH_DRIVING)
- alert_severity (LOW, MEDIUM, HIGH, CRITICAL)
- resolved, estimated_maintenance_hours

#### Analytics Views (Materialized)

**analytics_vehicle_kpi**
- Fleet-level KPIs: total_events, total_distance, avg_speed
- Safety metrics: harsh_events, max_speed
- Maintenance metrics: fuel_consumption, maintenance_alerts

**analytics_driver_score**
- Safety score (0-100)
- Efficiency score (0-100)
- Risk level (LOW, MEDIUM, HIGH)
- Event counters: harsh_events, speeding_events

**Data Location:** `/data/gold/`

## Pipeline Workflows

### Bronze to Silver Pipeline

Execution: Daily at 01:00 UTC

```
Bronze Data
    ↓
[Detection Phase]
    ├─ Schema Validation
    ├─ Null Constraints
    ├─ Data Type Check
    ├─ Range Validation
    └─ Entity-Specific Rules
    ↓
[Quality Report Generated]
    ↓
[Implementation Phase]
    ├─ Type Conversion
    ├─ Null Handling
    ├─ Outlier Removal
    ├─ Standardization
    └─ Feature Engineering
    ↓
[Quality Artifacts Written]
    ↓
Silver Data + Quality Reports
```

**Entry Point:** `src/pipelines/bronze_to_silver/transformer.py::BronzeToSilverTransformer`

**DAG:** `dag_bronze_to_silver.py`

### Silver to Gold Pipeline

Execution: Daily at 03:00 UTC (after Bronze to Silver completes)

```
Silver Data
    ↓
[Dimension Creation]
    ├─ dim_date (7-year range)
    ├─ dim_time (15-min intervals)
    ├─ dim_vehicle (SCD Type 2)
    └─ dim_driver (SCD Type 2)
    ↓
[Fact Table Creation]
    ├─ fact_driving_event (with metrics)
    └─ fact_maintenance_alert (rules-based)
    ↓
[Analytics Views]
    ├─ vehicle_kpi
    └─ driver_score
    ↓
Gold Data Warehouse
```

**Entry Point:** `src/pipelines/silver_to_gold/transformer.py::SilverToGoldTransformer`

**DAG:** `dag_silver_to_gold.py`

## Microservices Architecture

### Service Mesh

```
┌─────────────────────────────────────────┐
│  Fleet Vehicles V2 - Medallion Stack    │
└─────────────────────────────────────────┘
         │                  │              │
    ┌────▼──────┐      ┌────▼─────┐   ┌──▼──────┐
    │ Airflow   │      │PostgreSQL │   │ Great   │
    │Webserver  │      │ Database  │   │Expected │
    │ :8080     │      │ :5432     │   │ness     │
    └────┬──────┘      └────┬──────┘   └──┬──────┘
         │                  │              │
    [Orchestration]    [Metadata+Data] [Validation]
```

### Service Definitions

#### 1. PostgreSQL (Container: `medallion-postgres`)

**Role:** Metadata store and data warehouse

**Port:** 5432

**Responsibilities:**
- Airflow DAG/task metadata
- Connection tracking
- Fleet vehicles database
- XCom (Airflow inter-task communication)

**Health Check:** pg_isready every 10s

**Volumes:**
- `postgres-data:/var/lib/postgresql/data`

#### 2. Airflow Webserver (Container: `medallion-airflow-webserver`)

**Role:** DAG orchestration UI and REST API

**Port:** 8080

**Responsibilities:**
- Visual DAG management
- Task monitoring
- Manual DAG triggering
- Logs and metrics viewing
- REST API for external systems

**Health Check:** HTTP /health every 30s

**Features:**
- 2 worker processes
- 300s worker timeout
- LocalExecutor (suitable for dev/small deployments)

#### 3. Airflow Scheduler (Container: `medallion-airflow-scheduler`)

**Role:** Pipeline execution engine

**Responsibilities:**
- Continuous DAG monitoring
- Task scheduling based on configurations
- Dependency management
- Automatic retry on failure
- SLA enforcement

**Health Check:** Airflow scheduler job check every 30s

#### 4. Great Expectations (Container: `medallion-great-expectations`)

**Role:** Data quality validation engine

**Responsibilities:**
- Quality check execution
- Expectation validation
- Report generation
- Integration with Airflow

**Mounted Volumes:**
- Source data (read-only)
- Quality reports (write)

## Data Flow

### Complete End-to-End Flow

```
Source Systems
    ↓
Bronze Layer (/data/bronze/)
    ├─ autos.csv
    ├─ drivers.csv
    └─ events.csv
    ↓
[dag_bronze_to_silver - Daily 01:00]
    ├─ Load & Quality Check
    ├─ Clean & Transform
    └─ Write & Report
    ↓
Silver Layer (/data/silver/)
    ├─ silver_vehicle.csv
    ├─ silver_driver.csv
    └─ silver_events.csv
    ↓
[Artifacts]
    └─ /artifacts/quality_reports/quality_*.json
    ↓
[dag_silver_to_gold - Daily 03:00]
    ├─ Load Silver
    ├─ Build Dimensions
    ├─ Build Facts
    └─ Build Analytics
    ↓
Gold Layer (/data/gold/)
    ├─ Dimensions: dim_date, dim_time, dim_vehicle, dim_driver
    ├─ Facts: fact_driving_event, fact_maintenance_alert
    └─ Analytics: vehicle_kpi, driver_score
    ↓
Analytics & Reporting Systems
```

## Configuration Management

### Environment Variables

```
ENV                                Default: development
DB_HOST                            Default: postgres
DB_PORT                            Default: 5432
DB_NAME                            Default: fleet_vehicles
DB_USER                            Default: airflow
DB_PASSWORD                        Default: airflow

QUALITY_VALIDATION_ENABLED         Default: true
ANOMALY_DETECTION_ENABLED          Default: true
OUTLIER_THRESHOLD_PERCENTILE       Default: 0.5
MAX_NULL_PERCENTAGE                Default: 10.0
QUALITY_CHECKPOINT_ENABLED         Default: true

BRONZE_TO_SILVER_BATCH_SIZE        Default: 10000
SILVER_TO_GOLD_BATCH_SIZE          Default: 50000
```

### Configuration File: `src/config/settings.py`

Central configuration management following SoC principle.

## Directory Structure

```
fleet_vehicles_v2/
├── src/
│   ├── config/                    # Configuration management
│   │   └── settings.py
│   ├── infrastructure/            # Cross-cutting concerns
│   │   └── logging_config.py
│   ├── models/
│   │   └── schemas/
│   │       └── domain_models.py  # Data models for all layers
│   ├── orchestration/             # Airflow integration
│   │   ├── dags/
│   │   │   ├── dag_bronze_to_silver.py
│   │   │   └── dag_silver_to_gold.py
│   │   └── utils/
│   ├── pipelines/
│   │   ├── bronze_to_silver/
│   │   │   └── transformer.py
│   │   └── silver_to_gold/
│   │       └── transformer.py
│   └── quality/
│       ├── detectors/
│       │   ├── base_quality_checker.py
│       │   ├── event_quality_checker.py
│       │   └── master_data_quality_checker.py
│       └── expectations/
│           └── quality_report_writer.py
├── data/
│   ├── bronze/                    # Raw data
│   ├── silver/                    # Cleaned data
│   ├── gold/                      # Analytical data
│   └── .temp/                     # Working directory
├── artifacts/
│   ├── logs/                      # Application logs
│   └── quality_reports/           # Quality check reports
├── docker/
│   ├── Dockerfile
│   └── docker-compose.yml
├── documentation/                 # This folder
├── requirements.txt
├── .env.example
└── README.md
```

## Performance Considerations

### Batch Sizes
- Bronze to Silver: 10,000 rows (configurable)
- Silver to Gold: 50,000 rows (configurable)

### Indexing Strategy (Future)
- fact_driving_event: clustered on (vehicle_key, date_id)
- dim_vehicle: unique on (auto_id)
- dim_driver: unique on (person_id)

### Partitioning (Future)
- fact_driving_event: partitioned by year_month
- fact_maintenance_alert: partitioned by year_month

## Data Lineage

```
Source → Bronze → [Quality Check] → Silver → [Star Schema] → Gold → Analytics
         ↓
         Immutable
         Record

         ↓
         Schema + Data Types
         Audit Columns
         Consistency
         
         ↓
         Analytics-Ready
         Dimensional Model
         Aggregations
         KPIs
```

## Quality Assurance

### Validation Layers

1. **Bronze:** No validation (immutable source)
2. **Silver:** Full validation with rejection
3. **Gold:** Schema validation only (pre-built dimensions)

### Quality Metrics

- Rows processed
- Rows passed/failed per check
- Critical/error/warning issue counts
- Execution time
- Data lineage tracking

### Quality Reports

All quality reports persisted as JSON in `/artifacts/quality_reports/`:
- Timestamp of execution
- Execution ID for tracing
- Entity type
- Pipeline stage
- Check results with issue details

## Deployment Models

### Development (Current)
- Single-node Docker Compose
- LocalExecutor for Airflow
- File-based storage
- In-memory configurations

### Staging
- Kubernetes deployment
- Celery Executor for Airflow
- MinIO for distributed storage
- Prometheus + Grafana monitoring

### Production
- Kubernetes with auto-scaling
- Celery Executor with multiple workers
- Cloud storage (S3, Azure Blob)
- ELK stack for logging
- PagerDuty for alerting

## Monitoring & Alerting

### Airflow Built-in
- DAG success/failure rates
- Task duration SLAs
- Scheduler health
- Connection status

### Custom Metrics
- Quality check pass rates
- Data volume trends
- Pipeline execution time
- Error/critical issue counts

### Logs Location
- Airflow: `${AIRFLOW_HOME}/logs/`
- Application: `/opt/airflow/artifacts/logs/`
- Docker: `docker compose logs [service_name]`

## Related Documentation

- [CONVENTIONS.md](./CONVENTIONS.md) - Naming and coding standards
- [DEPLOYMENT.md](./DEPLOYMENT.md) - Setup and deployment guide
- [README.md](../README.md) - Project overview
