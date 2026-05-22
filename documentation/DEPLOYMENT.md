# Fleet Vehicles V2 - Deployment Guide

## Quick Start

### Prerequisites

- Docker and Docker Compose installed
- 4GB RAM minimum (8GB recommended)
- 10GB free disk space
- Python 3.11+ (for local development)

### Startup in 3 Steps

```bash
# 1. Clone repository and navigate
cd fleet_vehicles_v2

# 2. Build and start services
docker compose -f docker/docker-compose.yml up -d

# 3. Verify services
docker compose -f docker/docker-compose.yml ps
```

### First Access

- Airflow UI: http://localhost:8080
- Username: `admin`
- Password: `admin`

## Detailed Setup

### 1. Environment Configuration

Create `.env` file from template:

```bash
cp .env.example .env
```

Configure environment variables:

```
ENV=development
DB_HOST=postgres
DB_PORT=5432
DB_NAME=fleet_vehicles
DB_USER=airflow
DB_PASSWORD=airflow

QUALITY_VALIDATION_ENABLED=true
ANOMALY_DETECTION_ENABLED=true
OUTLIER_THRESHOLD_PERCENTILE=0.5
MAX_NULL_PERCENTAGE=10.0
QUALITY_CHECKPOINT_ENABLED=true

BRONZE_TO_SILVER_BATCH_SIZE=10000
SILVER_TO_GOLD_BATCH_SIZE=50000
```

### 2. Data Preparation

Place source files in Bronze layer:

```
data/bronze/
├── autos.csv          # Vehicle master data
├── drivers.csv        # Driver master data
└── events.csv         # Telematics events
```

Required columns:

**autos.csv:**
- AutoID, VIN, Make, Year, Transmission, OwnerID

**drivers.csv:**
- PersonID, FullName, Gender, DateOfBirth

**events.csv:**
- AutoID, EventID, timestamp, odometer, vehicle_speed, fuel_level,
  fuel_consumed_since_restart, engine_speed, torque_at_transmission,
  accelerator_pedal_position, steering_wheel_angle, latitude, longitude,
  brake_pedal_status, high_beam_status, windshield_wiper_status,
  headlamp_status, parking_brake_status

### 3. Docker Compose Build

```bash
# Build images
docker compose -f docker/docker-compose.yml build

# Verify build
docker images | grep medallion
```

### 4. Service Startup

```bash
# Start all services in background
docker compose -f docker/docker-compose.yml up -d

# Monitor startup
docker compose -f docker/docker-compose.yml logs -f

# Check service health
docker compose -f docker/docker-compose.yml ps
```

### 5. Airflow Initialization

Services auto-initialize on first startup:
- PostgreSQL database created
- Airflow metadata schema initialized
- Admin user created (admin/admin)

Verify in Airflow UI:
- Navigate to http://localhost:8080
- Should see DAGs: `medallion_bronze_to_silver`, `medallion_silver_to_gold`

## Local Development

### Python Environment Setup

```bash
# Create virtual environment
python -m venv venv

# Activate (Linux/Mac)
source venv/bin/activate

# Activate (Windows)
venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Install development tools
pip install black pylint mypy flake8 isort pytest pytest-cov
```

### Code Quality Tools

```bash
# Format code
black src/

# Check imports
isort src/

# Lint code
pylint src/

# Type checking
mypy src/

# Run tests
pytest tests/ -v --cov=src/
```

## Pipeline Execution

### Manual DAG Trigger via UI

1. Navigate to Airflow UI (http://localhost:8080)
2. Click on DAG name: `medallion_bronze_to_silver`
3. Click "Trigger DAG" button
4. Monitor execution in UI

### Manual DAG Trigger via CLI

```bash
# Inside container
docker compose -f docker/docker-compose.yml exec airflow-webserver \
  airflow dags trigger medallion_bronze_to_silver

# Or from local environment (if Airflow installed)
airflow dags trigger medallion_bronze_to_silver
```

### View Logs

```bash
# Airflow logs
docker compose -f docker/docker-compose.yml logs airflow-scheduler

# Application logs
docker compose -f docker/docker-compose.yml exec airflow-scheduler \
  tail -f /opt/airflow/artifacts/logs/*.log

# Quality reports
docker compose -f docker/docker-compose.yml exec airflow-scheduler \
  cat /opt/airflow/artifacts/quality_reports/quality_*.json | jq .
```

## Monitoring & Debugging

### Service Health

```bash
# Check all services
docker compose -f docker/docker-compose.yml ps

# Check specific service
docker compose -f docker/docker-compose.yml ps postgres

# Service logs
docker compose -f docker/docker-compose.yml logs [service_name]

# Real-time logs
docker compose -f docker/docker-compose.yml logs -f [service_name]
```

### Database Access

```bash
# Connect to PostgreSQL
docker compose -f docker/docker-compose.yml exec postgres \
  psql -U airflow -d fleet_vehicles

# Common queries
SELECT * FROM silver_vehicle LIMIT 10;
SELECT COUNT(*) FROM fact_driving_event;
SELECT * FROM analytics_vehicle_kpi;
```

### Data Inspection

```bash
# View Silver data
docker compose -f docker/docker-compose.yml exec airflow-scheduler \
  head -20 /opt/airflow/data/silver/silver_vehicle.csv

# View Gold data
docker compose -f docker/docker-compose.yml exec airflow-scheduler \
  head -20 /opt/airflow/data/gold/dim_date.csv

# View Quality Reports
docker compose -f docker/docker-compose.yml exec airflow-scheduler \
  ls -la /opt/airflow/artifacts/quality_reports/
```

## Troubleshooting

### Services Won't Start

```bash
# Check Docker daemon
docker ps

# Check disk space
df -h

# Check port availability
netstat -tlnp | grep 8080

# Clear volumes and restart
docker compose -f docker/docker-compose.yml down -v
docker compose -f docker/docker-compose.yml up -d
```

### PostgreSQL Connection Issues

```bash
# Test connection
docker compose -f docker/docker-compose.yml exec postgres \
  pg_isready -U airflow -d airflow

# Check credentials
docker compose -f docker/docker-compose.yml exec postgres \
  psql -U airflow -d airflow -c "\du"

# View PostgreSQL logs
docker compose -f docker/docker-compose.yml logs postgres
```

### DAG Not Running

1. Check DAG syntax:
   ```bash
   python -m py_compile src/orchestration/dags/dag_bronze_to_silver.py
   ```

2. Check Airflow logs:
   ```bash
   docker compose -f docker/docker-compose.yml logs airflow-scheduler | tail -100
   ```

3. Verify DAG in UI and check for import errors

### Out of Memory

```bash
# Monitor memory usage
docker stats

# Reduce batch sizes in .env
BRONZE_TO_SILVER_BATCH_SIZE=5000
SILVER_TO_GOLD_BATCH_SIZE=25000

# Restart services
docker compose -f docker/docker-compose.yml restart
```

### File Permission Issues

```bash
# Fix data directory permissions
docker compose -f docker/docker-compose.yml exec airflow-scheduler \
  chmod -R 777 /opt/airflow/data

# Fix artifacts directory
docker compose -f docker/docker-compose.yml exec airflow-scheduler \
  chmod -R 777 /opt/airflow/artifacts
```

## Advanced Configuration

### Scale to Multiple Workers

Edit `docker-compose.yml` to add worker services:

```yaml
airflow-worker-1:
  <<: *airflow-common
  command: celery worker --queues=default
  depends_on:
    - airflow-init

airflow-worker-2:
  <<: *airflow-common
  command: celery worker --queues=default
  depends_on:
    - airflow-init

# Change executor in environment
AIRFLOW__CORE__EXECUTOR: CeleryExecutor
AIRFLOW__CELERY__BROKER_URL: redis://redis:6379/0
```

### Add Redis for Celery

```yaml
redis:
  image: redis:7-alpine
  ports:
    - "6379:6379"
  networks:
    - medallion-net
```

### External Database

Modify connection in `.env`:

```
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://user:pass@external-host:5432/airflow
```

### Production Checklist

- [ ] Use environment-specific configurations
- [ ] Update Airflow secret key
- [ ] Configure proper database backend
- [ ] Set up monitoring and alerting
- [ ] Enable SSL/TLS for connections
- [ ] Configure log aggregation
- [ ] Set up backups for PostgreSQL
- [ ] Use external blob storage (S3, Azure)
- [ ] Configure RBAC and authentication
- [ ] Set resource limits on containers
- [ ] Test disaster recovery procedures
- [ ] Document runbooks and escalation

## Performance Tuning

### Airflow Settings

```
AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=16
AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG=1
AIRFLOW__SCHEDULER__CATCHUP_BY_DEFAULT=False
AIRFLOW__SCHEDULER__SCHEDULE_INTERVAL_CHECK=60
```

### Database Tuning

```sql
-- PostgreSQL configuration for better performance
ALTER SYSTEM SET shared_buffers = '256MB';
ALTER SYSTEM SET effective_cache_size = '1GB';
ALTER SYSTEM SET work_mem = '16MB';
-- Then: docker restart postgres
```

### Pipeline Optimization

```python
# In transformer
BATCH_SIZE = 50000  # Process large datasets in chunks
for chunk in read_chunks(filepath, batch_size=BATCH_SIZE):
    process(chunk)
    write(chunk)
```

## Backup & Recovery

### Backup Strategy

```bash
# Backup PostgreSQL
docker compose -f docker/docker-compose.yml exec postgres \
  pg_dump -U airflow fleet_vehicles > backup.sql

# Backup data directories
tar -czf data_backup.tar.gz data/ artifacts/

# Backup configuration
cp .env .env.backup
```

### Restore from Backup

```bash
# Restore PostgreSQL
docker compose -f docker/docker-compose.yml exec postgres \
  psql -U airflow fleet_vehicles < backup.sql

# Restore data
tar -xzf data_backup.tar.gz

# Restart services
docker compose -f docker/docker-compose.yml restart
```

## Cleanup

### Stop Services

```bash
# Stop all services (keep volumes)
docker compose -f docker/docker-compose.yml stop

# Stop and remove containers
docker compose -f docker/docker-compose.yml down

# Remove everything (including volumes)
docker compose -f docker/docker-compose.yml down -v
```

### Clean Up Docker Resources

```bash
# Remove unused images
docker image prune

# Remove unused volumes
docker volume prune

# Full cleanup
docker system prune -a --volumes
```

## Related Documentation

- [ARCHITECTURE.md](./ARCHITECTURE.md) - System architecture
- [CONVENTIONS.md](./CONVENTIONS.md) - Coding standards
- [README.md](../README.md) - Project overview
