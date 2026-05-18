"""
dag_gold_pipeline.py  —  SELF-CONTAINED VERSION
-------------------------------------------------
DAG Gold Pipeline : KPIs · Scoring · ML · PostgreSQL
Lit silver_fleet_joined et produit les 4 tables Gold.

Tasks (t2/t3/t4 tournent en parallèle) :
  t1: load_silver         → charge silver_fleet_joined
  t2: compute_kpis        → fleet_kpis (5 lignes)
  t3: score_drivers       → driver_scores (5 lignes)
  t4: detect_anomalies    → anomalies (~336 lignes)
  t5: build_alerts        → maintenance_alerts (3 lignes)
  t6: write_gold          → export CSV/Parquet + PostgreSQL
"""
from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

SILVER_DIR = "/opt/airflow/data/processed"
GOLD_DIR   = "/opt/airflow/data/gold"
TMP_DIR    = "/tmp/gold_tmp"

DB_URL = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"

default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


# ══════════════════════════════════════════════════════
# TASK 1 — Load Silver
# ══════════════════════════════════════════════════════
def task_load_silver(**ctx):
    import pandas as pd
    Path(TMP_DIR).mkdir(parents=True, exist_ok=True)

    try:
        df = pd.read_parquet(f"{SILVER_DIR}/silver_fleet_joined.parquet")
    except Exception:
        df = pd.read_csv(f"{SILVER_DIR}/silver_fleet_joined.csv", parse_dates=["timestamp"])

    # Normalize bool columns
    for col in ["brake_pedal_status", "high_beam_status",
                "windshield_wiper_status", "parking_brake_status"]:
        if col in df.columns:
            df[col] = df[col].map(
                {"True": True, "False": False, True: True, False: False,
                 "true": True, "false": False}
            )

    df.to_csv(f"{TMP_DIR}/silver_joined.csv", index=False)
    print(f"load_silver OK: {len(df)} rows x {len(df.columns)} cols")
    print(f"Active vehicles: {df['AutoID'].nunique()}")


# ══════════════════════════════════════════════════════
# TASK 2 — Fleet KPIs
# ══════════════════════════════════════════════════════
def task_compute_kpis(**ctx):
    import pandas as pd

    df = pd.read_csv(f"{TMP_DIR}/silver_joined.csv", parse_dates=["timestamp"])
    kpis = []

    for auto_id, grp in df.groupby("AutoID"):
        odo = grp["odometer"].dropna()
        kpi = {
            "auto_id"          : str(auto_id),
            "computed_date"    : str(pd.Timestamp.today().date()),
            "model"            : grp["Model"].iloc[0] if "Model" in grp.columns else "Unknown",
            "engine_type"      : grp["EngineType"].iloc[0] if "EngineType" in grp.columns else "",
            "avg_speed"        : round(float(grp["vehicle_speed"].mean()), 2),
            "max_speed"        : round(float(grp["vehicle_speed"].max()), 2),
            "avg_fuel_level"   : round(float(grp["fuel_level"].mean()), 2),
            "min_fuel_level"   : round(float(grp["fuel_level"].min()), 2),
            "total_distance_km": round(float(odo.max() - odo.min()), 2) if len(odo) > 1 else 0.0,
            "total_events"     : int(len(grp)),
            "driving_ratio"    : round(float((grp["driving_mode"] == "driving").mean()), 4),
            "idle_ratio"       : round(float((grp["driving_mode"] == "idle").mean()), 4),
            "maintenance_due"  : bool((grp["EventCategoryID"] == 1).any()),
        }
        kpis.append(kpi)
        print(f"  KPI AutoID={auto_id} | {kpi['model']} | avg_speed={kpi['avg_speed']} | dist={kpi['total_distance_km']}km")

    fleet_kpis = pd.DataFrame(kpis)
    fleet_kpis.to_csv(f"{TMP_DIR}/fleet_kpis.csv", index=False)
    print(f"compute_kpis OK: {len(fleet_kpis)} rows x {len(fleet_kpis.columns)} cols")


# ══════════════════════════════════════════════════════
# TASK 3 — Driver Scores
# ══════════════════════════════════════════════════════
def task_score_drivers(**ctx):
    import pandas as pd

    df = pd.read_csv(f"{TMP_DIR}/silver_joined.csv", parse_dates=["timestamp"])
    for col in ["brake_pedal_status", "high_beam_status", "windshield_wiper_status"]:
        if col in df.columns:
            df[col] = df[col].map({"True": True, "False": False, True: True, False: False})

    scores = []
    for auto_id, grp in df.groupby("AutoID"):
        n = len(grp)
        overspeed = int((grp["vehicle_speed"] > 120).sum())
        brakes    = int((grp["brake_pedal_status"] == True).sum())
        speed_score  = max(0.0, 100 - (overspeed / n) * 1000)
        brake_score  = max(0.0, 100 - (brakes / n) * 50)
        safety_score = round((speed_score * 0.5) + (brake_score * 0.5), 1)
        scores.append({
            "auto_id"         : str(auto_id),
            "driver_id"       : str(grp["OwnerID"].iloc[0]) if "OwnerID" in grp.columns else "",
            "driver_name"     : grp["FullName"].iloc[0] if "FullName" in grp.columns else "Unknown",
            "driver_age_group": grp["driver_age_group"].iloc[0] if "driver_age_group" in grp.columns else "",
            "computed_date"   : str(pd.Timestamp.today().date()),
            "safety_score"    : safety_score,
            "speed_score"     : round(speed_score, 1),
            "brake_score"     : round(brake_score, 1),
            "overspeed_count" : overspeed,
            "brake_events"    : brakes,
            "high_beam_rate"  : round(float((grp["high_beam_status"] == True).mean()), 4) if "high_beam_status" in grp.columns else 0.0,
            "wiper_rate"      : round(float((grp["windshield_wiper_status"] == True).mean()), 4) if "windshield_wiper_status" in grp.columns else 0.0,
            "total_events"    : n,
        })
        print(f"  Score AutoID={auto_id} | {grp['FullName'].iloc[0] if 'FullName' in grp.columns else '?'} | {safety_score}/100")

    ds = pd.DataFrame(scores)
    ds["safety_rank"] = ds["safety_score"].rank(ascending=False, method="min").astype(int)
    ds.to_csv(f"{TMP_DIR}/driver_scores.csv", index=False)
    print(f"score_drivers OK: {len(ds)} rows x {len(ds.columns)} cols")


# ══════════════════════════════════════════════════════
# TASK 4 — Anomaly Detection
# ══════════════════════════════════════════════════════
def task_detect_anomalies(**ctx):
    import pandas as pd
    from sklearn.ensemble import IsolationForest

    df = pd.read_csv(f"{TMP_DIR}/silver_joined.csv", parse_dates=["timestamp"])

    features = ["vehicle_speed", "fuel_consumed_since_restart",
                "engine_speed", "torque_at_transmission"]
    X = df[features].fillna(0)

    model = IsolationForest(contamination=0.05, random_state=42, n_estimators=100)
    df["anomaly_flag"]  = model.fit_predict(X)
    df["anomaly_score"] = model.score_samples(X)

    anom = df[df["anomaly_flag"] == -1].copy()

    def classify(row):
        if row["vehicle_speed"] > 120:        return ("overspeed",   "H")
        if row.get("fuel_level", 100) < 15:   return ("low_fuel",    "H")
        if row["engine_speed"] > 4000:         return ("high_rpm",    "M")
        return ("statistical", "L")

    anom[["anomaly_type", "severity"]] = anom.apply(classify, axis=1, result_type="expand")
    anom["detected_at"] = str(pd.Timestamp.now())

    out_cols = ["EventID", "AutoID", "timestamp", "detected_at",
                "anomaly_type", "severity", "anomaly_score",
                "vehicle_speed", "fuel_level", "engine_speed"]
    out_cols = [c for c in out_cols if c in anom.columns]
    anom[out_cols].to_csv(f"{TMP_DIR}/anomalies.csv", index=False)

    from collections import Counter
    print(f"detect_anomalies OK: {len(anom)} anomalies")
    print(f"  types: {dict(Counter(anom['anomaly_type']))}")
    print(f"  severity: {dict(Counter(anom['severity']))}")


# ══════════════════════════════════════════════════════
# TASK 5 — Maintenance Alerts
# ══════════════════════════════════════════════════════
def task_build_alerts(**ctx):
    import pandas as pd

    df = pd.read_csv(f"{TMP_DIR}/silver_joined.csv", parse_dates=["timestamp"])
    SERVICE_KM = 16_000
    ALERT_KM   = 2_000
    alerts = []

    for auto_id, grp in df.groupby("AutoID"):
        odo = grp["odometer"].dropna()
        max_odo = float(odo.max()) if len(odo) > 0 else 0.0
        model   = grp["Model"].iloc[0] if "Model" in grp.columns else "Unknown"
        next_svc = (max_odo // SERVICE_KM + 1) * SERVICE_KM
        km_left  = next_svc - max_odo

        if km_left < ALERT_KM:
            alerts.append({
                "auto_id": str(auto_id), "model": model,
                "alert_type": "mileage",
                "message": f"Service due in {km_left:.0f} km (next at {next_svc:.0f} km)",
                "current_odometer": round(max_odo, 2),
                "next_service_km": round(next_svc, 2),
                "status": "open",
            })
            print(f"  Alert AutoID={auto_id} | {model} | service in {km_left:.0f} km")

        for _, ev in grp[grp["EventCategoryID"] == 1].iterrows():
            alerts.append({
                "auto_id": str(auto_id), "model": model,
                "alert_type": "event",
                "message": str(ev.get("EventMessage", "Maintenance required")),
                "current_odometer": round(float(ev["odometer"]), 2),
                "next_service_km": round(next_svc, 2),
                "status": "open",
            })

    ma = pd.DataFrame(alerts) if alerts else pd.DataFrame(
        columns=["auto_id","model","alert_type","message","current_odometer","next_service_km","status"])
    ma["detected_at"] = str(pd.Timestamp.now())
    ma.to_csv(f"{TMP_DIR}/maintenance_alerts.csv", index=False)
    print(f"build_alerts OK: {len(ma)} alerts")


# ══════════════════════════════════════════════════════
# TASK 6 — Write Gold
# ══════════════════════════════════════════════════════
def task_write_gold(**ctx):
    import pandas as pd

    gold = Path(GOLD_DIR)
    gold.mkdir(parents=True, exist_ok=True)

    tables = {
        "fleet_kpis"         : pd.read_csv(f"{TMP_DIR}/fleet_kpis.csv"),
        "driver_scores"      : pd.read_csv(f"{TMP_DIR}/driver_scores.csv"),
        "anomalies"          : pd.read_csv(f"{TMP_DIR}/anomalies.csv"),
        "maintenance_alerts" : pd.read_csv(f"{TMP_DIR}/maintenance_alerts.csv"),
    }

    # Try Parquet first, fallback CSV
    try:
        import pyarrow  # noqa
        for name, df in tables.items():
            df.to_parquet(gold / f"{name}.parquet", index=False, compression="snappy")
        fmt = "Parquet"
    except ImportError:
        for name, df in tables.items():
            df.to_csv(gold / f"{name}.csv", index=False)
        fmt = "CSV"

    # Try PostgreSQL
    try:
        from sqlalchemy import create_engine, text
        engine = create_engine(DB_URL)
        with engine.connect() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS gold"))
            conn.commit()
        for name, df in tables.items():
            df.to_sql(name, engine, schema="gold", if_exists="replace", index=False, method="multi")
            print(f"  ✅ PostgreSQL gold.{name}: {len(df)} rows")
        pg_ok = True
    except Exception as e:
        print(f"  PostgreSQL skipped: {e}")
        pg_ok = False

    print(f"\nwrite_gold OK ({fmt}{' + PostgreSQL' if pg_ok else ''})")
    for name, df in tables.items():
        print(f"  {name:<25} {len(df):>5} rows × {len(df.columns):>2} cols")


# ══════════════════════════════════════════════════════
# DAG DEFINITION
# ══════════════════════════════════════════════════════
with DAG(
    dag_id="dag_gold_pipeline",
    description="Gold: fleet_kpis · driver_scores · anomalies · maintenance_alerts",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["gold", "pipeline", "fleet"],
) as dag:

    t1 = PythonOperator(task_id="load_silver",       python_callable=task_load_silver)
    t2 = PythonOperator(task_id="compute_kpis",      python_callable=task_compute_kpis)
    t3 = PythonOperator(task_id="score_drivers",     python_callable=task_score_drivers)
    t4 = PythonOperator(task_id="detect_anomalies",  python_callable=task_detect_anomalies)
    t5 = PythonOperator(task_id="build_alerts",      python_callable=task_build_alerts)
    t6 = PythonOperator(task_id="write_gold",        python_callable=task_write_gold)

    # t2, t3, t4, t5 run in parallel after t1
    t1 >> [t2, t3, t4, t5] >> t6
