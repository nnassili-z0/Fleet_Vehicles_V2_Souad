from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator

BRONZE_DIR = "/opt/airflow/data/raw"
SILVER_DIR = "/opt/airflow/data/processed"
TMP_DIR    = "/tmp/silver_tmp"
default_args = {"owner":"data-team","depends_on_past":False,"retries":1,"retry_delay":timedelta(minutes=5),"email_on_failure":False}

def task_clean(**ctx):
    import pandas as pd, numpy as np
    Path(TMP_DIR).mkdir(parents=True, exist_ok=True)
    dr = pd.read_csv(f"{BRONZE_DIR}/drivers.csv")
    au = pd.read_csv(f"{BRONZE_DIR}/autos.csv")
    ev = pd.read_csv(f"{BRONZE_DIR}/events.csv")
    print(f"loaded drivers={len(dr)} autos={len(au)} events={len(ev)}")
    ev = ev[ev["odometer"].astype(float) < 500000].copy()
    for c in ["vehicle_speed","fuel_level","odometer","fuel_consumed_since_restart","engine_speed","torque_at_transmission","accelerator_pedal_position","steering_wheel_angle","latitude","longitude"]:
        ev[c] = pd.to_numeric(ev[c], errors="coerce")
    ev["timestamp"] = pd.to_datetime(ev["timestamp"])
    for c in ["brake_pedal_status","high_beam_status","windshield_wiper_status","headlamp_status","parking_brake_status"]:
        ev[c] = ev[c].map({"true":True,"false":False})
    ev["AutoID"] = ev["AutoID"].astype(str)
    ev["EventID"] = ev["EventID"].astype(str)
    au["Transmission"] = au["Transmission"].replace("AUtomatic","Automatic")
    au["Year"] = pd.to_numeric(au["Year"], errors="coerce").astype("Int64")
    au["AutoID"] = au["AutoID"].astype(str)
    au["OwnerID"] = au["OwnerID"].astype(str)
    au["owner_known"] = au["OwnerID"].isin(set(dr["PersonID"].astype(str)))
    dr["DateOfBirth"] = pd.to_datetime(dr["DateOfBirth"], errors="coerce")
    dr["PersonID"] = dr["PersonID"].astype(str)
    dr.to_csv(f"{TMP_DIR}/clean_drivers.csv", index=False)
    au.to_csv(f"{TMP_DIR}/clean_autos.csv", index=False)
    ev.to_csv(f"{TMP_DIR}/clean_events.csv", index=False)
    print(f"clean_bronze OK events={len(ev)}")
    assert len(ev) == 6888, f"Expected 6888 got {len(ev)}"

def task_enrich(**ctx):
    import pandas as pd, numpy as np
    d = pd.read_csv(f"{TMP_DIR}/clean_drivers.csv", parse_dates=["DateOfBirth"])
    a = pd.read_csv(f"{TMP_DIR}/clean_autos.csv")
    e = pd.read_csv(f"{TMP_DIR}/clean_events.csv", parse_dates=["timestamp"])
    e["speed_category"] = pd.cut(e["vehicle_speed"],bins=[-1,0,30,90,120,float("inf")],labels=["stop","slow","normal","fast","overspeed"]).astype(str)
    e["driving_mode"] = np.select([(e["vehicle_speed"]==0)&(e["parking_brake_status"]==True),(e["vehicle_speed"]==0)],["parked","idle"],default="driving")
    e["fuel_alert"] = e["fuel_level"] < 20
    e["is_braking"] = e["brake_pedal_status"] == True
    e["is_accelerating"] = e["accelerator_pedal_position"] > 30
    e["event_date"] = e["timestamp"].dt.date
    e["hour_of_day"] = e["timestamp"].dt.hour
    e["day_of_week"] = e["timestamp"].dt.day_name()
    a["vehicle_age_years"] = 2024 - a["Year"]
    a["is_electric"] = a["EngineType"] == "Electric"
    a["is_hybrid"] = a["EngineType"] == "Hybrid"
    a["transmission_clean"] = a["Transmission"].replace("AUtomatic","Automatic")
    d["driver_age"] = 2024 - d["DateOfBirth"].dt.year
    d["driver_age_group"] = pd.cut(d["driver_age"],bins=[0,30,45,60,120],labels=["<30","30-45","45-60","60+"],right=False).astype(str)
    d.to_csv(f"{TMP_DIR}/enrich_drivers.csv", index=False)
    a.to_csv(f"{TMP_DIR}/enrich_autos.csv", index=False)
    e.to_csv(f"{TMP_DIR}/enrich_events.csv", index=False)
    print(f"enrich_silver OK {len(e)} rows x {len(e.columns)} cols")

def task_join(**ctx):
    import pandas as pd
    d = pd.read_csv(f"{TMP_DIR}/enrich_drivers.csv", parse_dates=["DateOfBirth"])
    a = pd.read_csv(f"{TMP_DIR}/enrich_autos.csv")
    e = pd.read_csv(f"{TMP_DIR}/enrich_events.csv", parse_dates=["timestamp"])
    df = e.merge(a, on="AutoID", how="left", suffixes=("","_auto"))
    df = df.merge(d.rename(columns={"PersonID":"OwnerID"}), on="OwnerID", how="left", suffixes=("","_drv"))
    df.to_csv(f"{TMP_DIR}/silver_joined.csv", index=False)
    print(f"join_tables OK {len(df)} rows x {len(df.columns)} cols")
    assert len(df) == 6888, f"Expected 6888 got {len(df)}"

def task_export(**ctx):
    import pandas as pd
    s = Path(SILVER_DIR)
    s.mkdir(parents=True, exist_ok=True)
    e = pd.read_csv(f"{TMP_DIR}/enrich_events.csv")
    a = pd.read_csv(f"{TMP_DIR}/enrich_autos.csv")
    d = pd.read_csv(f"{TMP_DIR}/enrich_drivers.csv")
    j = pd.read_csv(f"{TMP_DIR}/silver_joined.csv")
    try:
        import pyarrow
        e.to_parquet(s/"silver_events.parquet", index=False, compression="snappy")
        a.to_parquet(s/"silver_autos.parquet", index=False, compression="snappy")
        d.to_parquet(s/"silver_drivers.parquet", index=False, compression="snappy")
        j.to_parquet(s/"silver_fleet_joined.parquet", index=False, compression="snappy")
        print(f"export OK Parquet events={len(e)} autos={len(a)} drivers={len(d)} joined={len(j)}")
    except ImportError:
        e.to_csv(s/"silver_events.csv", index=False)
        a.to_csv(s/"silver_autos.csv", index=False)
        d.to_csv(s/"silver_drivers.csv", index=False)
        j.to_csv(s/"silver_fleet_joined.csv", index=False)
        print(f"export OK CSV events={len(e)} autos={len(a)} drivers={len(d)} joined={len(j)}")

def task_validate(**ctx):
    import pandas as pd
    s = Path(SILVER_DIR)
    errors = []
    try:
        ev = pd.read_parquet(s/"silver_events.parquet")
        au = pd.read_parquet(s/"silver_autos.parquet")
        jo = pd.read_parquet(s/"silver_fleet_joined.parquet")
    except:
        ev = pd.read_csv(s/"silver_events.csv")
        au = pd.read_csv(s/"silver_autos.csv")
        jo = pd.read_csv(s/"silver_fleet_joined.csv")
    if len(ev)!=6888: errors.append(f"events: expected 6888 got {len(ev)}")
    else: print(f"row_count OK {len(ev)}")
    if ev["odometer"].max()>=500000: errors.append(f"odometer outlier {ev['odometer'].max()}")
    else: print(f"odometer OK max={ev['odometer'].max():.0f}")
    if "AUtomatic" in au["transmission_clean"].values: errors.append("typo AUtomatic present")
    else: print("transmission OK")
    bad = set(ev["speed_category"].dropna().unique())-{"stop","slow","normal","fast","overspeed"}
    if bad: errors.append(f"bad speed_category {bad}")
    else: print(f"speed_category OK {ev['speed_category'].value_counts().to_dict()}")
    if len(jo)!=6888: errors.append(f"joined expected 6888 got {len(jo)}")
    else: print(f"joined OK {len(jo)} x {len(jo.columns)}")
    if errors: raise ValueError("FAILED\n"+"\n".join(errors))
    print("ALL CHECKS PASSED - Silver pipeline complete!")

with DAG(dag_id="dag_silver_pipeline",description="Silver pipeline",schedule_interval="@daily",start_date=datetime(2024,1,1),catchup=False,default_args=default_args,tags=["silver","pipeline"]) as dag:
    t1=PythonOperator(task_id="clean_bronze",python_callable=task_clean)
    t2=PythonOperator(task_id="enrich_silver",python_callable=task_enrich)
    t3=PythonOperator(task_id="join_tables",python_callable=task_join)
    t4=PythonOperator(task_id="export_parquet",python_callable=task_export)
    t5=PythonOperator(task_id="validate_silver",python_callable=task_validate)
    t1>>t2>>t3>>t4>>t5