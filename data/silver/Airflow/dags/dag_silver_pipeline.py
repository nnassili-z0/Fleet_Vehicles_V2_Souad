"""
dag_silver_pipeline.py
----------------------
DAG Airflow pour le pipeline Silver.
Planifié @daily — déclenche automatiquement après dag_ingest_fleet.

Tâches :
  t1_clean    → Nettoyage Bronze
  t2_enrich   → Enrichissement colonnes dérivées
  t3_join     → Jointures 3 tables
  t4_export   → Export Parquet Silver
  t5_validate → Validation qualité
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

# ── Paramètres ───────────────────────────────────────────────────────────
BRONZE_DIR = "/opt/airflow/data/raw"      # monté en :ro depuis data/bronze
SILVER_DIR = "/opt/airflow/data/processed"  # monté depuis data/silver

default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


# ── Tâches ────────────────────────────────────────────────────────────────

def task_clean(**ctx):
    """Étape 1 : Nettoyage Bronze → stockage XCom."""
    import pandas as pd
    from src.processing.clean_bronze import clean_events, clean_autos, clean_drivers

    drivers_raw = pd.read_csv(f"{BRONZE_DIR}/drivers.csv")
    autos_raw   = pd.read_csv(f"{BRONZE_DIR}/autos.csv")
    events_raw  = pd.read_csv(f"{BRONZE_DIR}/events.csv")

    driver_ids = set(drivers_raw["PersonID"].astype(str))
    clean_drv  = clean_drivers(drivers_raw)
    clean_aut  = clean_autos(autos_raw, driver_ids)
    clean_evt  = clean_events(events_raw)

    # Sauvegarder temporairement en Parquet pour XCom
    tmp = Path("/tmp/silver_tmp")
    tmp.mkdir(exist_ok=True)
    clean_drv.to_parquet(tmp / "clean_drivers.parquet",  index=False)
    clean_aut.to_parquet(tmp / "clean_autos.parquet",    index=False)
    clean_evt.to_parquet(tmp / "clean_events.parquet",   index=False)

    print(f"✅ Clean: drivers={len(clean_drv)}, autos={len(clean_aut)}, events={len(clean_evt)}")


def task_enrich(**ctx):
    """Étape 2 : Enrichissement — ajout des 13 colonnes dérivées."""
    import pandas as pd
    from src.processing.enrich_silver import enrich_events, enrich_autos, enrich_drivers

    tmp = Path("/tmp/silver_tmp")
    silver_drv = enrich_drivers(pd.read_parquet(tmp / "clean_drivers.parquet"))
    silver_aut = enrich_autos(pd.read_parquet(tmp / "clean_autos.parquet"))
    silver_evt = enrich_events(pd.read_parquet(tmp / "clean_events.parquet"))

    silver_drv.to_parquet(tmp / "enrich_drivers.parquet", index=False)
    silver_aut.to_parquet(tmp / "enrich_autos.parquet",   index=False)
    silver_evt.to_parquet(tmp / "enrich_events.parquet",  index=False)

    print(f"✅ Enrich: events={len(silver_evt)} cols={len(silver_evt.columns)}")


def task_join(**ctx):
    """Étape 3 : Jointure triple events ⋈ autos ⋈ drivers."""
    import pandas as pd
    from src.processing.join_silver import build_silver_joined

    tmp = Path("/tmp/silver_tmp")
    silver_drv = pd.read_parquet(tmp / "enrich_drivers.parquet")
    silver_aut = pd.read_parquet(tmp / "enrich_autos.parquet")
    silver_evt = pd.read_parquet(tmp / "enrich_events.parquet")

    silver_joined = build_silver_joined(silver_evt, silver_aut, silver_drv)
    silver_joined.to_parquet(tmp / "silver_joined.parquet", index=False)

    print(f"✅ Join: {len(silver_joined)} lignes × {len(silver_joined.columns)} colonnes")


def task_export(**ctx):
    """Étape 4 : Export final vers data/silver/."""
    import pandas as pd
    from src.processing.export_silver import export_silver

    tmp     = Path("/tmp/silver_tmp")
    silver  = Path(SILVER_DIR)

    results = export_silver(
        silver_events  = pd.read_parquet(tmp / "enrich_events.parquet"),
        silver_autos   = pd.read_parquet(tmp / "enrich_autos.parquet"),
        silver_drivers = pd.read_parquet(tmp / "enrich_drivers.parquet"),
        silver_joined  = pd.read_parquet(tmp / "silver_joined.parquet"),
        output_dir     = silver,
    )
    for name, meta in results.items():
        print(f"  {name}: {meta['rows']} lignes × {meta['cols']} cols")


def task_validate(**ctx):
    """Étape 5 : Contrôles qualité Silver."""
    import pandas as pd

    silver = Path(SILVER_DIR)
    errors = []

    # Charger
    events  = pd.read_parquet(silver / "silver_events.parquet")
    autos   = pd.read_parquet(silver / "silver_autos.parquet")
    joined  = pd.read_parquet(silver / "silver_fleet_joined.parquet")

    # Règle 1 : row count
    if len(events) != 6888:
        errors.append(f"Events: attendu 6888, obtenu {len(events)}")

    # Règle 2 : pas d'outlier odometer
    if events["odometer"].max() >= 500_000:
        errors.append(f"Odometer outlier toujours présent: {events['odometer'].max()}")

    # Règle 3 : pas de typo transmission
    if "AUtomatic" in autos["transmission_clean"].values:
        errors.append("Typo 'AUtomatic' toujours présente")

    # Règle 4 : speed_category valide
    valid_cats = {"stop", "slow", "normal", "fast", "overspeed"}
    actual_cats = set(events["speed_category"].unique())
    invalid = actual_cats - valid_cats
    if invalid:
        errors.append(f"speed_category invalide : {invalid}")

    # Règle 5 : shape joined
    if len(joined) != 6888:
        errors.append(f"Joined: attendu 6888 lignes, obtenu {len(joined)}")

    if errors:
        raise ValueError("Validation Silver ÉCHOUÉE:\n" + "\n".join(errors))

    print("✅ Validation Silver : tous les contrôles passés")
    print(f"   events  : {len(events):,} lignes × {len(events.columns)} cols")
    print(f"   autos   : {len(autos):,} lignes × {len(autos.columns)} cols")
    print(f"   joined  : {len(joined):,} lignes × {len(joined.columns)} cols")


# ── DAG definition ────────────────────────────────────────────────────────
with DAG(
    dag_id="dag_silver_pipeline",
    description="Pipeline Silver : nettoyage, enrichissement, jointures, export Parquet",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["silver", "pipeline", "fleet"],
) as dag:

    t1 = PythonOperator(task_id="clean_bronze",    python_callable=task_clean)
    t2 = PythonOperator(task_id="enrich_silver",   python_callable=task_enrich)
    t3 = PythonOperator(task_id="join_tables",     python_callable=task_join)
    t4 = PythonOperator(task_id="export_parquet",  python_callable=task_export)
    t5 = PythonOperator(task_id="validate_silver", python_callable=task_validate)

    t1 >> t2 >> t3 >> t4 >> t5
