"""
compute_fleet_kpis.py
---------------------
Calcule les KPIs de performance par véhicule depuis silver_fleet_joined.

Input  : DataFrame silver_fleet_joined (6 888 × 56)
Output : DataFrame fleet_kpis (5 lignes × 13 cols)

KPIs calculés :
  - avg_speed, max_speed        ← vehicle_speed
  - avg_fuel_level, min_fuel    ← fuel_level
  - total_distance_km           ← odometer (max - min)
  - total_events                ← count
  - driving_ratio, idle_ratio   ← driving_mode
  - maintenance_due             ← EventCategoryID == 1
"""

import pandas as pd
import numpy as np
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

SERVICE_INTERVAL_KM = 16_000    # ~10 000 miles → déclenchement alerte maintenance


def compute_fleet_kpis(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrège les événements Silver par véhicule (AutoID) en KPIs.

    Args:
        df: silver_fleet_joined DataFrame

    Returns:
        fleet_kpis DataFrame — une ligne par véhicule actif
    """
    kpis = []

    for auto_id, grp in df.groupby("AutoID"):
        odo = grp["odometer"].dropna()

        kpi = {
            "auto_id"          : str(auto_id),
            "computed_date"    : pd.Timestamp.today().date(),
            "model"            : grp["Model"].iloc[0] if "Model" in grp.columns else "Unknown",
            "engine_type"      : grp["EngineType"].iloc[0] if "EngineType" in grp.columns else "",

            # ── Vitesse
            "avg_speed"        : round(float(grp["vehicle_speed"].mean()), 2),
            "max_speed"        : round(float(grp["vehicle_speed"].max()), 2),

            # ── Carburant
            "avg_fuel_level"   : round(float(grp["fuel_level"].mean()), 2),
            "min_fuel_level"   : round(float(grp["fuel_level"].min()), 2),

            # ── Distance = delta odometer
            "total_distance_km": round(float(odo.max() - odo.min()), 2) if len(odo) > 1 else 0.0,

            # ── Activité
            "total_events"     : int(len(grp)),
            "driving_ratio"    : round(float((grp["driving_mode"] == "driving").mean()), 4),
            "idle_ratio"       : round(float((grp["driving_mode"] == "idle").mean()), 4),

            # ── Maintenance (EventCategoryID = 1)
            "maintenance_due"  : bool((grp["EventCategoryID"] == 1).any()),
        }
        kpis.append(kpi)
        logger.info(
            f"KPI AutoID={auto_id} | {kpi['model']} | "
            f"avg_speed={kpi['avg_speed']} | dist={kpi['total_distance_km']} km | "
            f"events={kpi['total_events']}"
        )

    result = pd.DataFrame(kpis)
    logger.info(f"fleet_kpis: {len(result)} rows x {len(result.columns)} cols")
    return result
