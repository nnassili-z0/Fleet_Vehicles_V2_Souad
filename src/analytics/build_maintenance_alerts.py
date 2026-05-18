"""
build_maintenance_alerts.py
---------------------------
Génère les alertes maintenance depuis silver_fleet_joined.

Deux sources d'alertes :
  1. Kilométrage proche du prochain entretien (< 2 000 km du seuil)
  2. Événements EventCategoryID = 1 (maintenance event explicite)

Input  : DataFrame silver_fleet_joined
Output : DataFrame maintenance_alerts
"""

import pandas as pd
import logging

logger = logging.getLogger(__name__)

SERVICE_INTERVAL_KM = 16_000   # ~10 000 miles
ALERT_THRESHOLD_KM  = 2_000    # Alerter quand il reste < 2 000 km


def build_maintenance_alerts(df: pd.DataFrame) -> pd.DataFrame:
    """
    Génère les alertes maintenance.

    Args:
        df: silver_fleet_joined DataFrame

    Returns:
        maintenance_alerts DataFrame
    """
    alerts = []

    for auto_id, grp in df.groupby("AutoID"):
        odo     = grp["odometer"].dropna()
        max_odo = float(odo.max()) if len(odo) > 0 else 0.0
        model   = grp["Model"].iloc[0] if "Model" in grp.columns else "Unknown"

        # Prochain entretien
        next_service = (max_odo // SERVICE_INTERVAL_KM + 1) * SERVICE_INTERVAL_KM
        km_to_service = next_service - max_odo

        # ── Alerte kilométrage ─────────────────────────────────────
        if km_to_service < ALERT_THRESHOLD_KM:
            alerts.append({
                "auto_id"          : str(auto_id),
                "model"            : model,
                "alert_type"       : "mileage",
                "message"          : f"Service due in {km_to_service:.0f} km (next at {next_service:.0f} km)",
                "current_odometer" : round(max_odo, 2),
                "next_service_km"  : round(next_service, 2),
                "status"           : "open",
            })
            logger.info(
                f"Mileage alert AutoID={auto_id} | {model} | "
                f"service in {km_to_service:.0f} km"
            )

        # ── Alerte événement maintenance (EventCategoryID=1) ───────
        maint_events = grp[grp["EventCategoryID"] == 1]
        for _, ev in maint_events.iterrows():
            msg = str(ev.get("EventMessage", "Maintenance required"))
            alerts.append({
                "auto_id"          : str(auto_id),
                "model"            : model,
                "alert_type"       : "event",
                "message"          : msg,
                "current_odometer" : round(float(ev["odometer"]), 2),
                "next_service_km"  : round(next_service, 2),
                "status"           : "open",
            })
            logger.info(f"Event alert AutoID={auto_id} | {msg}")

    if not alerts:
        logger.info("No maintenance alerts generated")
        return pd.DataFrame(columns=[
            "auto_id", "model", "alert_type", "message",
            "current_odometer", "next_service_km", "status", "detected_at"
        ])

    result = pd.DataFrame(alerts)
    result["detected_at"] = pd.Timestamp.now()
    logger.info(f"maintenance_alerts: {len(result)} rows")
    return result
