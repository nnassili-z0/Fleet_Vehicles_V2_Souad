"""
score_drivers.py
----------------
Calcule le score de sécurité (0-100) par conducteur.

Formule :
  safety_score = (speed_score × 50%) + (brake_score × 50%)

  speed_score  = 100 - (nb_overspeed / total_events) × 1000
  brake_score  = 100 - (nb_brake_events / total_events) × 50

Input  : DataFrame silver_fleet_joined
Output : DataFrame driver_scores — une ligne par conducteur actif
"""

import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)


def compute_driver_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule les scores de sécurité conducteurs depuis silver_fleet_joined.

    Args:
        df: silver_fleet_joined DataFrame

    Returns:
        driver_scores DataFrame trié par safety_rank (meilleur en premier)
    """
    # Normaliser les colonnes booléennes (peuvent être string ou bool)
    for col in ["brake_pedal_status", "high_beam_status", "windshield_wiper_status"]:
        if col in df.columns:
            df[col] = df[col].map(
                {"True": True, "False": False, True: True, False: False, "true": True, "false": False}
            )

    scores = []

    for auto_id, grp in df.groupby("AutoID"):
        n = len(grp)

        # ── Composante vitesse (50%) ──────────────────────────────────
        # Pénalité : 10 pts pour chaque 1% d'événements en excès de vitesse
        overspeed_count = int((grp["vehicle_speed"] > 120).sum())
        speed_score = max(0.0, 100 - (overspeed_count / n) * 1000)

        # ── Composante freinage (50%) ─────────────────────────────────
        # Pénalité : 0.5 pt par % de temps en freinage
        brake_count = int((grp["brake_pedal_status"] == True).sum())
        brake_score = max(0.0, 100 - (brake_count / n) * 50)

        # ── Score final ───────────────────────────────────────────────
        safety_score = round((speed_score * 0.5) + (brake_score * 0.5), 1)

        # ── Métriques additionnelles ──────────────────────────────────
        high_beam_rate = float((grp["high_beam_status"] == True).mean()) \
                         if "high_beam_status" in grp.columns else 0.0
        wiper_rate = float((grp["windshield_wiper_status"] == True).mean()) \
                     if "windshield_wiper_status" in grp.columns else 0.0

        scores.append({
            "auto_id"         : str(auto_id),
            "driver_id"       : str(grp["OwnerID"].iloc[0]) if "OwnerID" in grp.columns else "",
            "driver_name"     : grp["FullName"].iloc[0] if "FullName" in grp.columns else "Unknown",
            "driver_age_group": grp["driver_age_group"].iloc[0] if "driver_age_group" in grp.columns else "",
            "computed_date"   : pd.Timestamp.today().date(),
            "safety_score"    : safety_score,
            "speed_score"     : round(speed_score, 1),
            "brake_score"     : round(brake_score, 1),
            "overspeed_count" : overspeed_count,
            "brake_events"    : brake_count,
            "high_beam_rate"  : round(high_beam_rate, 4),
            "wiper_rate"      : round(wiper_rate, 4),
            "total_events"    : n,
        })

        logger.info(
            f"Score AutoID={auto_id} | {grp['FullName'].iloc[0] if 'FullName' in grp.columns else '?'} | "
            f"safety={safety_score}/100 | overspeeds={overspeed_count} | brakes={brake_count}"
        )

    result = pd.DataFrame(scores)

    # Classement (1 = meilleur)
    result["safety_rank"] = result["safety_score"].rank(
        ascending=False, method="min"
    ).astype(int)

    return result.sort_values("safety_rank").reset_index(drop=True)
