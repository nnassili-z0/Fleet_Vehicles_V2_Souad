"""
detect_anomalies.py
-------------------
Détecte les anomalies télémétriques avec IsolationForest (Scikit-learn).

Features utilisées :
  - vehicle_speed              (vitesse km/h)
  - fuel_consumed_since_restart (consommation L)
  - engine_speed               (RPM moteur)
  - torque_at_transmission     (couple Nm)

Sévérité H/M/L :
  H = vitesse > 120 km/h OU fuel < 15%
  M = RPM > 4000
  L = anomalie statistique

Input  : DataFrame silver_fleet_joined
Output : DataFrame anomalies — ~336 lignes (5% de 6 888)
"""

import pandas as pd
import numpy as np
import logging
from sklearn.ensemble import IsolationForest

logger = logging.getLogger(__name__)

FEATURES = [
    "vehicle_speed",
    "fuel_consumed_since_restart",
    "engine_speed",
    "torque_at_transmission",
]


def _classify_anomaly(row) -> tuple:
    """Classifie le type et la sévérité d'une anomalie."""
    if row["vehicle_speed"] > 120:
        return ("overspeed", "H")
    if row.get("fuel_level", 100) < 15:
        return ("low_fuel", "H")
    if row["engine_speed"] > 4000:
        return ("high_rpm", "M")
    return ("statistical", "L")


def detect_anomalies(
    df: pd.DataFrame,
    contamination: float = 0.05,
    random_state: int = 42,
) -> pd.DataFrame:
    """
    Applique IsolationForest sur les 4 features télémétriques.

    Args:
        df           : silver_fleet_joined DataFrame
        contamination: proportion attendue d'anomalies (défaut 5%)
        random_state : seed pour la reproductibilité

    Returns:
        DataFrame des anomalies détectées avec type et sévérité
    """
    df = df.copy()

    # Préparer les features
    X = df[FEATURES].fillna(0)

    # Entraîner et prédire
    model = IsolationForest(
        contamination=contamination,
        random_state=random_state,
        n_estimators=100,
    )
    df["anomaly_flag"]  = model.fit_predict(X)   # -1 = anomalie, 1 = normal
    df["anomaly_score"] = model.score_samples(X) # plus négatif = plus anormal

    # Filtrer uniquement les anomalies
    anomalies = df[df["anomaly_flag"] == -1].copy()

    # Classifier type + sévérité
    anomalies[["anomaly_type", "severity"]] = anomalies.apply(
        _classify_anomaly, axis=1, result_type="expand"
    )
    anomalies["detected_at"] = pd.Timestamp.now()

    # Sélectionner les colonnes de sortie
    output_cols = [
        "EventID", "AutoID", "timestamp", "detected_at",
        "anomaly_type", "severity", "anomaly_score",
        "vehicle_speed", "fuel_level", "engine_speed",
        "torque_at_transmission", "fuel_consumed_since_restart",
    ]
    # Garder seulement les colonnes qui existent
    output_cols = [c for c in output_cols if c in anomalies.columns]
    result = anomalies[output_cols].reset_index(drop=True)

    # Log summary
    from collections import Counter
    type_dist = dict(Counter(result["anomaly_type"]))
    sev_dist  = dict(Counter(result["severity"]))
    logger.info(f"Anomalies détectées : {len(result)} ({contamination*100:.0f}% de {len(df)})")
    logger.info(f"  Types    : {type_dist}")
    logger.info(f"  Sévérité : {sev_dist}")

    return result
