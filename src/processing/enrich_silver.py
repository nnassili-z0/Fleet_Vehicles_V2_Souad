"""
enrich_silver.py
----------------
Étape 2 du pipeline Silver.
Ajoute 13 colonnes dérivées aux 3 tables nettoyées.

Events (8 colonnes) :
  speed_category, driving_mode, fuel_alert, is_braking,
  is_accelerating, event_date, hour_of_day, day_of_week

Autos (4 colonnes) :
  vehicle_age_years, is_electric, is_hybrid, transmission_clean

Drivers (2 colonnes) :
  driver_age, driver_age_group
"""

import pandas as pd
import numpy as np
import logging
from datetime import date

logger = logging.getLogger(__name__)

REFERENCE_YEAR = 2024


def enrich_events(df: pd.DataFrame) -> pd.DataFrame:
    """Ajoute 8 colonnes dérivées aux événements."""
    df = df.copy()

    # ── 1. Catégorie de vitesse ───────────────────────────────────────────
    # Distribution réelle : stop=2751, slow=2604, normal=1533, overspeed=1
    df["speed_category"] = pd.cut(
        df["vehicle_speed"],
        bins=[-1, 0, 30, 90, 120, float("inf")],
        labels=["stop", "slow", "normal", "fast", "overspeed"],
        right=True,
    ).astype(str)

    # ── 2. Mode de conduite ───────────────────────────────────────────────
    # Distribution réelle : parked=2751, driving=4138
    conditions = [
        (df["vehicle_speed"] == 0) & (df["parking_brake_status"] == True),
        (df["vehicle_speed"] == 0),
    ]
    choices = ["parked", "idle"]
    df["driving_mode"] = np.select(conditions, choices, default="driving")

    # ── 3. Alertes booléennes ─────────────────────────────────────────────
    df["fuel_alert"]      = df["fuel_level"] < 20          # Aucun dans dataset actuel (min=75.1%)
    df["is_braking"]      = df["brake_pedal_status"] == True    # 3 822 events (55%)
    df["is_accelerating"] = df["accelerator_pedal_position"] > 30

    # ── 4. Extraction temporelle ──────────────────────────────────────────
    # Timestamps : 2017-05-11 → 2018-01-03
    df["event_date"]  = df["timestamp"].dt.date
    df["hour_of_day"] = df["timestamp"].dt.hour    # Distribution : 22h=3507, 19h=1617, 23h=1176
    df["day_of_week"] = df["timestamp"].dt.day_name()

    new_cols = ["speed_category", "driving_mode", "fuel_alert",
                "is_braking", "is_accelerating", "event_date",
                "hour_of_day", "day_of_week"]
    logger.info(f"enrich_events: {len(new_cols)} colonnes ajoutées → "
                f"{len(df)} lignes × {len(df.columns)} colonnes")
    return df


def enrich_autos(df: pd.DataFrame) -> pd.DataFrame:
    """Ajoute 4 colonnes dérivées aux véhicules."""
    df = df.copy()

    # ── 1. Âge du véhicule ───────────────────────────────────────────────
    df["vehicle_age_years"] = REFERENCE_YEAR - df["Year"]

    # ── 2. Type motorisation ─────────────────────────────────────────────
    # Electric=230, Hybrid=458, thermal=1812
    df["is_electric"] = df["EngineType"] == "Electric"
    df["is_hybrid"]   = df["EngineType"] == "Hybrid"

    # ── 3. Transmission normalisée (alias propre) ─────────────────────────
    df["transmission_clean"] = df["Transmission"].replace("AUtomatic", "Automatic")

    new_cols = ["vehicle_age_years", "is_electric", "is_hybrid", "transmission_clean"]
    logger.info(f"enrich_autos: {len(new_cols)} colonnes ajoutées → "
                f"{len(df)} lignes × {len(df.columns)} colonnes")
    return df


def enrich_drivers(df: pd.DataFrame) -> pd.DataFrame:
    """Ajoute 2 colonnes dérivées aux conducteurs."""
    df = df.copy()

    # ── 1. Âge ───────────────────────────────────────────────────────────
    # Plage réelle : 26–74 ans, moyenne 50
    df["driver_age"] = REFERENCE_YEAR - df["DateOfBirth"].dt.year

    # ── 2. Tranche d'âge ─────────────────────────────────────────────────
    # Distribution réelle : <30=80, 30-45=284, 45-60=349, 60+=285
    df["driver_age_group"] = pd.cut(
        df["driver_age"],
        bins=[0, 30, 45, 60, 120],
        labels=["<30", "30-45", "45-60", "60+"],
        right=False,
    ).astype(str)

    new_cols = ["driver_age", "driver_age_group"]
    logger.info(f"enrich_drivers: {len(new_cols)} colonnes ajoutées → "
                f"{len(df)} lignes × {len(df.columns)} colonnes")
    return df
