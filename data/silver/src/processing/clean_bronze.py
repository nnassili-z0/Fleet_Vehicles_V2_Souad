"""
clean_bronze.py
---------------
Étape 1 du pipeline Silver.
Nettoie les 3 tables sources (drivers, autos, events) :
  - Filtre l'outlier AutoID=1 (odometer=87M km)
  - Corrige le typo "AUtomatic" → "Automatic"
  - Convertit les types (bool strings, timestamps, floats)
  - Flagge les 6 véhicules sans conducteur connu
"""

import pandas as pd
import logging

logger = logging.getLogger(__name__)


def clean_events(df: pd.DataFrame) -> pd.DataFrame:
    """
    Nettoie la table events.
    Input  : 6 889 lignes × 24 colonnes (Bronze)
    Output : 6 888 lignes × 24 colonnes (1 outlier filtré)
    """
    initial = len(df)

    # ── 1. Filtrer outlier AutoID=1 (odometer = 87 654 321 km) ──────────
    df = df[df["odometer"].astype(float) < 500_000].copy()
    logger.info(f"Events outlier filter: {initial} → {len(df)} lignes "
                f"({initial - len(df)} filtrées)")

    # ── 2. Types numériques ───────────────────────────────────────────────
    numeric_cols = [
        "vehicle_speed", "fuel_level", "odometer",
        "fuel_consumed_since_restart", "engine_speed",
        "torque_at_transmission", "accelerator_pedal_position",
        "steering_wheel_angle", "latitude", "longitude",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # ── 3. Timestamp ──────────────────────────────────────────────────────
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # ── 4. Bool strings → bool ────────────────────────────────────────────
    bool_cols = [
        "brake_pedal_status", "high_beam_status",
        "windshield_wiper_status", "headlamp_status",
        "parking_brake_status",
    ]
    for col in bool_cols:
        df[col] = df[col].map({"true": True, "false": False})

    # ── 5. IDs en string ─────────────────────────────────────────────────
    df["AutoID"]  = df["AutoID"].astype(str)
    df["EventID"] = df["EventID"].astype(str)

    # ── 6. EventCategoryID en int ────────────────────────────────────────
    df["EventCategoryID"] = pd.to_numeric(df["EventCategoryID"], errors="coerce").astype("Int64")

    logger.info(f"clean_events: {len(df)} lignes × {len(df.columns)} colonnes")
    return df.reset_index(drop=True)


def clean_autos(df: pd.DataFrame, driver_ids: set) -> pd.DataFrame:
    """
    Nettoie la table autos.
    Input  : 2 500 lignes × 11 colonnes (Bronze)
    Output : 2 500 lignes × 12 colonnes (+ owner_known)
    """
    df = df.copy()

    # ── 1. Corriger typo "AUtomatic" → "Automatic" (1 cas) ───────────────
    typo_count = (df["Transmission"] == "AUtomatic").sum()
    df["Transmission"] = df["Transmission"].replace("AUtomatic", "Automatic")
    if typo_count:
        logger.info(f"Typo corrigée : {typo_count} × 'AUtomatic' → 'Automatic'")

    # ── 2. Types ──────────────────────────────────────────────────────────
    df["Year"]    = pd.to_numeric(df["Year"], errors="coerce").astype("Int64")
    df["AutoID"]  = df["AutoID"].astype(str)
    df["OwnerID"] = df["OwnerID"].astype(str)

    # ── 3. Flagger véhicules sans conducteur connu (6 cas) ───────────────
    df["owner_known"] = df["OwnerID"].isin(driver_ids)
    unknown = (~df["owner_known"]).sum()
    if unknown:
        logger.warning(f"{unknown} véhicule(s) avec OwnerID sans conducteur → owner_known=False")

    logger.info(f"clean_autos: {len(df)} lignes × {len(df.columns)} colonnes")
    return df.reset_index(drop=True)


def clean_drivers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Nettoie la table drivers.
    Input  : 998 lignes × 8 colonnes (Bronze) — aucune anomalie détectée
    Output : 998 lignes × 8 colonnes
    """
    df = df.copy()

    # ── 1. DateOfBirth en datetime ────────────────────────────────────────
    df["DateOfBirth"] = pd.to_datetime(df["DateOfBirth"], errors="coerce")

    # ── 2. ID en string ───────────────────────────────────────────────────
    df["PersonID"] = df["PersonID"].astype(str)

    logger.info(f"clean_drivers: {len(df)} lignes × {len(df.columns)} colonnes")
    return df.reset_index(drop=True)
