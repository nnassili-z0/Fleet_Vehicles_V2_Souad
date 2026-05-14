"""
join_silver.py
--------------
Étape 3 du pipeline Silver.
Fusionne les 3 tables enrichies en une table maître :
  silver_fleet_joined = events ⋈ autos ⋈ drivers
  Résultat attendu : 6 888 lignes × 54 colonnes
"""

import pandas as pd
import logging

logger = logging.getLogger(__name__)


def build_silver_joined(
    events: pd.DataFrame,
    autos: pd.DataFrame,
    drivers: pd.DataFrame,
) -> pd.DataFrame:
    """
    Jointure triple LEFT :
      events ← autos  (on AutoID)
      résultat ← drivers (on OwnerID = PersonID)

    LEFT join pour conserver tous les events même si
    l'auto ou le conducteur n'est pas retrouvé.
    """

    # ── Jointure 1 : events ← autos ──────────────────────────────────────
    df = events.merge(
        autos,
        on="AutoID",
        how="left",
        suffixes=("", "_auto"),
    )
    logger.info(f"Après join events ⋈ autos : {len(df)} lignes × {len(df.columns)} colonnes")

    # ── Jointure 2 : résultat ← drivers ──────────────────────────────────
    # Renommer PersonID → OwnerID pour la clé de jointure
    drivers_renamed = drivers.rename(columns={"PersonID": "OwnerID"})

    df = df.merge(
        drivers_renamed,
        on="OwnerID",
        how="left",
        suffixes=("", "_driver"),
    )
    logger.info(f"Après join ⋈ drivers : {len(df)} lignes × {len(df.columns)} colonnes")

    # ── Vérification ─────────────────────────────────────────────────────
    assert len(df) == 6888, f"Attendu 6888 lignes, obtenu {len(df)}"
    logger.info(
        f"silver_fleet_joined : {len(df):,} lignes × {len(df.columns)} colonnes ✅"
    )

    return df.reset_index(drop=True)
