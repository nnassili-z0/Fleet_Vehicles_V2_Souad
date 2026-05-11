"""
silver_pipeline.py
------------------
Point d'entrée principal du pipeline Silver.
Orchestre les 4 étapes dans l'ordre :
  1. clean_bronze
  2. enrich_silver
  3. join_silver
  4. export_silver

Usage :
    python silver_pipeline.py
    python silver_pipeline.py --bronze-dir data/bronze --silver-dir data/silver
"""

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

# ── Logging setup ────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("silver_pipeline")


def run_pipeline(bronze_dir: str = "data/bronze", silver_dir: str = "data/silver") -> dict:
    """
    Exécute le pipeline Silver complet.
    Retourne les métadonnées des fichiers exportés.
    """
    from src.processing.clean_bronze  import clean_events, clean_autos, clean_drivers
    from src.processing.enrich_silver import enrich_events, enrich_autos, enrich_drivers
    from src.processing.join_silver   import build_silver_joined
    from src.processing.export_silver import export_silver

    bronze = Path(bronze_dir)
    silver = Path(silver_dir)

    # ──────────────────────────────────────────────────────────────────────
    # Chargement Bronze
    # ──────────────────────────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("CHARGEMENT BRONZE")
    logger.info("=" * 60)

    drivers_raw = pd.read_csv(bronze / "drivers.csv")
    autos_raw   = pd.read_csv(bronze / "autos.csv")
    events_raw  = pd.read_csv(bronze / "events.csv")

    logger.info(f"drivers.csv  : {len(drivers_raw):,} lignes × {len(drivers_raw.columns)} cols")
    logger.info(f"autos.csv    : {len(autos_raw):,} lignes × {len(autos_raw.columns)} cols")
    logger.info(f"events.csv   : {len(events_raw):,} lignes × {len(events_raw.columns)} cols")

    # ──────────────────────────────────────────────────────────────────────
    # Étape 1 — Nettoyage
    # ──────────────────────────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("ÉTAPE 1 — NETTOYAGE")
    logger.info("=" * 60)

    driver_ids     = set(drivers_raw["PersonID"].astype(str))
    clean_drv      = clean_drivers(drivers_raw)
    clean_auto     = clean_autos(autos_raw, driver_ids)
    clean_evt      = clean_events(events_raw)

    # ──────────────────────────────────────────────────────────────────────
    # Étape 2 — Enrichissement
    # ──────────────────────────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("ÉTAPE 2 — ENRICHISSEMENT")
    logger.info("=" * 60)

    silver_drivers = enrich_drivers(clean_drv)
    silver_autos   = enrich_autos(clean_auto)
    silver_events  = enrich_events(clean_evt)

    # ──────────────────────────────────────────────────────────────────────
    # Étape 3 — Jointures
    # ──────────────────────────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("ÉTAPE 3 — JOINTURES")
    logger.info("=" * 60)

    silver_joined = build_silver_joined(silver_events, silver_autos, silver_drivers)

    # ──────────────────────────────────────────────────────────────────────
    # Étape 4 — Export Parquet
    # ──────────────────────────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("ÉTAPE 4 — EXPORT PARQUET")
    logger.info("=" * 60)

    results = export_silver(silver_events, silver_autos, silver_drivers, silver_joined, silver)

    # ──────────────────────────────────────────────────────────────────────
    # Résumé
    # ──────────────────────────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("PIPELINE SILVER TERMINÉ ✅")
    logger.info("=" * 60)
    for name, meta in results.items():
        logger.info(f"  {name:<28} {meta['rows']:>6,} lignes × {meta['cols']:>2} cols  → {meta['path']}")

    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pipeline Silver — Flotte Véhicules")
    parser.add_argument("--bronze-dir", default="data/bronze", help="Dossier Bronze")
    parser.add_argument("--silver-dir", default="data/silver", help="Dossier Silver")
    args = parser.parse_args()

    try:
        run_pipeline(args.bronze_dir, args.silver_dir)
    except Exception as e:
        logger.error(f"Pipeline échoué : {e}")
        raise
