"""
gold_pipeline.py
----------------
Point d'entrée principal du pipeline Gold.
Lit silver_fleet_joined et produit les 4 tables Gold.

Usage:
    python gold_pipeline.py
    python gold_pipeline.py --silver-dir data/silver --gold-dir data/gold
"""

import sys
import logging
import argparse
from pathlib import Path

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("gold_pipeline")


def run_pipeline(
    silver_dir: str = "data/silver",
    gold_dir: str   = "data/gold",
    db_url: str     = None,
) -> dict:
    """
    Exécute le pipeline Gold complet.

    Steps:
      1. Load silver_fleet_joined
      2. Compute fleet KPIs
      3. Score drivers
      4. Detect anomalies (IsolationForest)
      5. Build maintenance alerts
      6. Export to Gold (CSV/Parquet + optional PostgreSQL)

    Returns:
        dict with export metadata
    """
    sys.path.insert(0, str(Path(__file__).parent))

    from src.analytics.compute_fleet_kpis      import compute_fleet_kpis
    from src.analytics.score_drivers            import compute_driver_scores
    from src.analytics.detect_anomalies         import detect_anomalies
    from src.analytics.build_maintenance_alerts import build_maintenance_alerts
    from src.analytics.export_gold              import export_gold, write_to_postgres

    silver = Path(silver_dir)
    gold   = Path(gold_dir)

    # ──────────────────────────────────────────────────────────────────
    # STEP 1 — Load Silver
    # ──────────────────────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("STEP 1 — LOADING SILVER")
    logger.info("=" * 60)

    try:
        df = pd.read_parquet(silver / "silver_fleet_joined.parquet")
    except Exception:
        df = pd.read_csv(silver / "silver_fleet_joined.csv", parse_dates=["timestamp"])

    # Normalize bool columns
    for col in ["brake_pedal_status", "high_beam_status",
                "windshield_wiper_status", "parking_brake_status"]:
        if col in df.columns:
            df[col] = df[col].map(
                {"True": True, "False": False, True: True, False: False,
                 "true": True, "false": False}
            )

    logger.info(f"Silver loaded: {len(df):,} rows × {len(df.columns)} cols")
    logger.info(f"Active vehicles: {df['AutoID'].nunique()}")

    # ──────────────────────────────────────────────────────────────────
    # STEP 2 — Fleet KPIs
    # ──────────────────────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("STEP 2 — FLEET KPIs")
    logger.info("=" * 60)
    fleet_kpis = compute_fleet_kpis(df)

    # ──────────────────────────────────────────────────────────────────
    # STEP 3 — Driver Scores
    # ──────────────────────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("STEP 3 — DRIVER SCORES")
    logger.info("=" * 60)
    driver_scores = compute_driver_scores(df.copy())

    # ──────────────────────────────────────────────────────────────────
    # STEP 4 — Anomaly Detection
    # ──────────────────────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("STEP 4 — ANOMALY DETECTION (IsolationForest)")
    logger.info("=" * 60)
    anomalies = detect_anomalies(df.copy())

    # ──────────────────────────────────────────────────────────────────
    # STEP 5 — Maintenance Alerts
    # ──────────────────────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("STEP 5 — MAINTENANCE ALERTS")
    logger.info("=" * 60)
    maintenance_alerts = build_maintenance_alerts(df)

    # ──────────────────────────────────────────────────────────────────
    # STEP 6 — Export
    # ──────────────────────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("STEP 6 — EXPORT GOLD")
    logger.info("=" * 60)
    results = export_gold(fleet_kpis, driver_scores, anomalies, maintenance_alerts, gold)

    # Optional: write to PostgreSQL
    if db_url:
        logger.info("Writing to PostgreSQL...")
        for table, df_out in [
            ("fleet_kpis",         fleet_kpis),
            ("driver_scores",      driver_scores),
            ("anomalies",          anomalies),
            ("maintenance_alerts", maintenance_alerts),
        ]:
            write_to_postgres(df_out, table, db_url)

    # ──────────────────────────────────────────────────────────────────
    # SUMMARY
    # ──────────────────────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("GOLD PIPELINE COMPLETE ✅")
    logger.info("=" * 60)
    for name, meta in results.items():
        logger.info(
            f"  {name:<25} {meta['rows']:>5} rows × {meta['cols']:>2} cols  → {meta['path']}"
        )

    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Gold Pipeline — Flotte Véhicules")
    parser.add_argument("--silver-dir", default="data/silver")
    parser.add_argument("--gold-dir",   default="data/gold")
    parser.add_argument("--db-url",     default=None,
                        help="PostgreSQL URL (optional)")
    args = parser.parse_args()

    try:
        run_pipeline(args.silver_dir, args.gold_dir, args.db_url)
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise
