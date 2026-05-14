"""
export_silver.py
----------------
Étape 4 du pipeline Silver.
Exporte les 4 tables Silver en Parquet compressé (snappy)
dans data/silver/.
"""

import pandas as pd
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

SILVER_DIR = Path("data/silver")


def export_silver(
    silver_events: pd.DataFrame,
    silver_autos: pd.DataFrame,
    silver_drivers: pd.DataFrame,
    silver_joined: pd.DataFrame,
    output_dir: Path = SILVER_DIR,
) -> dict:
    """
    Exporte les 4 DataFrames Silver en Parquet snappy.
    Retourne un dict avec les chemins et tailles des fichiers créés.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    files = {
        "silver_events":       (silver_events,  output_dir / "silver_events.parquet"),
        "silver_autos":        (silver_autos,   output_dir / "silver_autos.parquet"),
        "silver_drivers":      (silver_drivers, output_dir / "silver_drivers.parquet"),
        "silver_fleet_joined": (silver_joined,  output_dir / "silver_fleet_joined.parquet"),
    }

    results = {}
    for name, (df, path) in files.items():
        df.to_parquet(path, index=False, compression="snappy")
        size_kb = path.stat().st_size / 1024
        results[name] = {"path": str(path), "rows": len(df), "cols": len(df.columns), "size_kb": round(size_kb, 1)}
        logger.info(f"  ✅ {path.name:<40} {len(df):>6,} lignes × {len(df.columns):>2} cols  ({size_kb:.1f} KB)")

    logger.info("Export Silver terminé — 4 fichiers Parquet créés")
    return results


def load_silver(table: str, silver_dir: Path = SILVER_DIR) -> pd.DataFrame:
    """Helper : charge une table Silver depuis Parquet."""
    path = silver_dir / f"{table}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Table Silver introuvable : {path}")
    return pd.read_parquet(path)
