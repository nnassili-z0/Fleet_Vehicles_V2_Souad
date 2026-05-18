"""
export_gold.py
--------------
Exporte les 4 tables Gold en CSV (et Parquet si pyarrow disponible).
Fallback PostgreSQL via SQLAlchemy si disponible.

Output files in data/gold/ :
  - fleet_kpis.csv / .parquet
  - driver_scores.csv / .parquet
  - anomalies.csv / .parquet
  - maintenance_alerts.csv / .parquet
"""

import pandas as pd
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

GOLD_DIR = Path("data/gold")


def export_gold(
    fleet_kpis: pd.DataFrame,
    driver_scores: pd.DataFrame,
    anomalies: pd.DataFrame,
    maintenance_alerts: pd.DataFrame,
    output_dir: Path = GOLD_DIR,
) -> dict:
    """
    Exporte les 4 tables Gold.

    Returns:
        dict avec les métadonnées de chaque export
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    tables = {
        "fleet_kpis"          : fleet_kpis,
        "driver_scores"       : driver_scores,
        "anomalies"           : anomalies,
        "maintenance_alerts"  : maintenance_alerts,
    }

    results = {}
    try:
        import pyarrow  # noqa
        fmt = "parquet"
    except ImportError:
        fmt = "csv"

    for name, df in tables.items():
        if fmt == "parquet":
            path = output_dir / f"{name}.parquet"
            df.to_parquet(path, index=False, compression="snappy")
        else:
            path = output_dir / f"{name}.csv"
            df.to_csv(path, index=False)

        size_kb = path.stat().st_size / 1024
        results[name] = {
            "path"   : str(path),
            "rows"   : len(df),
            "cols"   : len(df.columns),
            "size_kb": round(size_kb, 1),
            "format" : fmt,
        }
        logger.info(
            f"  ✅ {path.name:<35} {len(df):>5} rows × {len(df.columns):>2} cols  ({size_kb:.1f} KB)"
        )

    logger.info(f"Gold export complete ({fmt}) → {output_dir}/")
    return results


def write_to_postgres(
    df: pd.DataFrame,
    table_name: str,
    db_url: str,
    schema: str = "gold",
    if_exists: str = "replace",
) -> None:
    """
    Écrit un DataFrame dans PostgreSQL schéma gold.
    Appelé uniquement si sqlalchemy + psycopg2 disponibles.
    """
    try:
        from sqlalchemy import create_engine, text
        engine = create_engine(db_url)

        # Créer le schema gold s'il n'existe pas
        with engine.connect() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
            conn.commit()

        df.to_sql(
            name      = table_name,
            con       = engine,
            schema    = schema,
            if_exists = if_exists,
            index     = False,
            method    = "multi",
            chunksize = 500,
        )
        logger.info(f"✅ PostgreSQL gold.{table_name} → {len(df)} rows")

    except Exception as e:
        logger.warning(f"PostgreSQL write skipped ({e}) — CSV/Parquet used instead")
