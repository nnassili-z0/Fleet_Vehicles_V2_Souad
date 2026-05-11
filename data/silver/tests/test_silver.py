"""
test_silver.py
--------------
Tests pytest pour valider la couche Silver.
Couvre : row counts, types, outliers, enrichissement, jointures.

Lancer : python -m pytest tests/test_silver.py -v
"""

import sys
from pathlib import Path
import pandas as pd
import pytest

# Ajouter le dossier racine au path pour les imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.processing.clean_bronze  import clean_events, clean_autos, clean_drivers
from src.processing.enrich_silver import enrich_events, enrich_autos, enrich_drivers
from src.processing.join_silver   import build_silver_joined


# ── Fixtures ──────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def bronze_path():
    return Path("data/bronze")

@pytest.fixture(scope="session")
def drivers_raw(bronze_path):
    return pd.read_csv(bronze_path / "drivers.csv")

@pytest.fixture(scope="session")
def autos_raw(bronze_path):
    return pd.read_csv(bronze_path / "autos.csv")

@pytest.fixture(scope="session")
def events_raw(bronze_path):
    return pd.read_csv(bronze_path / "events.csv")

@pytest.fixture(scope="session")
def silver_drivers(drivers_raw):
    return enrich_drivers(clean_drivers(drivers_raw))

@pytest.fixture(scope="session")
def silver_autos(autos_raw, silver_drivers):
    driver_ids = set(silver_drivers["OwnerID"] if "OwnerID" in silver_drivers.columns
                     else silver_drivers.index.astype(str))
    # Reconstruire driver_ids depuis raw
    driver_ids = set(pd.read_csv("data/bronze/drivers.csv")["PersonID"].astype(str))
    return enrich_autos(clean_autos(autos_raw, driver_ids))

@pytest.fixture(scope="session")
def silver_events(events_raw):
    return enrich_events(clean_events(events_raw))

@pytest.fixture(scope="session")
def silver_joined(silver_events, silver_autos, silver_drivers):
    return build_silver_joined(silver_events, silver_autos, silver_drivers)


# ── Tests Bronze → Clean ──────────────────────────────────────────────────

class TestCleanBronze:

    def test_events_outlier_filtered(self, silver_events):
        """AutoID=1 (odometer=87M km) doit être filtré."""
        assert len(silver_events) == 6888, \
            f"Attendu 6888 lignes, obtenu {len(silver_events)}"

    def test_events_no_odometer_outlier(self, silver_events):
        """Plus aucun odometer >= 500 000 km."""
        assert silver_events["odometer"].max() < 500_000

    def test_events_no_speed_outlier(self, silver_events):
        """Vitesse max après filtre <= 120 km/h."""
        assert silver_events["vehicle_speed"].max() <= 120

    def test_autos_typo_corrected(self, silver_autos):
        """'AUtomatic' doit être corrigé en 'Automatic'."""
        assert "AUtomatic" not in silver_autos["Transmission"].values
        assert "AUtomatic" not in silver_autos["transmission_clean"].values

    def test_autos_owner_known_flag(self, silver_autos):
        """6 véhicules sans conducteur connu → owner_known=False."""
        unknown = (~silver_autos["owner_known"]).sum()
        assert unknown == 6, f"Attendu 6 véhicules owner_known=False, obtenu {unknown}"

    def test_drivers_dob_parsed(self, silver_drivers):
        """DateOfBirth doit être datetime."""
        assert pd.api.types.is_datetime64_any_dtype(silver_drivers["DateOfBirth"])

    def test_events_timestamp_parsed(self, silver_events):
        """timestamp doit être datetime."""
        assert pd.api.types.is_datetime64_any_dtype(silver_events["timestamp"])

    def test_events_bool_cols(self, silver_events):
        """Les colonnes bool doivent être True/False (pas 'true'/'false')."""
        assert silver_events["brake_pedal_status"].dtype == bool or \
               silver_events["brake_pedal_status"].isin([True, False]).all()


# ── Tests Enrichissement ──────────────────────────────────────────────────

class TestEnrichSilver:

    def test_speed_category_values(self, silver_events):
        """speed_category ∈ {stop, slow, normal, fast, overspeed}."""
        valid = {"stop", "slow", "normal", "fast", "overspeed"}
        actual = set(silver_events["speed_category"].dropna().unique())
        assert actual <= valid, f"Valeurs inattendues : {actual - valid}"

    def test_speed_category_distribution(self, silver_events):
        """Distribution réelle : stop=2751, slow=2604, normal=1533."""
        counts = silver_events["speed_category"].value_counts()
        assert counts.get("stop", 0)   == 2751
        assert counts.get("slow", 0)   == 2604
        assert counts.get("normal", 0) == 1533

    def test_driving_mode_values(self, silver_events):
        """driving_mode ∈ {parked, idle, driving}."""
        valid = {"parked", "idle", "driving"}
        actual = set(silver_events["driving_mode"].dropna().unique())
        assert actual <= valid

    def test_driving_mode_distribution(self, silver_events):
        """Distribution réelle : parked=2751, driving=4138."""
        counts = silver_events["driving_mode"].value_counts()
        assert counts.get("parked",  0) == 2751
        assert counts.get("driving", 0) == 4138

    def test_event_date_extracted(self, silver_events):
        """event_date doit exister et être de type date."""
        assert "event_date" in silver_events.columns
        assert silver_events["event_date"].notna().all()

    def test_hour_of_day_range(self, silver_events):
        """hour_of_day ∈ [0, 23]."""
        assert silver_events["hour_of_day"].between(0, 23).all()

    def test_vehicle_age_positive(self, silver_autos):
        """vehicle_age_years doit être positif."""
        assert (silver_autos["vehicle_age_years"] >= 0).all()

    def test_is_electric_count(self, silver_autos):
        """230 véhicules électriques dans le dataset."""
        assert silver_autos["is_electric"].sum() == 230

    def test_is_hybrid_count(self, silver_autos):
        """458 véhicules hybrides dans le dataset."""
        assert silver_autos["is_hybrid"].sum() == 458

    def test_driver_age_range(self, silver_drivers):
        """Âges entre 26 et 74 ans."""
        assert silver_drivers["driver_age"].min() >= 26
        assert silver_drivers["driver_age"].max() <= 74

    def test_driver_age_group_values(self, silver_drivers):
        """driver_age_group ∈ {<30, 30-45, 45-60, 60+}."""
        valid = {"<30", "30-45", "45-60", "60+"}
        actual = set(silver_drivers["driver_age_group"].dropna().unique())
        assert actual <= valid

    def test_fuel_alert_none_triggered(self, silver_events):
        """Aucune alerte carburant dans le dataset actuel (min fuel=75.1%)."""
        assert silver_events["fuel_alert"].sum() == 0


# ── Tests Jointures ───────────────────────────────────────────────────────

class TestJoinSilver:

    def test_joined_row_count(self, silver_joined):
        """Table jointe : 6 888 lignes."""
        assert len(silver_joined) == 6888

    def test_joined_col_count(self, silver_joined):
        """Table jointe : 54 colonnes."""
        assert len(silver_joined.columns) == 54, \
            f"Attendu 54 colonnes, obtenu {len(silver_joined.columns)}: {list(silver_joined.columns)}"

    def test_joined_has_event_cols(self, silver_joined):
        """Colonnes events présentes dans la table jointe."""
        for col in ["EventID", "vehicle_speed", "fuel_level", "speed_category"]:
            assert col in silver_joined.columns, f"Colonne manquante : {col}"

    def test_joined_has_auto_cols(self, silver_joined):
        """Colonnes autos présentes dans la table jointe."""
        for col in ["AutoID", "Model", "EngineType", "vehicle_age_years"]:
            assert col in silver_joined.columns, f"Colonne manquante : {col}"

    def test_joined_has_driver_cols(self, silver_joined):
        """Colonnes drivers présentes dans la table jointe."""
        for col in ["FullName", "Gender", "driver_age", "driver_age_group"]:
            assert col in silver_joined.columns, f"Colonne manquante : {col}"

    def test_no_duplicate_events(self, silver_joined):
        """Pas de doublon sur EventID dans la table jointe."""
        assert silver_joined["EventID"].nunique() == len(silver_joined)


# ── Tests Fichiers Parquet (si déjà exportés) ─────────────────────────────

class TestParquetFiles:

    def test_parquet_events_exists(self):
        assert Path("data/silver/silver_events.parquet").exists()

    def test_parquet_autos_exists(self):
        assert Path("data/silver/silver_autos.parquet").exists()

    def test_parquet_drivers_exists(self):
        assert Path("data/silver/silver_drivers.parquet").exists()

    def test_parquet_joined_exists(self):
        assert Path("data/silver/silver_fleet_joined.parquet").exists()

    def test_parquet_events_readable(self):
        df = pd.read_parquet("data/silver/silver_events.parquet")
        assert len(df) == 6888

    def test_parquet_joined_readable(self):
        df = pd.read_parquet("data/silver/silver_fleet_joined.parquet")
        assert len(df) == 6888
        assert len(df.columns) == 54
