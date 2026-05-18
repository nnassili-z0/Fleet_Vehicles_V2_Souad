"""
test_gold.py
------------
Tests pytest pour la couche Gold.
Valide : row counts, score ranges, anomaly types, alerts.

Run: python -m pytest tests/test_gold.py -v
"""

import sys
from pathlib import Path
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))


# ── Fixtures ──────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def silver_joined():
    p = Path("data/silver/silver_fleet_joined.csv")
    if not p.exists():
        pytest.skip("Silver data not found — run silver pipeline first")
    df = pd.read_csv(p, parse_dates=["timestamp"])
    for col in ["brake_pedal_status","high_beam_status","windshield_wiper_status"]:
        if col in df.columns:
            df[col] = df[col].map({"True":True,"False":False,True:True,False:False})
    return df

@pytest.fixture(scope="session")
def fleet_kpis(silver_joined):
    from src.analytics.compute_fleet_kpis import compute_fleet_kpis
    return compute_fleet_kpis(silver_joined)

@pytest.fixture(scope="session")
def driver_scores(silver_joined):
    from src.analytics.score_drivers import compute_driver_scores
    return compute_driver_scores(silver_joined.copy())

@pytest.fixture(scope="session")
def anomalies(silver_joined):
    from src.analytics.detect_anomalies import detect_anomalies
    return detect_anomalies(silver_joined.copy())

@pytest.fixture(scope="session")
def maintenance_alerts(silver_joined):
    from src.analytics.build_maintenance_alerts import build_maintenance_alerts
    return build_maintenance_alerts(silver_joined)


# ── fleet_kpis tests ──────────────────────────────────────────────────

class TestFleetKPIs:

    def test_row_count(self, fleet_kpis):
        """5 véhicules actifs dans le dataset."""
        assert len(fleet_kpis) == 5, f"Expected 5, got {len(fleet_kpis)}"

    def test_required_columns(self, fleet_kpis):
        for col in ["auto_id","avg_speed","max_speed","avg_fuel_level",
                    "total_distance_km","total_events","driving_ratio","maintenance_due"]:
            assert col in fleet_kpis.columns, f"Missing: {col}"

    def test_avg_speed_positive(self, fleet_kpis):
        assert (fleet_kpis["avg_speed"] >= 0).all()

    def test_avg_speed_realistic(self, fleet_kpis):
        """Vitesse moyenne réelle ~15.7 km/h."""
        assert fleet_kpis["avg_speed"].mean() == pytest.approx(15.65, abs=0.5)

    def test_max_speed_no_outlier(self, fleet_kpis):
        """Pas de vitesse > 200 km/h (outlier AutoID=1 filtré)."""
        assert fleet_kpis["max_speed"].max() < 200

    def test_fuel_level_range(self, fleet_kpis):
        assert (fleet_kpis["avg_fuel_level"] >= 0).all()
        assert (fleet_kpis["avg_fuel_level"] <= 100).all()

    def test_distance_positive(self, fleet_kpis):
        assert (fleet_kpis["total_distance_km"] >= 0).all()

    def test_driving_ratio_range(self, fleet_kpis):
        assert (fleet_kpis["driving_ratio"] >= 0).all()
        assert (fleet_kpis["driving_ratio"] <= 1).all()

    def test_all_auto_ids_present(self, fleet_kpis):
        """AutoIDs 2,3,4,5,6 doivent tous être là."""
        assert set(fleet_kpis["auto_id"].astype(str)) == {"2", "3", "4", "5", "6"}

    def test_total_events_sum(self, fleet_kpis):
        """Total events = 6888 (tous les événements Silver)."""
        assert fleet_kpis["total_events"].sum() == 6888


# ── driver_scores tests ───────────────────────────────────────────────

class TestDriverScores:

    def test_row_count(self, driver_scores):
        assert len(driver_scores) == 5

    def test_score_range(self, driver_scores):
        """Scores entre 0 et 100."""
        assert (driver_scores["safety_score"] >= 0).all()
        assert (driver_scores["safety_score"] <= 100).all()

    def test_all_drivers_100(self, driver_scores):
        """Tous les conducteurs ont 100/100 (0 overspeed, 0 freinage)."""
        assert (driver_scores["safety_score"] == 100.0).all()

    def test_safety_rank_unique(self, driver_scores):
        """Chaque rang est unique (ou ex-aequo géré)."""
        assert driver_scores["safety_rank"].notna().all()

    def test_driver_names_present(self, driver_scores):
        expected = {"Ravindra Parkar", "Krista Andrejev", "Isabella Rupp",
                    "Paulus Lippmaa", "Leticia Ribeiro"}
        actual = set(driver_scores["driver_name"])
        assert actual == expected

    def test_overspeed_zero(self, driver_scores):
        """Aucun excès de vitesse dans le dataset Silver filtré."""
        assert (driver_scores["overspeed_count"] == 0).all()

    def test_required_columns(self, driver_scores):
        for col in ["auto_id","driver_name","safety_score","speed_score",
                    "brake_score","overspeed_count","brake_events","safety_rank"]:
            assert col in driver_scores.columns


# ── anomalies tests ───────────────────────────────────────────────────

class TestAnomalies:

    def test_row_count(self, anomalies):
        """5% de 6888 = ~336 anomalies."""
        assert 300 <= len(anomalies) <= 400, f"Expected ~336, got {len(anomalies)}"

    def test_anomaly_types_valid(self, anomalies):
        valid = {"overspeed", "low_fuel", "high_rpm", "statistical"}
        actual = set(anomalies["anomaly_type"].unique())
        assert actual <= valid, f"Invalid types: {actual - valid}"

    def test_severity_valid(self, anomalies):
        valid = {"H", "M", "L"}
        actual = set(anomalies["severity"].unique())
        assert actual <= valid

    def test_no_null_auto_id(self, anomalies):
        assert anomalies["AutoID"].notna().all()

    def test_anomaly_score_negative(self, anomalies):
        """IsolationForest scores are negative for anomalies."""
        assert (anomalies["anomaly_score"] < 0).all()

    def test_all_vehicles_represented(self, anomalies):
        """Chaque véhicule a des anomalies détectées."""
        assert anomalies["AutoID"].nunique() == 5


# ── maintenance_alerts tests ──────────────────────────────────────────

class TestMaintenanceAlerts:

    def test_row_count(self, maintenance_alerts):
        """3 alertes kilométrage (AutoID 2, 3, 4)."""
        assert len(maintenance_alerts) == 3

    def test_alert_types(self, maintenance_alerts):
        valid = {"mileage", "event"}
        assert set(maintenance_alerts["alert_type"]).issubset(valid)

    def test_status_open(self, maintenance_alerts):
        assert (maintenance_alerts["status"] == "open").all()

    def test_vehicles_alerted(self, maintenance_alerts):
        """AutoID 2, 3, 4 ont des alertes (< 2000 km du prochain entretien)."""
        alerted = set(maintenance_alerts["auto_id"].astype(str))
        assert alerted == {"2", "3", "4"}

    def test_odometer_positive(self, maintenance_alerts):
        assert (maintenance_alerts["current_odometer"] > 0).all()

    def test_next_service_greater_current(self, maintenance_alerts):
        assert (maintenance_alerts["next_service_km"] >
                maintenance_alerts["current_odometer"]).all()


# ── Gold output files tests ───────────────────────────────────────────

class TestGoldFiles:

    def test_fleet_kpis_file_exists(self):
        assert Path("data/gold/fleet_kpis.csv").exists() or \
               Path("data/gold/fleet_kpis.parquet").exists()

    def test_driver_scores_file_exists(self):
        assert Path("data/gold/driver_scores.csv").exists() or \
               Path("data/gold/driver_scores.parquet").exists()

    def test_anomalies_file_exists(self):
        assert Path("data/gold/anomalies.csv").exists() or \
               Path("data/gold/anomalies.parquet").exists()

    def test_maintenance_alerts_file_exists(self):
        assert Path("data/gold/maintenance_alerts.csv").exists() or \
               Path("data/gold/maintenance_alerts.parquet").exists()

    def test_fleet_kpis_readable(self):
        try:
            df = pd.read_parquet("data/gold/fleet_kpis.parquet")
        except Exception:
            df = pd.read_csv("data/gold/fleet_kpis.csv")
        assert len(df) == 5

    def test_anomalies_readable(self):
        try:
            df = pd.read_parquet("data/gold/anomalies.parquet")
        except Exception:
            df = pd.read_csv("data/gold/anomalies.csv")
        assert len(df) >= 300
