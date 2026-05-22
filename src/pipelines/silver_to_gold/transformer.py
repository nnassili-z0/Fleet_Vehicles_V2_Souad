"""
Silver to Gold pipeline - Star Schema creation.

Transforms cleaned Silver data into Gold layer analytical data warehouse using
a Star Schema with:
- Fact tables: fact_driving_event, fact_maintenance_alert
- Dimension tables: dim_vehicle, dim_driver, dim_date, dim_time

Follows SOLID and clean architecture principles.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd

from src.config.settings import settings
from src.infrastructure.logging_config import get_logger

logger = get_logger(__name__)


class SilverToGoldTransformer:
    """
    Transforms Silver data to Gold layer using Star Schema pattern.

    Responsibilities:
    - Load Silver data
    - Create and populate dimension tables
    - Create and populate fact tables
    - Generate analytics views
    """

    def __init__(self, execution_id: str | None = None):
        """
        Initialize transformer.

        Args:
            execution_id: Unique execution identifier for tracing
        """
        self.execution_id = execution_id or datetime.utcnow().strftime(
            "%Y%m%d_%H%M%S"
        )
        settings.ensure_directories_exist()

    def load_silver_vehicles(self) -> pd.DataFrame:
        """Load vehicles from Silver layer."""
        path = settings.path_config.silver_data_dir / "silver_vehicle.csv"
        logger.info(f"Loading vehicles from {path}")
        return pd.read_csv(path, parse_dates=["created_at", "updated_at"])

    def load_silver_drivers(self) -> pd.DataFrame:
        """Load drivers from Silver layer."""
        path = settings.path_config.silver_data_dir / "silver_driver.csv"
        logger.info(f"Loading drivers from {path}")
        return pd.read_csv(
            path,
            parse_dates=["date_of_birth", "created_at", "updated_at"],
        )

    def load_silver_events(self) -> pd.DataFrame:
        """Load events from Silver layer."""
        path = settings.path_config.silver_data_dir / "silver_events.csv"
        logger.info(f"Loading events from {path}")
        df = pd.read_csv(path, parse_dates=["timestamp", "event_date"])
        return df

    # ========================================
    # DIMENSION TABLES
    # ========================================

    def create_dim_date(self) -> pd.DataFrame:
        """
        Create date dimension table.

        Covers 2 years of historical dates and 1 year future.
        """
        logger.info("Creating dim_date")

        start_date = datetime(2020, 1, 1)
        end_date = datetime(2027, 12, 31)

        dates = pd.date_range(start_date, end_date, freq="D")
        df = pd.DataFrame({"date": dates})

        df["date_id"] = (df["date"] - pd.Timestamp("2000-01-01")).dt.days
        df["year"] = df["date"].dt.year
        df["quarter"] = df["date"].dt.quarter
        df["month"] = df["date"].dt.month
        df["day"] = df["date"].dt.day
        df["day_of_week"] = df["date"].dt.dayofweek
        df["is_weekend"] = df["day_of_week"].isin([5, 6])
        df["week_of_year"] = df["date"].dt.isocalendar().week

        logger.info(f"Created {len(df)} date dimension rows")
        return df[
            [
                "date_id",
                "date",
                "year",
                "quarter",
                "month",
                "day",
                "day_of_week",
                "is_weekend",
                "week_of_year",
            ]
        ]

    def create_dim_time(self) -> pd.DataFrame:
        """Create time dimension table."""
        logger.info("Creating dim_time")

        times = []
        time_id = 0

        for hour in range(24):
            for minute in range(0, 60, 15):  # 15-minute intervals
                time_period = self._get_time_period(hour)
                times.append(
                    {
                        "time_id": time_id,
                        "hour": hour,
                        "minute": minute,
                        "second": 0,
                        "time_period": time_period,
                    }
                )
                time_id += 1

        df = pd.DataFrame(times)
        logger.info(f"Created {len(df)} time dimension rows")
        return df

    @staticmethod
    def _get_time_period(hour: int) -> str:
        """Determine time period from hour."""
        if 6 <= hour < 12:
            return "morning"
        elif 12 <= hour < 17:
            return "afternoon"
        elif 17 <= hour < 21:
            return "evening"
        else:
            return "night"

    def create_dim_vehicle(self, vehicles_df: pd.DataFrame) -> pd.DataFrame:
        """
        Create vehicle dimension table with SCD Type 2 support.

        Args:
            vehicles_df: Silver vehicle data

        Returns:
            Dimension table
        """
        logger.info("Creating dim_vehicle")

        df = vehicles_df.copy()
        df["vehicle_key"] = range(1, len(df) + 1)
        df["effective_date"] = df["created_at"].dt.date
        df["end_date"] = None
        df["is_current"] = True

        result = df[
            [
                "vehicle_key",
                "auto_id",
                "vin",
                "make",
                "year",
                "transmission",
                "owner_id",
                "effective_date",
                "end_date",
                "is_current",
            ]
        ]

        logger.info(f"Created {len(result)} vehicle dimension rows")
        return result

    def create_dim_driver(self, drivers_df: pd.DataFrame) -> pd.DataFrame:
        """
        Create driver dimension table with SCD Type 2 support.

        Args:
            drivers_df: Silver driver data

        Returns:
            Dimension table
        """
        logger.info("Creating dim_driver")

        df = drivers_df.copy()
        df["driver_key"] = range(1, len(df) + 1)
        df["effective_date"] = df["created_at"].dt.date
        df["end_date"] = None
        df["is_current"] = True

        result = df[
            [
                "driver_key",
                "person_id",
                "full_name",
                "gender",
                "date_of_birth",
                "age",
                "effective_date",
                "end_date",
                "is_current",
            ]
        ]

        logger.info(f"Created {len(result)} driver dimension rows")
        return result

    # ========================================
    # FACT TABLES
    # ========================================

    def create_fact_driving_event(
        self,
        events_df: pd.DataFrame,
        dim_vehicle: pd.DataFrame,
        dim_driver: pd.DataFrame,
        dim_date: pd.DataFrame,
        dim_time: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Create fact_driving_event fact table.

        Args:
            events_df: Silver events data
            dim_vehicle: Vehicle dimension
            dim_driver: Driver dimension
            dim_date: Date dimension
            dim_time: Time dimension

        Returns:
            Fact table
        """
        logger.info("Creating fact_driving_event")

        df = events_df.copy()

        # Join with dimensions
        df = df.merge(
            dim_vehicle[["vehicle_key", "auto_id"]],
            on="auto_id",
            how="left",
        )

        # For now, we'll use a simple join - in production, would use driver_id from events
        # Create a default driver_key (would need event-driver mapping in real scenario)
        df["driver_key"] = 1

        # Convert event_date to date_id
        df["date_id"] = (df["event_date"] - pd.Timestamp("2000-01-01")).dt.days

        # Create time_id based on hour (for simplicity, match to nearest 15-min interval)
        df["time_id"] = (df["timestamp"].dt.hour * 4 + df["timestamp"].dt.minute // 15) * 4

        # Calculate derived metrics
        df["is_speeding"] = df["vehicle_speed"] > 120
        df["is_harsh_braking"] = (
            (df["brake_pedal_status"] == True) & (df["vehicle_speed"] > 50)
        )
        df["is_harsh_acceleration"] = (
            (df["accelerator_pedal_position"] > 80) & (df["vehicle_speed"] > 20)
        )
        df["fuel_consumption_rate"] = df["fuel_consumed_since_restart"] / (
            df["odometer"] + 1
        )  # kg/km

        df["event_key"] = range(1, len(df) + 1)

        result = df[
            [
                "event_key",
                "event_id",
                "vehicle_key",
                "driver_key",
                "date_id",
                "time_id",
                "odometer",
                "vehicle_speed",
                "fuel_level",
                "engine_speed",
                "accelerator_pedal_position",
                "is_speeding",
                "is_harsh_braking",
                "is_harsh_acceleration",
                "fuel_consumption_rate",
            ]
        ]

        logger.info(f"Created {len(result)} fact driving event rows")
        return result

    def create_fact_maintenance_alert(
        self,
        events_df: pd.DataFrame,
        dim_vehicle: pd.DataFrame,
        dim_date: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Create fact_maintenance_alert fact table based on event patterns.

        Args:
            events_df: Silver events data
            dim_vehicle: Vehicle dimension
            dim_date: Date dimension

        Returns:
            Fact table
        """
        logger.info("Creating fact_maintenance_alert")

        alerts = []
        alert_key = 1

        for auto_id, group in events_df.groupby("auto_id"):
            # Get vehicle dimension key
            vehicle_key = dim_vehicle[dim_vehicle["auto_id"] == auto_id][
                "vehicle_key"
            ].values

            if len(vehicle_key) == 0:
                continue

            vehicle_key = vehicle_key[0]

            # Check for high mileage
            max_odometer = group["odometer"].max()
            if max_odometer > 200000:
                alerts.append(
                    {
                        "alert_key": alert_key,
                        "vehicle_key": vehicle_key,
                        "date_id": (
                            group["timestamp"].max().date() - pd.Timestamp("2000-01-01")
                        ).days,
                        "alert_type": "HIGH_MILEAGE",
                        "alert_severity": "MEDIUM",
                        "description": f"Vehicle has {max_odometer:.0f}km mileage",
                        "action_taken": None,
                        "resolved": False,
                        "estimated_maintenance_hours": 4,
                    }
                )
                alert_key += 1

            # Check for low fuel level
            low_fuel_events = (group["fuel_level"] < 10).sum()
            if low_fuel_events > 10:
                alerts.append(
                    {
                        "alert_key": alert_key,
                        "vehicle_key": vehicle_key,
                        "date_id": (
                            group["timestamp"].max().date() - pd.Timestamp("2000-01-01")
                        ).days,
                        "alert_type": "FUEL_LEVEL",
                        "alert_severity": "LOW",
                        "description": f"Frequent low fuel level ({low_fuel_events} events)",
                        "action_taken": None,
                        "resolved": False,
                        "estimated_maintenance_hours": 1,
                    }
                )
                alert_key += 1

            # Check for harsh driving events
            harsh_events = (
                ((group["is_braking"] == True) & (group["vehicle_speed"] > 50)).sum()
                + (group["is_accelerating"].sum())
            )
            if harsh_events > 100:
                alerts.append(
                    {
                        "alert_key": alert_key,
                        "vehicle_key": vehicle_key,
                        "date_id": (
                            group["timestamp"].max().date() - pd.Timestamp("2000-01-01")
                        ).days,
                        "alert_type": "HARSH_DRIVING",
                        "alert_severity": "HIGH",
                        "description": f"High number of harsh events ({harsh_events})",
                        "action_taken": None,
                        "resolved": False,
                        "estimated_maintenance_hours": 2,
                    }
                )
                alert_key += 1

        df = pd.DataFrame(alerts)
        logger.info(f"Created {len(df)} maintenance alert rows")
        return df

    # ========================================
    # ANALYTICS VIEWS
    # ========================================

    def create_vehicle_kpi_view(
        self, events_df: pd.DataFrame, vehicles_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Create materialized view of vehicle KPIs."""
        logger.info("Creating vehicle KPI view")

        kpis = []
        for auto_id, group in events_df.groupby("auto_id"):
            vehicle_info = vehicles_df[vehicles_df["auto_id"] == auto_id]

            kpi = {
                "auto_id": auto_id,
                "total_events": len(group),
                "total_distance_km": group["odometer"].max() - group["odometer"].min(),
                "average_speed": group["vehicle_speed"].mean(),
                "max_speed": group["vehicle_speed"].max(),
                "total_harsh_events": (
                    ((group["is_braking"] == True) & (group["vehicle_speed"] > 50)).sum()
                    + (group["is_accelerating"].sum())
                ),
                "avg_fuel_consumption": group["fuel_level"].mean(),
                "maintenance_alerts_count": (group["fuel_level"] < 10).sum(),
                "last_event_date": group["timestamp"].max().date(),
            }
            kpis.append(kpi)

        df = pd.DataFrame(kpis)
        logger.info(f"Created {len(df)} vehicle KPI rows")
        return df

    def create_driver_score_view(self, events_df: pd.DataFrame) -> pd.DataFrame:
        """Create materialized view of driver scores."""
        logger.info("Creating driver score view")

        # For now, use vehicle as proxy (real implementation would use driver_id)
        # This is a placeholder implementation

        scores = []
        for auto_id, group in events_df.groupby("auto_id"):
            total_events = len(group)
            harsh_events = (
                ((group["is_braking"] == True) & (group["vehicle_speed"] > 50)).sum()
                + (group["is_accelerating"].sum())
            )
            speeding_events = (group["vehicle_speed"] > 120).sum()

            safety_score = max(0, 100 - (harsh_events / total_events * 50))
            efficiency_score = max(0, 100 - (group["fuel_level"].std() / 20 * 30))

            risk_level = (
                "HIGH"
                if harsh_events / total_events > 0.2
                else ("MEDIUM" if harsh_events / total_events > 0.1 else "LOW")
            )

            scores.append(
                {
                    "auto_id": auto_id,
                    "full_name": f"Vehicle {auto_id}",
                    "safety_score": round(safety_score, 2),
                    "efficiency_score": round(efficiency_score, 2),
                    "total_trips": total_events,
                    "harsh_events_count": harsh_events,
                    "speeding_events_count": speeding_events,
                    "overall_risk_level": risk_level,
                }
            )

        df = pd.DataFrame(scores)
        logger.info(f"Created {len(df)} driver score rows")
        return df

    # ========================================
    # OUTPUT WRITING
    # ========================================

    def write_gold_data(self, prefix: str, df: pd.DataFrame) -> None:
        """Write dataframe to Gold layer."""
        output_path = settings.path_config.gold_data_dir / f"{prefix}.csv"
        df.to_csv(output_path, index=False)
        logger.info(f"Written {len(df)} rows to {output_path}")

    def cleanup_previous_gold_outputs(self) -> None:
        """Remove previous Gold CSV outputs before a new pipeline run."""
        for dir_path in (
            settings.path_config.gold_dir,
            settings.path_config.gold_data_dir,
        ):
            for csv_file in dir_path.glob("*.csv"):
                try:
                    csv_file.unlink()
                    logger.info(f"Removed old Gold output: {csv_file}")
                except Exception as cleanup_error:
                    logger.warning(
                        f"Unable to remove old Gold output {csv_file}: {cleanup_error}"
                    )

    # ========================================
    # ORCHESTRATION
    # ========================================

    def run_full_pipeline(self) -> dict[str, Any]:
        """
        Execute complete Silver to Gold pipeline.

        Returns:
            Summary of pipeline execution
        """
        logger.info(f"Starting Silver to Gold pipeline (execution_id={self.execution_id})")

        results = {
            "execution_id": self.execution_id,
            "timestamp": datetime.utcnow().isoformat(),
            "tables": {},
        }

        try:
            # Clean old Gold outputs before generating new ones
            self.cleanup_previous_gold_outputs()

            # Load Silver data
            vehicles = self.load_silver_vehicles()
            drivers = self.load_silver_drivers()
            events = self.load_silver_events()

            # Create dimensions
            dim_date = self.create_dim_date()
            self.write_gold_data("dim_date", dim_date)
            results["tables"]["dim_date"] = len(dim_date)

            dim_time = self.create_dim_time()
            self.write_gold_data("dim_time", dim_time)
            results["tables"]["dim_time"] = len(dim_time)

            dim_vehicle = self.create_dim_vehicle(vehicles)
            self.write_gold_data("dim_vehicle", dim_vehicle)
            results["tables"]["dim_vehicle"] = len(dim_vehicle)

            dim_driver = self.create_dim_driver(drivers)
            self.write_gold_data("dim_driver", dim_driver)
            results["tables"]["dim_driver"] = len(dim_driver)

            # Create facts
            fact_driving = self.create_fact_driving_event(
                events, dim_vehicle, dim_driver, dim_date, dim_time
            )
            self.write_gold_data("fact_driving_event", fact_driving)
            results["tables"]["fact_driving_event"] = len(fact_driving)

            fact_maintenance = self.create_fact_maintenance_alert(
                events, dim_vehicle, dim_date
            )
            self.write_gold_data("fact_maintenance_alert", fact_maintenance)
            results["tables"]["fact_maintenance_alert"] = len(fact_maintenance)

            # Create analytics views
            vehicle_kpi = self.create_vehicle_kpi_view(events, vehicles)
            self.write_gold_data("analytics_vehicle_kpi", vehicle_kpi)
            results["tables"]["analytics_vehicle_kpi"] = len(vehicle_kpi)

            driver_score = self.create_driver_score_view(events)
            self.write_gold_data("analytics_driver_score", driver_score)
            results["tables"]["analytics_driver_score"] = len(driver_score)

            results["status"] = "success"
            logger.info("Silver to Gold pipeline completed successfully")

        except Exception as e:
            logger.exception(f"Pipeline failed: {e}")
            results["status"] = "failed"
            results["error"] = str(e)
            raise

        return results
