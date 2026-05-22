"""
Bronze to Silver pipeline - Data Cleansing and Quality Checks.

Transforms raw Bronze data into cleaned Silver data with:
1. Schema validation
2. Data type conversion
3. Null handling
4. Outlier removal
5. Standardization
6. Quality artifacts generation

Follows SOLID principles:
- SoC: Separation of concerns (detection vs. implementation)
- DI: Dependency injection via configuration
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from src.config.settings import settings
from src.infrastructure.logging_config import get_logger
from src.quality.detectors.event_quality_checker import EventQualityChecker
from src.quality.detectors.master_data_quality_checker import (
    DriverQualityChecker,
    VehicleQualityChecker,
)
from src.quality.expectations.quality_report_writer import QualityReportWriter

logger = get_logger(__name__)


class BronzeToSilverTransformer:
    """
    Transforms Bronze data to Silver layer with data quality checks.

    Responsibilities:
    - Load Bronze data
    - Run quality checks (detection phase)
    - Clean and standardize data
    - Write cleaned data to Silver
    - Generate quality artifacts
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
        self.report_writer = QualityReportWriter()
        self.vehicle_checker = VehicleQualityChecker()
        self.driver_checker = DriverQualityChecker()
        self.event_checker = EventQualityChecker()

        # Ensure directories exist
        settings.ensure_directories_exist()

    def load_bronze_vehicles(self) -> pd.DataFrame:
        """Load vehicles from Bronze layer."""
        bronze_path = settings.path_config.bronze_dir / "autos.csv"
        logger.info(f"Loading vehicles from {bronze_path}")
        return pd.read_csv(bronze_path)

    def load_bronze_drivers(self) -> pd.DataFrame:
        """Load drivers from Bronze layer."""
        bronze_path = settings.path_config.bronze_dir / "drivers.csv"
        logger.info(f"Loading drivers from {bronze_path}")
        return pd.read_csv(bronze_path)

    def load_bronze_events(self) -> pd.DataFrame:
        """Load events from Bronze layer."""
        bronze_path = settings.path_config.bronze_dir / "events.csv"
        logger.info(f"Loading events from {bronze_path}")
        return pd.read_csv(bronze_path)

    # ========================================
    # VEHICLE CLEANING
    # ========================================

    def check_vehicle_quality(
        self, df: pd.DataFrame
    ) -> tuple[pd.DataFrame, dict[str, Any]]:
        """
        Run quality checks on vehicles (detection phase).

        Args:
            df: Vehicle dataframe from Bronze

        Returns:
            Tuple of (original df, quality summary)
        """
        logger.info(f"Running quality checks on {len(df)} vehicles")
        results = self.vehicle_checker.run_all_checks(df)
        summary = self.vehicle_checker.get_summary(results)

        # Log findings
        if summary["has_critical_issues"]:
            logger.error(
                f"Critical quality issues found: {summary['critical_issue_count']}"
            )
        else:
            logger.info("Vehicle quality checks passed")

        return df, summary

    def clean_vehicles(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean vehicle data (implementation phase).

        Args:
            df: Vehicle dataframe from Bronze

        Returns:
            Cleaned dataframe
        """
        logger.info("Cleaning vehicle data")
        df = df.copy()

        # Standardize column names
        df = df.rename(
            columns={
                "AutoID": "auto_id",
                "VIN": "vin",
                "Make": "make",
                "Year": "year",
                "Transmission": "transmission",
                "OwnerID": "owner_id",
            }
        )

        # Data type conversions
        df["auto_id"] = df["auto_id"].astype(str)
        df["vin"] = df["vin"].astype(str)
        df["owner_id"] = df["owner_id"].astype(str)
        df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")

        # Standardization
        df["transmission"] = df["transmission"].str.strip().str.title()
        df["make"] = df["make"].str.strip().str.title()

        # Add metadata columns
        df["created_at"] = datetime.utcnow()
        df["updated_at"] = datetime.utcnow()

        # Remove duplicates
        df = df.drop_duplicates(subset=["auto_id"], keep="first")

        logger.info(f"Cleaned {len(df)} vehicles")
        return df

    # ========================================
    # DRIVER CLEANING
    # ========================================

    def check_driver_quality(
        self, df: pd.DataFrame
    ) -> tuple[pd.DataFrame, dict[str, Any]]:
        """
        Run quality checks on drivers (detection phase).

        Args:
            df: Driver dataframe from Bronze

        Returns:
            Tuple of (original df, quality summary)
        """
        logger.info(f"Running quality checks on {len(df)} drivers")
        results = self.driver_checker.run_all_checks(df)
        summary = self.driver_checker.get_summary(results)

        if summary["has_critical_issues"]:
            logger.error(
                f"Critical quality issues found: {summary['critical_issue_count']}"
            )
        else:
            logger.info("Driver quality checks passed")

        return df, summary

    def clean_drivers(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean driver data (implementation phase).

        Args:
            df: Driver dataframe from Bronze

        Returns:
            Cleaned dataframe
        """
        logger.info("Cleaning driver data")
        df = df.copy()

        # Standardize column names
        df = df.rename(
            columns={
                "PersonID": "person_id",
                "FullName": "full_name",
                "Gender": "gender",
                "DateOfBirth": "date_of_birth",
            }
        )

        # Data type conversions
        df["person_id"] = df["person_id"].astype(str)
        df["date_of_birth"] = pd.to_datetime(df["date_of_birth"], errors="coerce")

        # Calculate age
        age_days = (datetime.utcnow() - df["date_of_birth"]).dt.days
        age_years = pd.to_numeric(age_days, errors="coerce") / 365.25
        df["age"] = age_years.apply(
            lambda value: int(value) if pd.notna(value) else pd.NA
        ).astype("Int64")

        # Standardization
        df["full_name"] = df["full_name"].str.strip().str.title()
        df["gender"] = df["gender"].str.strip().str.upper()

        # Add metadata
        df["created_at"] = datetime.utcnow()
        df["updated_at"] = datetime.utcnow()

        # Remove duplicates
        df = df.drop_duplicates(subset=["person_id"], keep="first")

        logger.info(f"Cleaned {len(df)} drivers")
        return df

    # ========================================
    # EVENT CLEANING
    # ========================================

    def check_event_quality(
        self, df: pd.DataFrame
    ) -> tuple[pd.DataFrame, dict[str, Any]]:
        """
        Run quality checks on events (detection phase).

        Args:
            df: Event dataframe from Bronze

        Returns:
            Tuple of (original df, quality summary)
        """
        logger.info(f"Running quality checks on {len(df)} events")
        results = self.event_checker.run_all_checks(df)
        summary = self.event_checker.get_summary(results)

        if summary["has_critical_issues"]:
            logger.error(
                f"Critical quality issues found: {summary['critical_issue_count']}"
            )
        else:
            logger.info("Event quality checks passed")

        return df, summary

    def clean_events(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean event data (implementation phase).

        Args:
            df: Event dataframe from Bronze

        Returns:
            Cleaned dataframe
        """
        logger.info("Cleaning event data")
        df = df.copy()

        # Standardize column names
        df = df.rename(
            columns={
                "AutoID": "auto_id",
                "EventID": "event_id",
            }
        )

        # Data type conversions
        df["event_id"] = df["event_id"].astype(str)
        df["auto_id"] = df["auto_id"].astype(str)
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

        # Convert numeric columns
        numeric_columns = [
            "odometer",
            "vehicle_speed",
            "fuel_level",
            "fuel_consumed_since_restart",
            "engine_speed",
            "torque_at_transmission",
            "accelerator_pedal_position",
            "steering_wheel_angle",
            "latitude",
            "longitude",
        ]
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Convert boolean columns
        boolean_columns = [
            "brake_pedal_status",
            "high_beam_status",
            "windshield_wiper_status",
            "headlamp_status",
            "parking_brake_status",
        ]
        for col in boolean_columns:
            if col in df.columns:
                df[col] = df[col].map({"true": True, "false": False, True: True, False: False})

        # Remove outliers in odometer
        df = df[df["odometer"] < 500000].copy()

        # Extract event_date
        df["event_date"] = df["timestamp"].dt.date

        # Feature engineering (categorization)
        df["speed_category"] = pd.cut(
            df["vehicle_speed"],
            bins=[-1, 0, 30, 90, 120, float("inf")],
            labels=["stop", "slow", "normal", "fast", "overspeed"],
        ).astype(str)

        df["driving_mode"] = "driving"
        df.loc[
            (df["vehicle_speed"] == 0) & (df["parking_brake_status"] == True),
            "driving_mode",
        ] = "parked"
        df.loc[(df["vehicle_speed"] == 0) & (df["parking_brake_status"] != True),
               "driving_mode"] = "idle"

        df["fuel_alert"] = df["fuel_level"] < 20
        df["is_braking"] = df["brake_pedal_status"] == True
        df["is_accelerating"] = df["accelerator_pedal_position"] > 30

        # Add metadata
        df["created_at"] = datetime.utcnow()
        df["updated_at"] = datetime.utcnow()

        # Remove duplicates
        df = df.drop_duplicates(subset=["event_id"], keep="first")

        logger.info(f"Cleaned {len(df)} events")
        return df

    # ========================================
    # OUTPUT WRITING
    # ========================================

    def write_silver_vehicles(self, df: pd.DataFrame) -> Path:
        """Write cleaned vehicles to Silver layer."""
        output_path = settings.path_config.silver_data_dir / "silver_vehicle.csv"
        df.to_csv(output_path, index=False)
        logger.info(f"Written {len(df)} vehicles to {output_path}")
        return output_path

    def write_silver_drivers(self, df: pd.DataFrame) -> Path:
        """Write cleaned drivers to Silver layer."""
        output_path = settings.path_config.silver_data_dir / "silver_driver.csv"
        df.to_csv(output_path, index=False)
        logger.info(f"Written {len(df)} drivers to {output_path}")
        return output_path

    def write_silver_events(self, df: pd.DataFrame) -> Path:
        """Write cleaned events to Silver layer."""
        output_path = settings.path_config.silver_data_dir / "silver_events.csv"
        df.to_csv(output_path, index=False)
        logger.info(f"Written {len(df)} events to {output_path}")
        return output_path

    def cleanup_previous_silver_outputs(self) -> None:
        """Remove previous Silver CSV outputs before a new pipeline run."""
        for dir_path in (
            settings.path_config.silver_dir,
            settings.path_config.silver_data_dir,
        ):
            for csv_file in dir_path.glob("*.csv"):
                try:
                    csv_file.unlink()
                    logger.info(f"Removed old Silver output: {csv_file}")
                except Exception as cleanup_error:
                    logger.warning(
                        f"Unable to remove old Silver output {csv_file}: {cleanup_error}"
                    )

    # ========================================
    # ORCHESTRATION
    # ========================================

    def run_full_pipeline(self) -> dict[str, Any]:
        """
        Execute complete Bronze to Silver pipeline.

        Returns:
            Summary of pipeline execution
        """
        logger.info(f"Starting Bronze to Silver pipeline (execution_id={self.execution_id})")

        results = {
            "execution_id": self.execution_id,
            "timestamp": datetime.utcnow().isoformat(),
            "entities": {},
        }

        try:
            # Clean old Silver outputs before generating new ones
            self.cleanup_previous_silver_outputs()

            # Process vehicles
            vehicles_bronze = self.load_bronze_vehicles()
            _, vehicle_quality = self.check_vehicle_quality(vehicles_bronze)
            vehicles_silver = self.clean_vehicles(vehicles_bronze)
            self.write_silver_vehicles(vehicles_silver)
            self.report_writer.write_report("vehicle", "bronze_to_silver",
                                           vehicle_quality, self.execution_id)
            results["entities"]["vehicle"] = {
                "status": "success",
                "record_count": len(vehicles_silver),
                "quality": vehicle_quality,
            }
            logger.info("Vehicle processing completed successfully")

            # Process drivers
            drivers_bronze = self.load_bronze_drivers()
            _, driver_quality = self.check_driver_quality(drivers_bronze)
            drivers_silver = self.clean_drivers(drivers_bronze)
            self.write_silver_drivers(drivers_silver)
            self.report_writer.write_report("driver", "bronze_to_silver",
                                           driver_quality, self.execution_id)
            results["entities"]["driver"] = {
                "status": "success",
                "record_count": len(drivers_silver),
                "quality": driver_quality,
            }
            logger.info("Driver processing completed successfully")

            # Process events
            events_bronze = self.load_bronze_events()
            _, event_quality = self.check_event_quality(events_bronze)
            events_silver = self.clean_events(events_bronze)
            self.write_silver_events(events_silver)
            self.report_writer.write_report("event", "bronze_to_silver",
                                           event_quality, self.execution_id)
            results["entities"]["event"] = {
                "status": "success",
                "record_count": len(events_silver),
                "quality": event_quality,
            }
            logger.info("Event processing completed successfully")

            results["status"] = "success"
            logger.info(f"Bronze to Silver pipeline completed successfully")

        except Exception as e:
            logger.exception(f"Pipeline failed: {e}")
            results["status"] = "failed"
            results["error"] = str(e)
            raise

        return results
