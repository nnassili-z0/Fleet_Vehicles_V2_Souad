"""
Event-specific quality checks for telematics data.

Implements domain-specific validation rules for vehicle events
following the single responsibility principle.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

import pandas as pd

from src.config.settings import settings
from src.quality.detectors.base_quality_checker import (
    BaseQualityChecker,
    QualityCheckResult,
    QualityIssue,
    SeverityLevel,
)


class EventQualityChecker(BaseQualityChecker):
    """Quality checks specific to telematics events."""

    EXPECTED_COLUMNS = {
        "event_id",
        "auto_id",
        "timestamp",
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
        "brake_pedal_status",
        "high_beam_status",
        "windshield_wiper_status",
        "headlamp_status",
        "parking_brake_status",
    }

    NOT_NULL_COLUMNS = {
        "event_id",
        "auto_id",
        "timestamp",
        "odometer",
        "vehicle_speed",
        "latitude",
        "longitude",
    }

    def __init__(self):
        """Initialize event quality checker."""
        super().__init__("event")

    def check_schema(self, df: pd.DataFrame) -> QualityCheckResult:
        """Check if event dataframe has correct schema."""
        return self.check_schema_consistency(df, self.EXPECTED_COLUMNS)

    def check_not_null_constraints(self, df: pd.DataFrame) -> QualityCheckResult:
        """Check for null values in required columns."""
        return self.check_null_constraints(df, self.NOT_NULL_COLUMNS)

    def check_timestamp_validity(self, df: pd.DataFrame) -> QualityCheckResult:
        """
        Check if timestamps are valid and reasonable.

        A valid timestamp should not be in the future or too far in the past.
        """
        result = QualityCheckResult(check_name=f"{self.entity_name}_timestamp_validity")
        result.total_rows_checked = len(df)

        if "timestamp" not in df.columns:
            result.passed = False
            return result

        try:
            timestamps = pd.to_datetime(df["timestamp"], errors="coerce")
        except Exception:
            result.passed = False
            issue = QualityIssue(
                check_name=result.check_name,
                severity=SeverityLevel.CRITICAL,
                description="Timestamp parsing failed",
                affected_rows=len(df),
                affected_columns=["timestamp"],
            )
            result.add_issue(issue)
            return result

        now = datetime.utcnow()
        future_mask = timestamps > now
        past_mask = timestamps < (now - timedelta(days=730))  # > 2 years old

        invalid_count = future_mask.sum() + past_mask.sum()

        if invalid_count > 0:
            result.passed = False
            issue = QualityIssue(
                check_name=result.check_name,
                severity=SeverityLevel.ERROR,
                description="Invalid timestamps detected",
                affected_rows=invalid_count,
                affected_columns=["timestamp"],
                details={
                    "future_count": int(future_mask.sum()),
                    "too_old_count": int(past_mask.sum()),
                },
            )
            result.add_issue(issue)
        else:
            result.passed = True

        return result

    def check_odometer_monotonic(self, df: pd.DataFrame) -> QualityCheckResult:
        """
        Check if odometer values are generally monotonic.

        Odometer should only increase (or stay same) over time per vehicle.
        """
        result = QualityCheckResult(
            check_name=f"{self.entity_name}_odometer_monotonic"
        )
        result.total_rows_checked = len(df)

        if "odometer" not in df.columns or "auto_id" not in df.columns:
            result.passed = True
            return result

        violations = []

        for auto_id, group in df.groupby("auto_id"):
            sorted_group = group.sort_values("timestamp", na_position="last")
            odometer_diffs = sorted_group["odometer"].diff()
            negative_diffs = (odometer_diffs < -10).sum()  # Allow 10km tolerance

            if negative_diffs > 0:
                violations.append(
                    {
                        "auto_id": str(auto_id),
                        "negative_diffs": int(negative_diffs),
                    }
                )

        if violations:
            result.passed = False
            issue = QualityIssue(
                check_name=result.check_name,
                severity=SeverityLevel.WARNING,
                description="Odometer decreases detected",
                affected_rows=len(violations),
                affected_columns=["odometer", "auto_id"],
                details={"violations": violations},
            )
            result.add_issue(issue)
        else:
            result.passed = True

        return result

    def check_speed_range(self, df: pd.DataFrame) -> QualityCheckResult:
        """Check if vehicle speeds are within realistic range."""
        return self.check_value_range(
            df, "vehicle_speed", min_val=0, max_val=350
        )  # km/h

    def check_fuel_level_range(self, df: pd.DataFrame) -> QualityCheckResult:
        """Check if fuel level is between 0-100%."""
        return self.check_value_range(df, "fuel_level", min_val=0, max_val=100)

    def check_coordinates_validity(self, df: pd.DataFrame) -> QualityCheckResult:
        """Check if GPS coordinates are valid."""
        result = QualityCheckResult(
            check_name=f"{self.entity_name}_coordinates_validity"
        )
        result.total_rows_checked = len(df)

        if "latitude" not in df.columns or "longitude" not in df.columns:
            result.passed = True
            return result

        invalid_lat = (df["latitude"] < -90) | (df["latitude"] > 90)
        invalid_lon = (df["longitude"] < -180) | (df["longitude"] > 180)
        invalid_count = (invalid_lat | invalid_lon).sum()

        if invalid_count > 0:
            result.passed = False
            issue = QualityIssue(
                check_name=result.check_name,
                severity=SeverityLevel.WARNING,
                description="Invalid GPS coordinates detected",
                affected_rows=invalid_count,
                affected_columns=["latitude", "longitude"],
                details={"invalid_count": int(invalid_count)},
            )
            result.add_issue(issue)
        else:
            result.passed = True

        return result

    def check_no_duplicates(self, df: pd.DataFrame) -> QualityCheckResult:
        """Check for duplicate event IDs."""
        return self.check_duplicates(df, ["event_id"])

    def run_all_checks(self, df: pd.DataFrame) -> list[QualityCheckResult]:
        """
        Execute all quality checks on events.

        Args:
            df: Events dataframe

        Returns:
            List of quality check results
        """
        results = [
            self.check_schema(df),
            self.check_not_null_constraints(df),
            self.check_timestamp_validity(df),
            self.check_odometer_monotonic(df),
            self.check_speed_range(df),
            self.check_fuel_level_range(df),
            self.check_coordinates_validity(df),
            self.check_no_duplicates(df),
        ]

        return results

    def get_summary(self, results: list[QualityCheckResult]) -> dict[str, Any]:
        """
        Generate summary of quality checks.

        Args:
            results: List of check results

        Returns:
            Summary dictionary
        """
        total_checks = len(results)
        passed_checks = sum(1 for r in results if r.passed)
        critical_issues = [
            issue
            for result in results
            for issue in result.issues
            if issue.severity == SeverityLevel.CRITICAL
        ]

        return {
            "total_checks": total_checks,
            "passed_checks": passed_checks,
            "failed_checks": total_checks - passed_checks,
            "has_critical_issues": len(critical_issues) > 0,
            "critical_issue_count": len(critical_issues),
            "all_results": [r.to_dict() for r in results],
        }
