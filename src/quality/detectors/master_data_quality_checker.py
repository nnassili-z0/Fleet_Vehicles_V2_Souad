"""
Vehicle and Driver specific quality checks.

Implements domain-specific validation rules for vehicle and driver data.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from src.quality.detectors.base_quality_checker import (
    BaseQualityChecker,
    QualityCheckResult,
    QualityIssue,
    SeverityLevel,
)


class VehicleQualityChecker(BaseQualityChecker):
    """Quality checks specific to vehicle master data."""

    EXPECTED_COLUMNS = {
        "auto_id",
        "vin",
        "make",
        "year",
        "transmission",
        "owner_id",
    }

    NOT_NULL_COLUMNS = {"auto_id", "vin", "make"}

    def __init__(self):
        """Initialize vehicle quality checker."""
        super().__init__("vehicle")

    def check_schema(self, df: pd.DataFrame) -> QualityCheckResult:
        """Check if vehicle dataframe has correct schema."""
        return self.check_schema_consistency(df, self.EXPECTED_COLUMNS)

    def check_not_null_constraints(self, df: pd.DataFrame) -> QualityCheckResult:
        """Check for null values in required columns."""
        return self.check_null_constraints(df, self.NOT_NULL_COLUMNS)

    def check_year_validity(self, df: pd.DataFrame) -> QualityCheckResult:
        """Check if vehicle year is realistic."""
        return self.check_value_range(df, "year", min_val=1990, max_val=2050)

    def check_vin_format(self, df: pd.DataFrame) -> QualityCheckResult:
        """Check if VIN has proper format (17 characters)."""
        result = QualityCheckResult(check_name=f"{self.entity_name}_vin_format")
        result.total_rows_checked = len(df)

        if "vin" not in df.columns:
            result.passed = True
            return result

        invalid_vin = df["vin"].astype(str).str.len() != 17
        invalid_count = invalid_vin.sum()

        if invalid_count > 0:
            result.passed = False
            issue = QualityIssue(
                check_name=result.check_name,
                severity=SeverityLevel.WARNING,
                description="Invalid VIN format detected",
                affected_rows=invalid_count,
                affected_columns=["vin"],
            )
            result.add_issue(issue)
        else:
            result.passed = True

        return result

    def check_no_duplicates(self, df: pd.DataFrame) -> QualityCheckResult:
        """Check for duplicate vehicle IDs."""
        return self.check_duplicates(df, ["auto_id"])

    def run_all_checks(self, df: pd.DataFrame) -> list[QualityCheckResult]:
        """Execute all quality checks on vehicles."""
        results = [
            self.check_schema(df),
            self.check_not_null_constraints(df),
            self.check_year_validity(df),
            self.check_vin_format(df),
            self.check_no_duplicates(df),
        ]
        return results

    def get_summary(self, results: list[QualityCheckResult]) -> dict[str, Any]:
        """Generate summary of quality checks."""
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


class DriverQualityChecker(BaseQualityChecker):
    """Quality checks specific to driver master data."""

    EXPECTED_COLUMNS = {
        "person_id",
        "full_name",
        "gender",
        "date_of_birth",
    }

    NOT_NULL_COLUMNS = {"person_id", "full_name"}

    def __init__(self):
        """Initialize driver quality checker."""
        super().__init__("driver")

    def check_schema(self, df: pd.DataFrame) -> QualityCheckResult:
        """Check if driver dataframe has correct schema."""
        return self.check_schema_consistency(df, self.EXPECTED_COLUMNS)

    def check_not_null_constraints(self, df: pd.DataFrame) -> QualityCheckResult:
        """Check for null values in required columns."""
        return self.check_null_constraints(df, self.NOT_NULL_COLUMNS)

    def check_date_of_birth_validity(self, df: pd.DataFrame) -> QualityCheckResult:
        """Check if date of birth is valid."""
        result = QualityCheckResult(
            check_name=f"{self.entity_name}_date_of_birth_validity"
        )
        result.total_rows_checked = len(df)

        if "date_of_birth" not in df.columns:
            result.passed = True
            return result

        try:
            dob = pd.to_datetime(df["date_of_birth"], errors="coerce")
        except Exception:
            result.passed = False
            issue = QualityIssue(
                check_name=result.check_name,
                severity=SeverityLevel.CRITICAL,
                description="Date of birth parsing failed",
                affected_rows=len(df),
                affected_columns=["date_of_birth"],
            )
            result.add_issue(issue)
            return result

        # Check if age is between 18 and 100
        today = pd.Timestamp.now()
        age = (today - dob).dt.days / 365.25
        invalid_age = (age < 18) | (age > 100)
        invalid_count = invalid_age.sum()

        if invalid_count > 0:
            result.passed = False
            issue = QualityIssue(
                check_name=result.check_name,
                severity=SeverityLevel.WARNING,
                description="Invalid driver age detected",
                affected_rows=invalid_count,
                affected_columns=["date_of_birth"],
            )
            result.add_issue(issue)
        else:
            result.passed = True

        return result

    def check_gender_values(self, df: pd.DataFrame) -> QualityCheckResult:
        """Check if gender values are valid."""
        result = QualityCheckResult(check_name=f"{self.entity_name}_gender_values")
        result.total_rows_checked = len(df)

        if "gender" not in df.columns:
            result.passed = True
            return result

        valid_genders = {"M", "F", "Other", None}
        invalid_gender = ~df["gender"].isin(valid_genders)
        invalid_count = invalid_gender.sum()

        if invalid_count > 0:
            result.passed = False
            issue = QualityIssue(
                check_name=result.check_name,
                severity=SeverityLevel.WARNING,
                description="Invalid gender values detected",
                affected_rows=invalid_count,
                affected_columns=["gender"],
            )
            result.add_issue(issue)
        else:
            result.passed = True

        return result

    def check_no_duplicates(self, df: pd.DataFrame) -> QualityCheckResult:
        """Check for duplicate driver IDs."""
        return self.check_duplicates(df, ["person_id"])

    def run_all_checks(self, df: pd.DataFrame) -> list[QualityCheckResult]:
        """Execute all quality checks on drivers."""
        results = [
            self.check_schema(df),
            self.check_not_null_constraints(df),
            self.check_date_of_birth_validity(df),
            self.check_gender_values(df),
            self.check_no_duplicates(df),
        ]
        return results

    def get_summary(self, results: list[QualityCheckResult]) -> dict[str, Any]:
        """Generate summary of quality checks."""
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
