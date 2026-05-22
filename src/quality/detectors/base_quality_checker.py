"""
Data Quality framework for Fleet Vehicles V2.

Implements data quality checks using Great Expectations principles.
Provides detection and validation logic that can be used in both
detection and implementation phases.

Naming Conventions:
  - Check classes: {Entity}QualityCheck
  - Check methods: check_{specific_rule}
  - Results: QualityCheckResult
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable

import pandas as pd

from src.infrastructure.logging_config import get_logger

logger = get_logger(__name__)


class SeverityLevel(Enum):
    """Severity levels for quality issues."""

    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


@dataclass
class QualityIssue:
    """Represents a single quality issue."""

    check_name: str
    severity: SeverityLevel
    description: str
    affected_rows: int
    affected_columns: list[str] = field(default_factory=list)
    details: dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> dict[str, Any]:
        """Convert issue to dictionary format for serialization."""
        return {
            "check_name": self.check_name,
            "severity": self.severity.value,
            "description": self.description,
            "affected_rows": self.affected_rows,
            "affected_columns": self.affected_columns,
            "details": self.details,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class QualityCheckResult:
    """Result of a quality check execution."""

    check_name: str
    passed: bool = True
    issues: list[QualityIssue] = field(default_factory=list)
    total_rows_checked: int = 0
    execution_time_ms: float = 0.0

    def add_issue(self, issue: QualityIssue) -> None:
        """Add an issue to the result."""
        self.issues.append(issue)

    def has_critical_issues(self) -> bool:
        """Check if result contains critical severity issues."""
        return any(issue.severity == SeverityLevel.CRITICAL for issue in self.issues)

    def to_dict(self) -> dict[str, Any]:
        """Convert result to dictionary format."""
        return {
            "check_name": self.check_name,
            "passed": self.passed,
            "issues": [issue.to_dict() for issue in self.issues],
            "total_rows_checked": self.total_rows_checked,
            "execution_time_ms": self.execution_time_ms,
        }


class BaseQualityChecker:
    """
    Base class for quality checkers.

    Implements common quality checking logic following SoC and SOLID principles.
    Subclasses implement specific business rules via the check_* methods.
    """

    def __init__(self, entity_name: str):
        """Initialize quality checker."""
        self.entity_name = entity_name
        self.logger = logger

    def check_schema_consistency(
        self, df: pd.DataFrame, expected_columns: set[str]
    ) -> QualityCheckResult:
        """
        Check if dataframe has expected schema.

        Args:
            df: Input dataframe
            expected_columns: Set of expected column names

        Returns:
            QualityCheckResult
        """
        result = QualityCheckResult(
            check_name=f"{self.entity_name}_schema_consistency"
        )
        result.total_rows_checked = len(df)

        actual_columns = set(df.columns)
        missing_columns = expected_columns - actual_columns
        extra_columns = actual_columns - expected_columns

        if missing_columns or extra_columns:
            result.passed = False
            issue = QualityIssue(
                check_name=result.check_name,
                severity=SeverityLevel.CRITICAL,
                description="Schema inconsistency detected",
                affected_rows=len(df),
                affected_columns=list(missing_columns | extra_columns),
                details={
                    "missing_columns": list(missing_columns),
                    "extra_columns": list(extra_columns),
                },
            )
            result.add_issue(issue)
        else:
            result.passed = True

        return result

    def check_null_constraints(
        self, df: pd.DataFrame, not_null_columns: set[str]
    ) -> QualityCheckResult:
        """
        Check for null values in columns that should not have them.

        Args:
            df: Input dataframe
            not_null_columns: Column names that should not have nulls

        Returns:
            QualityCheckResult
        """
        result = QualityCheckResult(
            check_name=f"{self.entity_name}_null_constraints"
        )
        result.total_rows_checked = len(df)

        violations = {}
        for col in not_null_columns:
            if col in df.columns:
                null_count = df[col].isnull().sum()
                if null_count > 0:
                    violations[col] = null_count

        if violations:
            result.passed = False
            issue = QualityIssue(
                check_name=result.check_name,
                severity=SeverityLevel.ERROR,
                description="Null constraint violation",
                affected_rows=sum(violations.values()),
                affected_columns=list(violations.keys()),
                details=violations,
            )
            result.add_issue(issue)
        else:
            result.passed = True

        return result

    def check_data_types(
        self, df: pd.DataFrame, expected_types: dict[str, str]
    ) -> QualityCheckResult:
        """
        Check if columns have expected data types.

        Args:
            df: Input dataframe
            expected_types: Dict of {column_name: expected_dtype}

        Returns:
            QualityCheckResult
        """
        result = QualityCheckResult(check_name=f"{self.entity_name}_data_types")
        result.total_rows_checked = len(df)

        violations = {}
        for col, expected_type in expected_types.items():
            if col in df.columns:
                actual_type = str(df[col].dtype)
                if actual_type != expected_type:
                    violations[col] = {
                        "expected": expected_type,
                        "actual": actual_type,
                    }

        if violations:
            result.passed = False
            issue = QualityIssue(
                check_name=result.check_name,
                severity=SeverityLevel.WARNING,
                description="Data type mismatch",
                affected_rows=len(df),
                affected_columns=list(violations.keys()),
                details=violations,
            )
            result.add_issue(issue)
        else:
            result.passed = True

        return result

    def check_value_range(
        self, df: pd.DataFrame, column: str, min_val: float, max_val: float
    ) -> QualityCheckResult:
        """
        Check if numeric column values are within expected range.

        Args:
            df: Input dataframe
            column: Column name to check
            min_val: Minimum acceptable value
            max_val: Maximum acceptable value

        Returns:
            QualityCheckResult
        """
        result = QualityCheckResult(check_name=f"{self.entity_name}_value_range_{column}")
        result.total_rows_checked = len(df)

        if column not in df.columns:
            result.passed = False
            issue = QualityIssue(
                check_name=result.check_name,
                severity=SeverityLevel.ERROR,
                description=f"Column '{column}' not found",
                affected_rows=0,
            )
            result.add_issue(issue)
            return result

        out_of_range = (df[column] < min_val) | (df[column] > max_val)
        violation_count = out_of_range.sum()

        if violation_count > 0:
            result.passed = False
            issue = QualityIssue(
                check_name=result.check_name,
                severity=SeverityLevel.WARNING,
                description=f"Values outside range [{min_val}, {max_val}]",
                affected_rows=violation_count,
                affected_columns=[column],
                details={
                    "min_expected": min_val,
                    "max_expected": max_val,
                    "min_actual": float(df[column].min()),
                    "max_actual": float(df[column].max()),
                },
            )
            result.add_issue(issue)
        else:
            result.passed = True

        return result

    def check_duplicates(
        self, df: pd.DataFrame, key_columns: list[str]
    ) -> QualityCheckResult:
        """
        Check for duplicate rows based on key columns.

        Args:
            df: Input dataframe
            key_columns: Columns that define uniqueness

        Returns:
            QualityCheckResult
        """
        result = QualityCheckResult(check_name=f"{self.entity_name}_duplicates")
        result.total_rows_checked = len(df)

        # Filter to only existing columns
        valid_key_cols = [col for col in key_columns if col in df.columns]

        if not valid_key_cols:
            result.passed = True
            return result

        duplicates_mask = df.duplicated(subset=valid_key_cols, keep=False)
        duplicate_count = duplicates_mask.sum()

        if duplicate_count > 0:
            result.passed = False
            issue = QualityIssue(
                check_name=result.check_name,
                severity=SeverityLevel.ERROR,
                description="Duplicate rows detected",
                affected_rows=duplicate_count,
                affected_columns=valid_key_cols,
                details={"duplicate_count": duplicate_count},
            )
            result.add_issue(issue)
        else:
            result.passed = True

        return result
