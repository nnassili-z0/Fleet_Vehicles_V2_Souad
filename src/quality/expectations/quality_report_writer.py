"""
Quality report management for artifact tracking.

Handles creation, persistence, and retrieval of quality check reports.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from src.config.settings import settings
from src.infrastructure.logging_config import get_logger

logger = get_logger(__name__)


class QualityReportWriter:
    """Writes quality check results to artifacts."""

    def __init__(self, report_dir: Path | None = None):
        """
        Initialize report writer.

        Args:
            report_dir: Directory to write reports (default: settings.quality_reports_dir)
        """
        self.report_dir = report_dir or settings.path_config.quality_reports_dir
        self.report_dir.mkdir(parents=True, exist_ok=True)

    def write_report(
        self,
        entity_type: str,
        pipeline_stage: str,
        results: dict[str, Any],
        execution_id: str | None = None,
    ) -> Path:
        """
        Write quality check report to file.

        Args:
            entity_type: Type of entity (vehicle, driver, event)
            pipeline_stage: Pipeline stage (bronze_to_silver, silver_to_gold)
            results: Quality check results summary
            execution_id: Optional execution ID for tracing

        Returns:
            Path to written report file
        """
        execution_id = execution_id or datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        timestamp = datetime.utcnow().isoformat()

        report = {
            "metadata": {
                "timestamp": timestamp,
                "execution_id": execution_id,
                "entity_type": entity_type,
                "pipeline_stage": pipeline_stage,
            },
            "results": results,
        }

        # Create filename with timestamp
        filename = (
            f"quality_{pipeline_stage}_{entity_type}_{execution_id}.json"
        )
        filepath = self.report_dir / filename

        try:
            with open(filepath, "w") as f:
                json.dump(report, f, indent=2, default=str)
            logger.info(f"Quality report written: {filepath}")
            return filepath
        except Exception as e:
            logger.error(f"Failed to write quality report: {e}")
            raise

    def write_batch_report(
        self,
        pipeline_stage: str,
        entity_results: dict[str, dict[str, Any]],
        execution_id: str | None = None,
    ) -> Path:
        """
        Write batch quality check report for multiple entities.

        Args:
            pipeline_stage: Pipeline stage name
            entity_results: Dict of {entity_type: results}
            execution_id: Optional execution ID for tracing

        Returns:
            Path to written report file
        """
        execution_id = execution_id or datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        timestamp = datetime.utcnow().isoformat()

        report = {
            "metadata": {
                "timestamp": timestamp,
                "execution_id": execution_id,
                "pipeline_stage": pipeline_stage,
                "entity_count": len(entity_results),
            },
            "entities": entity_results,
        }

        filename = f"quality_batch_{pipeline_stage}_{execution_id}.json"
        filepath = self.report_dir / filename

        try:
            with open(filepath, "w") as f:
                json.dump(report, f, indent=2, default=str)
            logger.info(f"Batch quality report written: {filepath}")
            return filepath
        except Exception as e:
            logger.error(f"Failed to write batch quality report: {e}")
            raise

    @staticmethod
    def read_report(filepath: Path) -> dict[str, Any]:
        """
        Read quality report from file.

        Args:
            filepath: Path to report file

        Returns:
            Report data
        """
        try:
            with open(filepath) as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to read quality report: {e}")
            raise

    def get_latest_reports(self, limit: int = 10) -> list[Path]:
        """
        Get latest quality reports.

        Args:
            limit: Maximum number of reports to return

        Returns:
            List of report paths, sorted by modification time (newest first)
        """
        json_files = sorted(
            self.report_dir.glob("quality_*.json"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        return json_files[:limit]
