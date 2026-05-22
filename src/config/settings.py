"""
Configuration settings for Fleet Vehicles V2 data platform.

This module provides centralized configuration management following the
separation of concerns principle. All environment-specific settings are
defined here to avoid magic strings and enable environment-aware behavior.

Naming Conventions:
  - CONSTANT_CASE for configuration constants
  - snake_case for variables and functions
  - Prefixes indicate purpose (PATH_, DB_, QUALITY_)
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Final

import yaml


@dataclass
class PathConfig:
    """File system paths for data layers and artifacts."""

    # Data layers
    bronze_dir: Path
    silver_dir: Path
    silver_data_dir: Path
    gold_dir: Path
    gold_data_dir: Path

    # Temporary working directories
    temp_dir: Path

    # Artifacts
    logs_dir: Path
    quality_reports_dir: Path
    artifacts_dir: Path


@dataclass
class DatabaseConfig:
    """Database connection parameters."""

    db_type: str
    host: str
    port: int
    database: str
    username: str
    password: str

    @property
    def connection_string(self) -> str:
        """Generate database connection string."""
        if self.db_type == "postgresql":
            return (
                f"postgresql+psycopg2://{self.username}:{self.password}"
                f"@{self.host}:{self.port}/{self.database}"
            )
        raise ValueError(f"Unsupported database type: {self.db_type}")


@dataclass
class QualityConfig:
    """Data quality check parameters."""

    validation_enabled: bool
    anomaly_detection_enabled: bool
    outlier_threshold_percentile: float
    max_null_percentage: float
    checkpoint_enabled: bool


class Settings:
    """Main configuration class - loads environment and defaults."""

    # Platform mode
    ENVIRONMENT: Final[str] = os.getenv("ENV", "development")
    DEBUG: Final[bool] = ENVIRONMENT != "production"

    # Airflow
    AIRFLOW_HOME: Final[Path] = Path(os.getenv("AIRFLOW_HOME", "/opt/airflow"))

    # Paths
    PROJECT_ROOT: Final[Path] = Path(__file__).parent.parent.parent
    DATA_DIR: Final[Path] = PROJECT_ROOT / "data"

    path_config: PathConfig = PathConfig(
        bronze_dir=DATA_DIR / "bronze",
        silver_dir=DATA_DIR / "silver",
        silver_data_dir=DATA_DIR / "silver" / "data",
        gold_dir=DATA_DIR / "gold",
        gold_data_dir=DATA_DIR / "gold" / "data",
        temp_dir=DATA_DIR / ".temp",
        logs_dir=PROJECT_ROOT / "artifacts" / "logs",
        quality_reports_dir=PROJECT_ROOT / "artifacts" / "quality_reports",
        artifacts_dir=PROJECT_ROOT / "artifacts",
    )

    # Database
    db_config: DatabaseConfig = DatabaseConfig(
        db_type=os.getenv("DB_TYPE", "postgresql"),
        host=os.getenv("DB_HOST", "postgres"),
        port=int(os.getenv("DB_PORT", "5432")),
        database=os.getenv("DB_NAME", "fleet_vehicles"),
        username=os.getenv("DB_USER", "airflow"),
        password=os.getenv("DB_PASSWORD", "airflow"),
    )

    # Data Quality
    quality_config: QualityConfig = QualityConfig(
        validation_enabled=os.getenv("QUALITY_VALIDATION_ENABLED", "true").lower()
        == "true",
        anomaly_detection_enabled=os.getenv("ANOMALY_DETECTION_ENABLED", "true").lower()
        == "true",
        outlier_threshold_percentile=float(
            os.getenv("OUTLIER_THRESHOLD_PERCENTILE", "0.5")
        ),
        max_null_percentage=float(os.getenv("MAX_NULL_PERCENTAGE", "10.0")),
        checkpoint_enabled=os.getenv("QUALITY_CHECKPOINT_ENABLED", "true").lower()
        == "true",
    )

    # Pipelines
    BRONZE_TO_SILVER_BATCH_SIZE: Final[int] = int(
        os.getenv("BRONZE_TO_SILVER_BATCH_SIZE", "10000")
    )
    SILVER_TO_GOLD_BATCH_SIZE: Final[int] = int(
        os.getenv("SILVER_TO_GOLD_BATCH_SIZE", "50000")
    )

    @classmethod
    def ensure_directories_exist(cls) -> None:
        """Create required directories if they don't exist."""
        cls.path_config.bronze_dir.mkdir(parents=True, exist_ok=True)
        cls.path_config.silver_dir.mkdir(parents=True, exist_ok=True)
        cls.path_config.silver_data_dir.mkdir(parents=True, exist_ok=True)
        cls.path_config.gold_dir.mkdir(parents=True, exist_ok=True)
        cls.path_config.gold_data_dir.mkdir(parents=True, exist_ok=True)
        cls.path_config.temp_dir.mkdir(parents=True, exist_ok=True)
        cls.path_config.logs_dir.mkdir(parents=True, exist_ok=True)
        cls.path_config.quality_reports_dir.mkdir(parents=True, exist_ok=True)

    @classmethod
    def load_yaml(cls, config_path: str | Path) -> dict:
        """Load configuration from YAML file."""
        with open(config_path) as f:
            return yaml.safe_load(f)


# Global settings instance
settings = Settings()
