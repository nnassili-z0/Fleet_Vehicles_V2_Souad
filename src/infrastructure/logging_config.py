"""
Logging infrastructure for Fleet Vehicles V2.

Provides centralized logging configuration following SoC principle.
All logging is configured here to maintain consistency across the platform.

Naming Conventions:
  - Logger names: module.submodule format
  - Log levels: DEBUG, INFO, WARNING, ERROR, CRITICAL
"""

from __future__ import annotations

import logging
import logging.handlers
from pathlib import Path
from typing import Final

from src.config.settings import settings


class LoggerFactory:
    """Factory for creating consistently configured loggers."""

    _LOG_FORMAT: Final[str] = (
        "[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s"
    )
    _DATE_FORMAT: Final[str] = "%Y-%m-%d %H:%M:%S"

    @staticmethod
    def create_logger(
        name: str,
        level: int = logging.INFO,
        add_file_handler: bool = False,
        log_dir: Path | None = None,
    ) -> logging.Logger:
        """
        Create and configure a logger instance.

        Args:
            name: Logger name (typically __name__)
            level: Logging level (default: INFO)
            add_file_handler: Whether to add file handler
            log_dir: Directory for log files (default: settings.logs_dir)

        Returns:
            Configured logger instance
        """
        logger = logging.getLogger(name)
        logger.setLevel(level)

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(
            logging.Formatter(LoggerFactory._LOG_FORMAT, LoggerFactory._DATE_FORMAT)
        )
        logger.addHandler(console_handler)

        # File handler if requested
        if add_file_handler:
            log_dir = log_dir or settings.path_config.logs_dir
            log_file = log_dir / f"{name}.log"
            file_handler = logging.handlers.RotatingFileHandler(
                log_file, maxBytes=10_485_760, backupCount=5
            )  # 10MB
            file_handler.setFormatter(
                logging.Formatter(LoggerFactory._LOG_FORMAT, LoggerFactory._DATE_FORMAT)
            )
            logger.addHandler(file_handler)

        return logger


def get_logger(name: str) -> logging.Logger:
    """
    Get or create logger for a module.

    Args:
        name: Module name (typically __name__)

    Returns:
        Logger instance
    """
    return LoggerFactory.create_logger(name, add_file_handler=True)
