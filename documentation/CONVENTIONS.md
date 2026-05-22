# Fleet Vehicles V2 - Code Standards and Naming Conventions

## Overview

This document establishes industrial-grade coding standards for the Fleet Vehicles V2 data platform. All contributions must adhere to these conventions to maintain code quality, readability, and maintainability.

## Design Principles

### SOLID Principles

**S - Single Responsibility Principle**
- Each class/function has exactly one reason to change
- Example: `EventQualityChecker` handles event validation only, not vehicle validation

**O - Open/Closed Principle**
- Open for extension, closed for modification
- Use inheritance and composition for new features
- Example: `BaseQualityChecker` provides extension points for entity-specific checks

**L - Liskov Substitution Principle**
- Derived classes can substitute base classes without breaking functionality
- All quality checkers implement the same interface

**I - Interface Segregation Principle**
- Don't force clients to depend on interfaces they don't use
- Separate data models for different layers (Bronze, Silver, Gold)

**D - Dependency Inversion Principle**
- Depend on abstractions, not concretions
- Use configuration injection instead of hardcoding

### DRY (Don't Repeat Yourself)

- Extract common logic into reusable functions
- Use base classes for shared behavior
- Centralize configuration in `src/config/settings.py`

### KISS (Keep It Simple, Stupid)

- Solve problems with straightforward approaches
- Avoid over-engineering
- Prefer clarity over cleverness
- Use standard patterns and libraries

### YAGNI (You Aren't Gonna Need It)

- Don't add features until they're needed
- Remove speculative code
- Focus on current requirements

## Architecture Principles

### Separation of Concerns (SoC)

```
Config Layer          - All settings management
   ↓
Infrastructure Layer  - Logging, connections, utilities
   ↓
Quality Layer         - Detection and validation logic
   ↓
Pipeline Layer        - Transformation and processing
   ↓
Orchestration Layer   - DAG definitions and scheduling
```

### Clean Architecture

```
Entities
  ↓
Use Cases (Business Logic)
  ↓
Interface Adapters (Transformers)
  ↓
Frameworks & Drivers (Airflow, Docker)
```

### Pola (Polars Integration)

Currently using Pandas, but architecture supports future Polars migration:
- Use DataFrame-agnostic abstractions where possible
- Document data frame operations for easy porting

## Naming Conventions

### Python Modules & Packages

```python
# Module names: lowercase with underscores
src/pipelines/bronze_to_silver/transformer.py
src/quality/detectors/event_quality_checker.py

# Package init files: include docstring
src/pipelines/__init__.py
```

### Classes

```python
# PascalCase
class BronzeToSilverTransformer:
    pass

class EventQualityChecker:
    pass

class QualityReportWriter:
    pass

# Naming conventions:
# - "Checker" suffix for validation classes
# - "Transformer" suffix for transformation classes
# - "Writer" suffix for persistence classes
# - "Factory" suffix for object creation classes
```

### Functions & Methods

```python
# snake_case for all functions and methods
def load_bronze_vehicles() -> pd.DataFrame:
    pass

def check_timestamp_validity(df: pd.DataFrame) -> QualityCheckResult:
    pass

def write_gold_data(prefix: str, df: pd.DataFrame) -> None:
    pass

# Naming conventions:
# - Verb prefix for actions: load_, check_, write_, create_, run_
# - Positive naming: "validate_" not "reject_"
# - Question methods return bool: "has_", "is_", "should_"
```

### Variables & Constants

```python
# Constants: UPPER_SNAKE_CASE
EXPECTED_COLUMNS = {"auto_id", "vin", "make"}
NOT_NULL_COLUMNS = {"auto_id", "full_name"}
DEFAULT_BATCH_SIZE = 10000

# Variables: snake_case
vehicle_df = pd.read_csv(...)
quality_results = checker.run_all_checks(df)
execution_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

# Boolean variables: use is_/has_/should_ prefix
is_valid = result.passed
has_errors = len(issues) > 0
should_proceed = not result.has_critical_issues()
```

### Data Engineering Specific

```python
# Data layers: prefix with layer name
bronze_vehicle        # From Bronze layer
silver_vehicle        # From Silver layer
fact_driving_event    # Fact table in Gold
dim_vehicle          # Dimension table in Gold

# Quality entities: quality_ prefix for checks
quality_report       # Report of quality checks
quality_issue        # Single issue found
quality_checker      # Checker instance

# Metrics/KPIs: kpi_ prefix
kpi_vehicle         # Vehicle KPI view
kpi_driver          # Driver KPI view

# Artifacts: artifact_ or with specific type
artifact_log        # Log file
artifact_report     # Quality report
```

### Database Objects

```python
# Table names: lowercase, descriptive
bronze_vehicle       # Bronze layer table
silver_driver        # Silver layer table
fact_maintenance_alert  # Fact table
dim_date            # Dimension table

# Column names: snake_case, descriptive
auto_id             # Vehicle identifier
person_id           # Person identifier
event_timestamp     # Event date/time
is_speeding         # Boolean flag
created_at          # Audit column
updated_at          # Audit column

# Surrogate keys: _key suffix
vehicle_key
driver_key
event_key
```

## Code Style

### Python Style Guide (PEP 8 + Enhancements)

**Line Length:** 88 characters (Black formatter standard)

```python
# Good
long_variable_name = some_function(
    arg1, arg2, arg3
)

# Avoid
very_long_function_name(very_long_argument_1, very_long_argument_2, very_long_argument_3)
```

**Imports**

```python
# Order: stdlib, third-party, local
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from src.config.settings import settings
from src.infrastructure.logging_config import get_logger
```

**Type Hints**

```python
# Always use type hints for function signatures
def load_data(filepath: Path, encoding: str = "utf-8") -> pd.DataFrame:
    """Load data from CSV file."""
    pass

# Return type for all functions
def validate(df: pd.DataFrame) -> bool:
    pass

# Use Optional for nullable values
def get_value(key: str) -> Optional[str]:
    pass

# Use Union for multiple types
from typing import Union
def process(data: Union[pd.DataFrame, list]) -> dict[str, Any]:
    pass
```

**Docstrings** (Google/NumPy Style)

```python
def check_value_range(
    self, df: pd.DataFrame, column: str, min_val: float, max_val: float
) -> QualityCheckResult:
    """
    Check if numeric column values are within expected range.

    Validates that all values in the specified column fall within
    the provided range. Issues a warning-level quality issue if
    violations are found.

    Args:
        df: Input dataframe to validate
        column: Column name to check
        min_val: Minimum acceptable value (inclusive)
        max_val: Maximum acceptable value (inclusive)

    Returns:
        QualityCheckResult containing check status and any issues found.
        Severity: WARNING if violations detected, PASSED if all values valid.

    Raises:
        ValueError: If column doesn't exist in dataframe

    Example:
        >>> result = checker.check_value_range(events_df, "vehicle_speed", 0, 350)
        >>> if result.passed:
        ...     print("All speeds valid")
    """
    pass
```

**Logging**

```python
from src.infrastructure.logging_config import get_logger

logger = get_logger(__name__)

# Usage
logger.debug("Detailed information for diagnosis")
logger.info("Successful operation")
logger.warning("Warning: something unexpected")
logger.error("Error: operation failed")
logger.critical("Critical: system cannot continue")

# Good examples
logger.info(f"Loading {len(df)} vehicles from {path}")
logger.error(f"Failed to process {entity_type}: {str(e)}")
logger.warning(f"Found {duplicate_count} duplicates in {column}")
```

**Error Handling**

```python
# Specific exceptions, not bare except
try:
    result = transformer.run_pipeline()
except FileNotFoundError as e:
    logger.error(f"Data file not found: {e}")
    raise
except Exception as e:
    logger.error(f"Unexpected error: {e}")
    raise

# Use context managers for resource management
from pathlib import Path

report_path = settings.path_config.logs_dir / "report.json"
with open(report_path, "w") as f:
    json.dump(report, f, indent=2)
```

## Project Structure Conventions

### Pipeline Modules

Each pipeline should follow this structure:

```python
# File: src/pipelines/{layer}_to_{layer}/transformer.py

"""
{Source} to {Target} pipeline — [Brief description]

Implements [key transformations] following [design principles].
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from src.config.settings import settings
from src.infrastructure.logging_config import get_logger

logger = get_logger(__name__)


class {SourceTarget}Transformer:
    """Transform {source} data to {target} layer."""

    def __init__(self, execution_id: str | None = None):
        """Initialize transformer."""
        self.execution_id = execution_id or datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        settings.ensure_directories_exist()

    # Data loading methods
    def load_{source}_data(self) -> pd.DataFrame:
        """Load {source} data."""
        pass

    # Transformation methods
    def transform_{entity}(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform {entity} data."""
        pass

    # Output methods
    def write_{target}_{entity}(self, df: pd.DataFrame) -> Path:
        """Write {entity} to {target} layer."""
        pass

    # Orchestration
    def run_full_pipeline(self) -> dict[str, Any]:
        """Execute complete pipeline."""
        pass
```

### Quality Checker Modules

```python
# File: src/quality/detectors/{entity}_quality_checker.py

class {Entity}QualityChecker(BaseQualityChecker):
    """Quality checks specific to {entity} data."""

    EXPECTED_COLUMNS = {...}
    NOT_NULL_COLUMNS = {...}

    def __init__(self):
        super().__init__("{entity_lower}")

    # Entity-specific checks
    def check_{specific_rule}(self, df: pd.DataFrame) -> QualityCheckResult:
        """Check specific business rule."""
        pass

    # Orchestration
    def run_all_checks(self, df: pd.DataFrame) -> list[QualityCheckResult]:
        """Execute all checks."""
        pass

    def get_summary(self, results: list[QualityCheckResult]) -> dict[str, Any]:
        """Generate summary."""
        pass
```

### DAG Modules

```python
# File: src/orchestration/dags/dag_{workflow}.py

"""
{Workflow} DAG — [Brief description]

This DAG:
1. [Step 1]
2. [Step 2]
3. [Step 3]

Schedule: [Schedule expression]
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

DAG_CONFIG = {
    "dag_id": "medallion_{workflow_name}",
    "default_args": DEFAULT_ARGS,
    "description": "[Full description]",
    "schedule_interval": "[Schedule]",
    "catchup": False,
    "tags": ["medallion", "[layer]", "[type]"],
}

def task_function(**context) -> dict:
    """Task implementation."""
    pass

dag = DAG(**DAG_CONFIG)
task = PythonOperator(task_id="...", python_callable=task_function, dag=dag)
```

## Data Model Conventions

### Schema Definition

```python
@dataclass
class SilverVehicle:
    """Cleaned and validated vehicle data.

    This model represents the Silver layer vehicle data after
    data quality checks and standardization. All fields follow
    industrial naming conventions and include proper type hints.
    """

    # Identifiers (never change)
    auto_id: str
    vin: str

    # Attributes
    make: str
    year: Optional[int]
    transmission: Optional[str]
    owner_id: str

    # Derived attributes
    owner_known: bool

    # Audit columns
    created_at: datetime
    updated_at: datetime

    class Meta:
        table_name: str = "silver_vehicle"
        schema: str = "silver"
```

## Testing Conventions

### Test File Organization

```
tests/
├── unit/
│   ├── test_quality_checkers.py
│   ├── test_transformers.py
│   └── test_config.py
├── integration/
│   ├── test_bronze_to_silver.py
│   └── test_silver_to_gold.py
└── fixtures/
    ├── sample_bronze_data.csv
    └── expected_results.json
```

### Test Naming

```python
def test_{function_or_method}_{scenario}_{expected_result}():
    """Test description."""
    pass

# Examples
def test_check_value_range_within_limits_passes():
    pass

def test_check_value_range_outliers_detected():
    pass

def test_load_bronze_vehicles_file_not_found_raises_error():
    pass
```

## Documentation Conventions

### Module Docstrings

```python
"""
Module description — One-line summary.

Detailed description of module purpose, key classes/functions,
and usage patterns. Can span multiple paragraphs.

Classes:
    TransformerClass: Main transformation logic
    ValidatorClass: Data validation

Functions:
    main_function: Entry point

Example:
    >>> from module import function
    >>> result = function(data)
"""
```

### Inline Comments

```python
# Good: Explains WHY, not WHAT
# Remove odometer outliers (> 500k km likely erroneous)
df = df[df["odometer"] < 500000].copy()

# Avoid: Comments that just repeat code
# Set created_at to current time
df["created_at"] = datetime.utcnow()
```

## Version Control Conventions

### Commit Messages

```
<type>(<scope>): <subject>

<body>

<footer>

# Type: feat, fix, refactor, docs, test, chore, perf
# Scope: area affected (bronze_to_silver, quality_checks, etc.)
# Subject: < 50 characters, imperative mood
# Body: Detailed explanation (wrap at 72 characters)
# Footer: Closes issue references

Examples:
feat(bronze_to_silver): add timestamp validation for events
fix(quality_checker): correct odometer monotonic check logic
refactor(transformers): extract common logic to base class
docs: add deployment guide
```

### Branch Naming

```
<type>/<description>

Types:
- feature/    : new functionality
- bugfix/     : bug fixes
- refactor/   : code refactoring
- docs/       : documentation
- test/       : tests

Examples:
feature/implement-star-schema
bugfix/null-handling-in-events
refactor/extract-quality-checks
docs/add-api-documentation
```

## Performance Optimization

### Code Patterns to Follow

```python
# Good: Vectorized pandas operations
df["speed_category"] = pd.cut(df["vehicle_speed"], bins=[...])

# Avoid: Row-by-row operations
for idx, row in df.iterrows():
    df.at[idx, "speed_category"] = categorize(row["vehicle_speed"])

# Good: Use groupby for aggregations
results = df.groupby("auto_id").agg({"odometer": "max"})

# Avoid: Multiple passes through data
for auto_id in df["auto_id"].unique():
    subset = df[df["auto_id"] == auto_id]
    # process subset
```

### Memory Efficiency

```python
# Read large files in chunks
for chunk in pd.read_csv("large_file.csv", chunksize=10000):
    process_chunk(chunk)

# Use appropriate data types
df["auto_id"] = df["auto_id"].astype("category")  # Save memory
df["year"] = pd.to_numeric(df["year"], downcast="integer")

# Delete unused data
del large_dataframe
gc.collect()
```

## Code Review Checklist

- [ ] Follows naming conventions
- [ ] Type hints present on all functions
- [ ] Docstrings complete (Google style)
- [ ] SOLID principles applied
- [ ] No code duplication (DRY)
- [ ] Appropriate error handling
- [ ] Logging statements present
- [ ] Tests included
- [ ] No magic numbers/strings (use constants)
- [ ] Configuration externalized
- [ ] Performance acceptable
- [ ] Security considerations addressed

## Related Documentation

- [ARCHITECTURE.md](./ARCHITECTURE.md) - System architecture
- [DEPLOYMENT.md](./DEPLOYMENT.md) - Deployment guide
