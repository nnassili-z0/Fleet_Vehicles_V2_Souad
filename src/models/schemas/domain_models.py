"""
Data models and schemas for medallion architecture.

Defines the structure of data at each layer (Bronze, Silver, Gold)
following clean architecture principles. Each model is self-documenting
and can be used for validation and type checking.

Naming Conventions:
  - Class names: PascalCase
  - Column names: snake_case
  - Prefixes indicate entity type (fact_, dim_, src_)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional


# ========================================
# BRONZE LAYER - RAW DATA MODELS
# ========================================


@dataclass
class BronzeVehicle:
    """Raw vehicle data from source system."""

    auto_id: str
    vin: str
    make: str
    year: Optional[int]
    transmission: Optional[str]
    owner_id: str

    class Meta:
        table_name: str = "bronze_vehicle"
        schema: str = "bronze"


@dataclass
class BronzeDriver:
    """Raw driver data from source system."""

    person_id: str
    full_name: str
    gender: Optional[str]
    date_of_birth: Optional[date]

    class Meta:
        table_name: str = "bronze_driver"
        schema: str = "bronze"


@dataclass
class BronzeEvent:
    """Raw telematics event from vehicle."""

    event_id: str
    auto_id: str
    timestamp: datetime
    odometer: float
    vehicle_speed: float
    fuel_level: float
    fuel_consumed_since_restart: float
    engine_speed: float
    torque_at_transmission: float
    accelerator_pedal_position: float
    steering_wheel_angle: float
    latitude: float
    longitude: float
    brake_pedal_status: bool
    high_beam_status: bool
    windshield_wiper_status: bool
    headlamp_status: bool
    parking_brake_status: bool

    class Meta:
        table_name: str = "bronze_event"
        schema: str = "bronze"


# ========================================
# SILVER LAYER - CLEANED DATA MODELS
# ========================================


@dataclass
class SilverVehicle:
    """Cleaned and validated vehicle data."""

    auto_id: str
    vin: str
    make: str
    year: Optional[int]
    transmission: Optional[str]
    owner_id: str
    owner_known: bool
    created_at: datetime
    updated_at: datetime

    class Meta:
        table_name: str = "silver_vehicle"
        schema: str = "silver"


@dataclass
class SilverDriver:
    """Cleaned and validated driver data."""

    person_id: str
    full_name: str
    gender: Optional[str]
    date_of_birth: Optional[date]
    age: Optional[int]
    created_at: datetime
    updated_at: datetime

    class Meta:
        table_name: str = "silver_driver"
        schema: str = "silver"


@dataclass
class SilverEvent:
    """Cleaned and validated telematics event."""

    event_id: str
    auto_id: str
    timestamp: datetime
    event_date: date
    odometer: float
    vehicle_speed: float
    fuel_level: float
    fuel_consumed_since_restart: float
    engine_speed: float
    torque_at_transmission: float
    accelerator_pedal_position: float
    steering_wheel_angle: float
    latitude: float
    longitude: float
    brake_pedal_status: bool
    high_beam_status: bool
    windshield_wiper_status: bool
    headlamp_status: bool
    parking_brake_status: bool
    speed_category: Optional[str]
    driving_mode: Optional[str]
    fuel_alert: bool
    is_braking: bool
    is_accelerating: bool
    created_at: datetime
    updated_at: datetime

    class Meta:
        table_name: str = "silver_event"
        schema: str = "silver"


# ========================================
# GOLD LAYER - FACT & DIMENSION MODELS
# ========================================


@dataclass
class DimDate:
    """Date dimension for temporal analysis."""

    date_id: int
    date: date
    year: int
    quarter: int
    month: int
    day: int
    day_of_week: int
    is_weekend: bool
    week_of_year: int

    class Meta:
        table_name: str = "dim_date"
        schema: str = "gold"


@dataclass
class DimTime:
    """Time dimension for temporal analysis."""

    time_id: int
    hour: int
    minute: int
    second: int
    time_period: str  # morning, afternoon, evening, night

    class Meta:
        table_name: str = "dim_time"
        schema: str = "gold"


@dataclass
class DimVehicle:
    """Vehicle dimension for analysis."""

    vehicle_key: int
    auto_id: str
    vin: str
    make: str
    year: Optional[int]
    transmission: Optional[str]
    owner_id: str
    effective_date: date
    end_date: Optional[date]
    is_current: bool

    class Meta:
        table_name: str = "dim_vehicle"
        schema: str = "gold"


@dataclass
class DimDriver:
    """Driver dimension for analysis."""

    driver_key: int
    person_id: str
    full_name: str
    gender: Optional[str]
    date_of_birth: Optional[date]
    age: Optional[int]
    effective_date: date
    end_date: Optional[date]
    is_current: bool

    class Meta:
        table_name: str = "dim_driver"
        schema: str = "gold"


@dataclass
class FactDrivingEvent:
    """Fact table for driving events."""

    event_key: int
    event_id: str
    vehicle_key: int
    driver_key: int
    date_id: int
    time_id: int
    odometer: float
    vehicle_speed: float
    fuel_level: float
    engine_speed: float
    accelerator_pedal_position: float
    is_speeding: bool
    is_harsh_braking: bool
    is_harsh_acceleration: bool
    fuel_consumption_rate: float

    class Meta:
        table_name: str = "fact_driving_event"
        schema: str = "gold"


@dataclass
class FactMaintenanceAlert:
    """Fact table for maintenance alerts."""

    alert_key: int
    vehicle_key: int
    date_id: int
    alert_type: str
    alert_severity: str  # LOW, MEDIUM, HIGH, CRITICAL
    description: str
    action_taken: Optional[str]
    resolved: bool
    estimated_maintenance_hours: Optional[float]

    class Meta:
        table_name: str = "fact_maintenance_alert"
        schema: str = "gold"


# ========================================
# ANALYTICS MODELS
# ========================================


@dataclass
class VehicleKPI:
    """Key performance indicators for fleet vehicles."""

    auto_id: str
    total_events: int
    total_distance_km: float
    average_speed: float
    max_speed: float
    total_harsh_events: int
    avg_fuel_consumption: float
    maintenance_alerts_count: int
    last_event_date: date

    class Meta:
        table_name: str = "analytics_vehicle_kpi"


@dataclass
class DriverScore:
    """Driver safety and performance score."""

    person_id: str
    full_name: str
    safety_score: float  # 0-100
    efficiency_score: float  # 0-100
    total_trips: int
    harsh_events_count: int
    speeding_events_count: int
    overall_risk_level: str  # LOW, MEDIUM, HIGH

    class Meta:
        table_name: str = "analytics_driver_score"
