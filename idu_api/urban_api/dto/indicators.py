"""
Indicators DTO are defined here.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Optional


@dataclass
class IndicatorDTO:
    indicator_id: int
    name_full: str
    name_short: str
    measurement_unit_id: Optional[int]
    measurement_unit_name: Optional[str]
    level: int
    list_label: str
    parent_id: int


@dataclass
class IndicatorValueDTO:
    indicator_id: int
    territory_id: int
    date_type: Literal["year", "half_year", "quarter", "month", "day"]
    date_value: datetime
    value: int
    value_type: Literal["real", "forecast", "target"]
    information_source: str


@dataclass
class MeasurementUnitDTO:
    measurement_unit_id: int
    name: str
