"""Normatives DTO are defined here."""

from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Optional


@dataclass(frozen=True)
class NormativeDTO:  # pylint: disable=too-many-instance-attributes
    service_type_id: Optional[int]
    service_type_name: Optional[str]
    urban_function_id: Optional[int]
    urban_function_name: Optional[str]
    year: int
    is_regulated: bool
    radius_availability_meters: Optional[int]
    time_availability_minutes: Optional[int]
    services_per_1000_normative: Optional[float]
    services_capacity_per_1000_normative: Optional[float]
    normative_type: Literal["self", "parent", "global"]
    source: Optional[str]
    created_at: datetime
    updated_at: datetime
