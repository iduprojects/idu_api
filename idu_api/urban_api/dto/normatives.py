"""Normatives DTOs are defined here."""

from dataclasses import dataclass
from datetime import datetime
from typing import Literal


@dataclass(frozen=True)
class NormativeDTO:  # pylint: disable=too-many-instance-attributes
    service_type_id: int | None
    service_type_name: str | None
    urban_function_id: int | None
    urban_function_name: str | None
    year: int
    is_regulated: bool
    radius_availability_meters: int | None
    time_availability_minutes: int | None
    services_per_1000_normative: float | None
    services_capacity_per_1000_normative: float | None
    normative_type: Literal["self", "parent", "global"]
    source: str | None
    created_at: datetime
    updated_at: datetime
