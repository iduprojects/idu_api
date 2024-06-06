"""
Service types normatives DTO are defined here.
"""

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class ServiceTypesDTO:
    service_type_id: int
    urban_function_id: Optional[int]
    name: str
    capacity_modeled: Optional[int]
    code: str


@dataclass(frozen=True)
class ServiceTypesNormativesDTO:
    normative_id: int
    service_type_id: Optional[int]
    urban_function_id: Optional[int]
    territory_id: int
    is_regulated: bool
    radius_availability_meters: Optional[int]
    time_availability_minutes: Optional[int]
    services_per_1000_normative: Optional[float]
    services_capacity_per_1000_normative: Optional[float]


@dataclass(frozen=True)
class UrbanFunctionDTO:
    urban_function_id: int
    parent_urban_function_id: Optional[int]
    name: str
    level: int
    list_label: str
    code: str
