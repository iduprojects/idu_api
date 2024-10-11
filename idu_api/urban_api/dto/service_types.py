"""Service types DTO are defined here."""

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class ServiceTypesDTO:
    service_type_id: int
    urban_function_id: int | None
    urban_function_name: str | None
    name: str
    capacity_modeled: int | None
    code: str
    infrastructure_type: Literal["basic", "additional", "comfort"]


@dataclass(frozen=True)
class UrbanFunctionDTO:
    urban_function_id: int
    parent_urban_function_id: int | None
    parent_urban_function_name: str | None
    name: str
    level: int
    list_label: str
    code: str
