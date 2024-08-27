"""Service types DTO are defined here."""

from dataclasses import dataclass


@dataclass(frozen=True)
class ServiceTypesDTO:
    service_type_id: int
    urban_function_id: int | None
    name: str
    capacity_modeled: int | None
    code: str


@dataclass(frozen=True)
class UrbanFunctionDTO:
    urban_function_id: int
    parent_urban_function_id: int | None
    name: str
    level: int
    list_label: str
    code: str
