"""Service types DTOs are defined here."""

from dataclasses import dataclass
from typing import Any, Literal, Self


@dataclass(frozen=True)
class ServiceTypeDTO:
    service_type_id: int
    urban_function_id: int | None
    urban_function_name: str | None
    name: str
    capacity_modeled: int | None
    code: str
    infrastructure_type: Literal["basic", "additional", "comfort"] | None
    properties: dict[str, Any]


@dataclass(frozen=True)
class UrbanFunctionDTO:
    urban_function_id: int
    parent_urban_function_id: int | None
    parent_urban_function_name: str | None
    name: str
    level: int
    list_label: str
    code: str


@dataclass(frozen=True)
class ServiceTypesHierarchyDTO:
    urban_function_id: int
    parent_urban_function_id: int | None
    name: str
    level: int
    list_label: str
    code: str
    children: list[Self | ServiceTypeDTO]
