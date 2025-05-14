"""Social groups and values DTOs are defined here."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal


@dataclass(frozen=True)
class SocGroupDTO:
    soc_group_id: int
    name: str


@dataclass(frozen=True)
class SocGroupWithServiceTypesDTO:
    soc_group_id: int
    name: str
    service_types: list[dict[str, Any]]


@dataclass(frozen=True)
class SocValueDTO:
    soc_value_id: int
    name: str
    rank: int
    normative_value: float
    decree_value: float


@dataclass(frozen=True)
class SocValueIndicatorValueDTO:
    soc_value_id: int
    soc_value_name: str
    territory_id: int
    territory_name: str
    year: int
    value: float
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class SocValueWithServiceTypesDTO:
    soc_value_id: int
    service_type_id: int
    service_types: list[dict[str, Any]]

