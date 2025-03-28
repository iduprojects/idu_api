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


@dataclass(frozen=True)
class SocValueWithSocGroupsDTO:
    soc_value_id: int
    name: str
    soc_groups: list[SocGroupWithServiceTypesDTO]


@dataclass(frozen=True)
class SocGroupIndicatorValueDTO:
    soc_group_id: int
    soc_group_name: str
    soc_value_id: int
    soc_value_name: str
    territory_id: int
    territory_name: str
    year: int
    value: float
    value_type: Literal["real", "forecast", "target"]
    created_at: datetime
    updated_at: datetime
