"""Indicators DTOs are defined here."""

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal


@dataclass(frozen=True)
class IndicatorDTO:
    indicator_id: int
    name_full: str
    name_short: str
    measurement_unit_id: int | None
    measurement_unit_name: str | None
    service_type_id: int | None
    service_type_name: str | None
    physical_object_type_id: int | None
    physical_object_type_name: str | None
    level: int
    list_label: str
    parent_id: int
    created_at: datetime
    updated_at: datetime

    @classmethod
    def fields(cls) -> Iterable[str]:
        return cls.__annotations__.keys()


@dataclass(frozen=True)
class IndicatorValueDTO:  # pylint: disable=too-many-instance-attributes
    indicator_value_id: int
    indicator_id: int
    parent_id: int
    name_full: str
    measurement_unit_id: int | None
    measurement_unit_name: str | None
    level: int
    list_label: str
    territory_id: int
    territory_name: str
    date_type: Literal["year", "half_year", "quarter", "month", "day"]
    date_value: datetime
    value: float
    value_type: Literal["real", "forecast", "target"]
    information_source: str
    created_at: datetime
    updated_at: datetime

    @classmethod
    def fields(cls) -> Iterable[str]:
        return cls.__annotations__.keys()


@dataclass(frozen=True)
class MeasurementUnitDTO:
    measurement_unit_id: int
    name: str


@dataclass(frozen=True)
class IndicatorsGroupDTO:
    indicators_group_id: int
    name: str
    indicators: list[IndicatorDTO]


@dataclass(frozen=True)
class ShortScenarioIndicatorValueDTO:
    indicator_id: int
    name_full: str
    measurement_unit_name: str | None
    value: float
    comment: str | None


@dataclass(frozen=True)
class ScenarioIndicatorValueDTO:  # pylint: disable=too-many-instance-attributes
    indicator_value_id: int
    indicator_id: int
    parent_id: int
    name_full: str
    measurement_unit_id: int | None
    measurement_unit_name: str | None
    level: int
    list_label: str
    scenario_id: int
    scenario_name: str
    territory_id: int | None
    territory_name: str | None
    hexagon_id: int | None
    value: float
    comment: str | None
    information_source: str | None
    properties: dict[str, Any]
    created_at: datetime
    updated_at: datetime
