"""Functional zones DTO are defined here."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from shapely import geometry as geom


@dataclass(frozen=True)
class FunctionalZoneTypeDTO:
    functional_zone_type_id: int
    name: str
    zone_nickname: str | None
    description: str | None


@dataclass
class FunctionalZoneDataDTO:
    functional_zone_id: int
    territory_id: int
    territory_name: str
    functional_zone_type_id: int
    functional_zone_type_name: str
    geometry: geom.Polygon | geom.MultiPolygon
    properties: dict[str, Any]
    created_at: datetime
    updated_at: datetime

    def __post_init__(self) -> None:
        if isinstance(self.geometry, dict):
            self.geometry = geom.shape(self.geometry)


@dataclass
class ProjectsProfileDTO:
    profile_id: int
    scenario_id: int
    scenario_name: str
    functional_zone_type_id: int
    functional_zone_type_name: str
    geometry: geom.Polygon | geom.MultiPolygon
    properties: dict[str, Any]
    created_at: datetime
    updated_at: datetime

    def __post_init__(self) -> None:
        if isinstance(self.geometry, dict):
            self.geometry = geom.shape(self.geometry)
