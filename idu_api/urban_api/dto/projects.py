"""Projects DTOs are defined here."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any

import shapely.geometry as geom


@dataclass(frozen=True)
class ProjectDTO:
    project_id: int
    user_id: str
    territory_id: int
    territory_name: str
    name: str
    description: str | None
    is_regional: bool
    public: bool
    properties: dict[str, Any]
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class ProjectWithBaseScenarioDTO:
    project_id: int
    user_id: str
    territory_id: int
    territory_name: str
    scenario_id: int
    scenario_name: str
    name: str
    description: str | None
    is_regional: bool
    public: bool
    properties: dict[str, Any]
    created_at: datetime
    updated_at: datetime


@dataclass
class ProjectTerritoryDTO:
    project_territory_id: int
    project_id: int
    project_name: str
    project_user_id: str
    territory_id: int
    territory_name: str
    geometry: geom.Polygon | geom.MultiPolygon
    centre_point: geom.Point
    properties: dict[str, Any] | None

    def __post_init__(self) -> None:
        if isinstance(self.centre_point, dict):
            self.centre_point = geom.shape(self.centre_point)
        if isinstance(self.geometry, dict):
            self.geometry = geom.shape(self.geometry)
