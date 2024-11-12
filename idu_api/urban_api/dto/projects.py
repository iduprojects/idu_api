"""Projects DTOs are defined here."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any

import shapely.geometry as geom


@dataclass
class ProjectDTO:
    project_id: int
    user_id: str
    territory_id: int
    territory_name: str
    name: str
    description: str | None
    is_regional: bool
    public: bool
    created_at: datetime
    updated_at: datetime
    properties: dict[str, Any]


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
