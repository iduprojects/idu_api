"""Projects DTOs are defined here."""

# pylint: disable=too-many-instance-attributes

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any

import shapely.geometry as geom


@dataclass(frozen=True)
class ProjectDTO:
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
    scenario_id: int
    scenario_name: str
    geometry: geom.Polygon | geom.MultiPolygon
    centre_point: geom.Point
    properties: dict[str, Any] | None

    def __post_init__(self) -> None:
        if isinstance(self.centre_point, dict):
            self.centre_point = geom.shape(self.centre_point)
        if isinstance(self.geometry, dict):
            self.geometry = geom.shape(self.geometry)


@dataclass
class ProjectWithTerritoryDTO:
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
    geometry: geom.Polygon | geom.MultiPolygon
    centre_point: geom.Point

    def __post_init__(self) -> None:
        if isinstance(self.centre_point, dict):
            self.centre_point = geom.shape(self.centre_point)
        if isinstance(self.geometry, dict):
            self.geometry = geom.shape(self.geometry)

    def to_geojson_dict(self):
        project = asdict(self)
        project["territory"] = {"id": project.pop("territory_id"), "name": project.pop("territory_name")}
        project["base_scenario"] = {"id": project.pop("scenario_id"), "name": project.pop("scenario_name")}

        return project
