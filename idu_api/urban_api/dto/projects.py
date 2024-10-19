from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

import shapely.geometry as geom


@dataclass
class ProjectDTO:
    project_id: int
    user_id: str
    name: str
    project_territory_id: int
    description: str | None
    public: bool
    image_url: str | None
    created_at: datetime
    updated_at: datetime
    properties: dict[str, Any]


@dataclass
class ProjectTerritoryDTO:
    project_territory_id: int
    parent_territory_id: int | None
    geometry: geom.Polygon | geom.MultiPolygon | geom.Point
    centre_point: geom.Point
    properties: dict[str, Any] | None

    def __post_init__(self) -> None:
        if isinstance(self.centre_point, dict):
            self.centre_point = geom.shape(self.centre_point)
        if self.geometry is None:
            self.geometry = self.centre_point
        if isinstance(self.geometry, dict):
            self.geometry = geom.shape(self.geometry)


@dataclass
class ProjectsIndicatorDTO:
    scenario_id: int
    indicator_id: int
    date_type: Literal["year", "half_year", "quarter", "month", "day"]
    date_value: datetime
    value: float
    value_type: Literal["real", "forecast", "target"]
    information_source: str
