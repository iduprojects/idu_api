from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

import shapely.geometry as geom


@dataclass
class ProjectDTO:
    project_id: int
    user_id: int
    name: str
    project_territory_id: int
    description: str | None
    public: bool
    image_url: str | None
    created_at: datetime
    updated_at: datetime


@dataclass
class ProjectTerritoryDTO:
    project_territory_id: int
    parent_territory_id: int
    geometry: geom.Polygon | geom.MultiPolygon | geom.Point
    centre_point: geom.Point
    properties: Optional[dict[str, Any]]

    def __post_init__(self) -> None:
        if isinstance(self.centre_point, dict):
            self.centre_point = geom.shape(self.centre_point)
        if self.geometry is None:
            self.geometry = self.centre_point
        if isinstance(self.geometry, dict):
            self.geometry = geom.shape(self.geometry)
