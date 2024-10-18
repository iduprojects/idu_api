from dataclasses import dataclass
from typing import Any, Dict, Optional

import shapely.geometry as geom

from idu_api.city_api.dto.base import Base


@dataclass()
class PhysicalObjectsDTO(Base):
    physical_object_id: int
    name: Optional[str]
    properties: Dict[str, Any]
    geometry: geom.Polygon | geom.MultiPolygon | geom.Point
    centre_point: geom.Point

    def __post_init__(self) -> None:
        if isinstance(self.centre_point, dict):
            self.centre_point = geom.shape(self.centre_point)
        if self.geometry is None:
            self.geometry = self.centre_point
        if isinstance(self.geometry, dict):
            self.geometry = geom.shape(self.geometry)
