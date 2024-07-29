from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional

import shapely.geometry as geom


@dataclass()
class UrbanObjectDTO:
    urban_object_id: int
    physical_object_id: int
    physical_object_type_id: int
    physical_object_type_name: str
    physical_object_name: Optional[str]
    physical_object_properties: Dict[str, Any]
    physical_object_created_at: datetime
    physical_object_updated_at: datetime
    object_geometry_id: int
    territory_id: int
    address: Optional[str]
    geometry: geom.Polygon | geom.MultiPolygon | geom.Point
    centre_point: geom.Point
    service_id: Optional[int]
    service_type_id: Optional[int]
    urban_function_id: Optional[int]
    service_type_name: Optional[str]
    service_type_capacity_modeled: Optional[int]
    service_type_code: Optional[str]
    territory_type_id: Optional[int]
    territory_type_name: Optional[str]
    service_name: Optional[str]
    capacity_real: Optional[int]
    service_properties: Optional[Dict[str, Any]]
    service_created_at: Optional[datetime]
    service_updated_at: Optional[datetime]

    def __post_init__(self) -> None:
        if isinstance(self.centre_point, dict):
            self.centre_point = geom.shape(self.centre_point)
        if self.geometry is None:
            self.geometry = self.centre_point
        if isinstance(self.geometry, dict):
            self.geometry = geom.shape(self.geometry)
