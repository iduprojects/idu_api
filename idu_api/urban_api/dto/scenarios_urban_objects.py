from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

import shapely.geometry as geom


@dataclass
class ScenarioUrbanObjectDTO:  # pylint: disable=too-many-instance-attributes
    urban_object_id: int
    scenario_id: int
    physical_object_id: int
    physical_object_type_id: int
    physical_object_type_name: str
    physical_object_name: str | None
    physical_object_properties: dict[str, Any]
    physical_object_created_at: datetime
    physical_object_updated_at: datetime
    object_geometry_id: int
    territory_id: int
    address: str | None
    osm_id: str | None
    geometry: geom.Polygon | geom.MultiPolygon | geom.Point
    centre_point: geom.Point
    object_geometry_created_at: datetime
    object_geometry_updated_at: datetime
    service_id: int | None
    service_type_id: int | None
    urban_function_id: int | None
    urban_function_name: str | None
    service_type_name: str | None
    service_type_capacity_modeled: int | None
    service_type_code: str | None
    infrastructure_type: str | None
    territory_type_id: int | None
    territory_type_name: str | None
    service_name: str | None
    capacity_real: int | None
    service_properties: Optional[dict[str, Any]]
    service_created_at: datetime | None
    service_updated_at: datetime | None

    def __post_init__(self) -> None:
        if isinstance(self.centre_point, dict):
            self.centre_point = geom.shape(self.centre_point)
        if self.geometry is None:
            self.geometry = self.centre_point
        if isinstance(self.geometry, dict):
            self.geometry = geom.shape(self.geometry)
