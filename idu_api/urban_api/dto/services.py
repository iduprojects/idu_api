"""
Services DTO are defined here.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

import shapely.geometry as geom


@dataclass(frozen=True)
class ServiceDTO:  # pylint: disable=too-many-instance-attributes
    service_id: int
    service_type_id: int
    urban_function_id: int
    service_type_name: str
    service_type_capacity_modeled: int
    service_type_code: str
    territory_type_id: Optional[int]
    territory_type_name: Optional[str]
    name: Optional[str]
    capacity_real: Optional[int]
    properties: Dict[str, Any]
    created_at: datetime
    updated_at: datetime


@dataclass
class ServiceWithGeometryDTO:  # pylint: disable=too-many-instance-attributes
    service_id: int
    service_type_id: int
    urban_function_id: int
    service_type_name: str
    service_type_capacity_modeled: int
    service_type_code: str
    territory_type_id: Optional[int]
    territory_type_name: Optional[str]
    name: Optional[str]
    capacity_real: Optional[int]
    properties: Dict[str, Any]
    geometry: geom.Polygon | geom.MultiPolygon | geom.Point
    centre_point: geom.Point
    created_at: datetime
    updated_at: datetime

    def __post_init__(self) -> None:
        if isinstance(self.centre_point, dict):
            self.centre_point = geom.shape(self.centre_point)
        if self.geometry is None:
            self.geometry = self.centre_point
        if isinstance(self.geometry, dict):
            self.geometry = geom.shape(self.geometry)


@dataclass(frozen=True)
class ServiceWithTerritoriesDTO:  # pylint: disable=too-many-instance-attributes
    service_id: int
    service_type_id: int
    urban_function_id: int
    service_type_name: str
    service_type_capacity_modeled: int
    service_type_code: str
    territory_type_id: Optional[int]
    territory_type_name: Optional[str]
    name: Optional[str]
    capacity_real: Optional[int]
    properties: Dict[str, Any]
    territories: List[Dict[str, Any]]
    created_at: datetime
    updated_at: datetime
