"""Physical objects DTOs are defined here."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

import shapely.geometry as geom


@dataclass(frozen=True)
class PhysicalObjectTypeDTO:
    """Physical object type with all its attributes."""

    physical_object_type_id: int
    name: str


@dataclass(frozen=True)
class PhysicalObjectDataDTO:
    physical_object_id: int
    physical_object_type_id: int
    physical_object_type_name: str
    name: Optional[str]
    properties: Dict[str, Any]
    created_at: datetime
    updated_at: datetime


@dataclass
class PhysicalObjectWithGeometryDTO:
    physical_object_id: int
    physical_object_type_id: int
    physical_object_type_name: str
    name: Optional[str]
    address: Optional[str]
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
class PhysicalObjectWithTerritoryDTO:
    physical_object_id: int
    physical_object_type_id: int
    physical_object_type_name: str
    name: Optional[str]
    properties: Dict[str, Any]
    territories: List[Dict[str, Any]]
    created_at: datetime
    updated_at: datetime
