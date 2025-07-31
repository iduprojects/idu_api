"""Buffers DTOs are defined here."""

from dataclasses import asdict, dataclass
from typing import Any

import shapely.geometry as geom
from shapely.wkb import loads as wkb_loads


@dataclass(frozen=True)
class BufferTypeDTO:
    buffer_type_id: int
    name: str


@dataclass(frozen=True)
class DefaultBufferValueDTO:
    buffer_type_id: int
    buffer_type_name: str
    physical_object_type_id: int | None
    physical_object_type_name: str | None
    service_type_id: int | None
    service_type_name: str | None
    buffer_value: float


@dataclass
class BufferDTO:
    buffer_type_id: int
    buffer_type_name: str
    urban_object_id: int
    physical_object_id: int
    physical_object_name: str
    physical_object_type_id: int
    physical_object_type_name: str
    object_geometry_id: int
    territory_id: int
    territory_name: str
    service_id: int | None
    service_name: str | None
    service_type_id: int | None
    service_type_name: str | None
    geometry: geom.Polygon | geom.MultiPolygon
    is_custom: bool

    def __post_init__(self) -> None:
        if isinstance(self.geometry, bytes):
            self.geometry = wkb_loads(self.geometry)

    def to_geojson_dict(self) -> dict[str, Any]:
        buffer = asdict(self)

        buffer["buffer_type"] = {
            "id": buffer.pop("buffer_type_id"),
            "name": buffer.pop("buffer_type_name"),
        }
        buffer["urban_object"] = {
            "id": buffer.pop("urban_object_id"),
            "physical_object": {
                "id": buffer.pop("physical_object_id"),
                "name": buffer.pop("physical_object_name"),
                "type": {
                    "id": buffer.pop("physical_object_type_id"),
                    "name": buffer.pop("physical_object_type_name"),
                },
            },
            "object_geometry": {
                "id": buffer.pop("object_geometry_id"),
                "territory": {
                    "id": buffer.pop("territory_id"),
                    "name": buffer.pop("territory_name"),
                },
            },
        }
        service = {
            "id": buffer.pop("service_id"),
            "name": buffer.pop("service_name"),
            "type": {
                "id": buffer.pop("service_type_id"),
                "name": buffer.pop("service_type_name"),
            },
        }
        buffer["urban_object"]["service"] = service if service["id"] is not None else None

        return buffer


@dataclass
class ScenarioBufferDTO(BufferDTO):
    is_scenario_object: bool
    is_locked: bool
