"""Services DTO are defined here."""

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any

import shapely.geometry as geom


@dataclass(frozen=True)
class ServiceDTO:  # pylint: disable=too-many-instance-attributes
    service_id: int
    service_type_id: int
    urban_function_id: int
    urban_function_name: str
    service_type_name: str
    service_type_capacity_modeled: int
    service_type_code: str
    infrastructure_type: str
    territory_type_id: int | None
    territory_type_name: str | None
    name: str | None
    capacity_real: int | None
    properties: dict[str, Any]
    created_at: datetime
    updated_at: datetime


@dataclass
class ServiceWithGeometryDTO:  # pylint: disable=too-many-instance-attributes
    service_id: int
    service_type_id: int
    urban_function_id: int
    urban_function_name: str
    service_type_name: str
    service_type_capacity_modeled: int
    service_type_code: str
    infrastructure_type: str
    territory_type_id: int | None
    territory_type_name: str | None
    name: str | None
    capacity_real: int | None
    properties: dict[str, Any]
    address: str | None
    osm_id: str | None
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

    def to_geojson_dict(self) -> dict[str, Any]:
        service = asdict(self)
        territory_type = service.pop("territory_type_id", None), service.pop("territory_type_name", None)
        service["territory_type"] = (
            {"territory_type_id": territory_type[0], "name": territory_type[1]}
            if territory_type[0] is not None
            else None
        )
        service_type = {
            "service_type_id": service.pop("service_type_id", None),
            "urban_function": {
                "id": service.pop("urban_function_id", None),
                "name": service.pop("urban_function_name", None),
            },
            "name": service.pop("service_type_name", None),
            "capacity_modeled": service.pop("service_type_capacity_modeled", None),
            "code": service.pop("service_type_code", None),
            "infrastructure_type": service.pop("infrastructure_type", None),
        }
        service["service_type"] = service_type

        return service


@dataclass(frozen=True)
class ServiceWithTerritoriesDTO:  # pylint: disable=too-many-instance-attributes
    service_id: int
    service_type_id: int
    urban_function_id: int
    urban_function_name: str
    service_type_name: str
    service_type_capacity_modeled: int
    service_type_code: str
    infrastructure_type: str
    territory_type_id: int | None
    territory_type_name: str | None
    name: str | None
    capacity_real: int | None
    properties: dict[str, Any]
    territories: list[dict[str, Any]]
    created_at: datetime
    updated_at: datetime
