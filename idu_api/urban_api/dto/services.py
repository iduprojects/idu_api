"""Services DTOs are defined here."""

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any

import shapely.geometry as geom

Geom = geom.Polygon | geom.MultiPolygon | geom.Point | geom.LineString | geom.MultiLineString


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
    service_type_properties: dict[str, Any]
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
    service_type_properties: dict[str, Any]
    territory_type_id: int | None
    territory_type_name: str | None
    name: str | None
    capacity_real: int | None
    properties: dict[str, Any]
    object_geometry_id: int
    address: str | None
    osm_id: str | None
    geometry: Geom
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
            "properties": service.pop("service_type_properties", None),
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
    service_type_properties: dict[str, Any]
    territory_type_id: int | None
    territory_type_name: str | None
    name: str | None
    capacity_real: int | None
    properties: dict[str, Any]
    territories: list[dict[str, Any]]
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class ServicesCountCapacityDTO:
    territory_id: int
    count: int
    capacity: int


@dataclass(frozen=True)
class ShortServiceDTO:
    service_id: int
    service_type_id: int
    service_type_name: str
    territory_type_id: int | None
    territory_type_name: str | None
    name: str | None
    capacity_real: int | None
    properties: dict[str, Any]


@dataclass(frozen=True)
class ShortScenarioServiceDTO(ShortServiceDTO):
    is_scenario_object: bool


@dataclass(frozen=True)
class ScenarioServiceDTO(ServiceDTO):
    is_scenario_object: bool
