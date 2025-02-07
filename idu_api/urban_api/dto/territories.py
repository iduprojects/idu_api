"""Territories DTOs are defined here."""

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any

import shapely.geometry as geom

from idu_api.urban_api.dto.indicators import IndicatorValueDTO
from idu_api.urban_api.dto.normatives import NormativeDTO


@dataclass(frozen=True)
class TerritoryTypeDTO:
    """Territory type DTO used to transfer territory type data."""

    territory_type_id: int | None
    name: str


@dataclass(frozen=True)
class TargetCityTypeDTO:
    """Target city type DTO used to transfer target city type data."""

    target_city_type_id: int | None
    name: str
    description: str


@dataclass
class TerritoryDTO:  # pylint: disable=too-many-instance-attributes
    """Territory DTO used to transfer territory data."""

    territory_id: int
    territory_type_id: int
    territory_type_name: str
    parent_id: int
    parent_name: str
    name: str
    geometry: geom.Polygon | geom.MultiPolygon | geom.Point
    level: int
    properties: dict[str, Any] | None
    centre_point: geom.Point
    admin_center_id: int | None
    admin_center_name: str | None
    target_city_type_id: int | None
    target_city_type_name: str | None
    target_city_type_description: str | None
    okato_code: str | None
    oktmo_code: str | None
    is_city: bool
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
        territory = asdict(self)
        territory["territory_type"] = {
            "id": territory.pop("territory_type_id"),
            "name": territory.pop("territory_type_name"),
        }
        territory["parent"] = {
            "id": territory.pop("parent_id"),
            "name": territory.pop("parent_name"),
        }
        territory["admin_center"] = {
            "id": territory.pop("admin_center_id"),
            "name": territory.pop("admin_center_name"),
        }
        territory["target_city_type"] = {
            "id": territory.pop("target_city_type_id"),
            "name": territory.pop("target_city_type_name"),
            "description": territory.pop("target_city_type_description"),
        }
        return territory


@dataclass(frozen=True)
class TerritoryWithoutGeometryDTO:  # pylint: disable=too-many-instance-attributes
    """Territory DTO used to transfer territory data without geometry."""

    territory_id: int
    territory_type_id: int
    territory_type_name: str
    parent_id: int
    parent_name: str
    name: str
    level: int
    properties: dict[str, Any]
    admin_center_id: int | None
    admin_center_name: str | None
    target_city_type_id: int | None
    target_city_type_name: str | None
    target_city_type_description: str | None
    okato_code: str | None
    oktmo_code: str | None
    is_city: bool
    created_at: datetime
    updated_at: datetime


@dataclass
class TerritoryWithIndicatorDTO:
    """Territory DTO used to transfer short territory data with indicator."""

    territory_id: int
    name: str
    geometry: geom.Polygon | geom.MultiPolygon | geom.Point
    centre_point: geom.Point
    indicator_name: str
    indicator_value: float
    measurement_unit_name: str | None

    def __post_init__(self) -> None:
        if isinstance(self.centre_point, dict):
            self.centre_point = geom.shape(self.centre_point)
        if self.geometry is None:
            self.geometry = self.centre_point
        if isinstance(self.geometry, dict):
            self.geometry = geom.shape(self.geometry)

    def to_geojson_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class TerritoryWithIndicatorsDTO:
    """Territory DTO used to transfer short territory data with list of indicators."""

    territory_id: int
    name: str
    geometry: geom.Polygon | geom.MultiPolygon | geom.Point
    centre_point: geom.Point
    indicators: list[IndicatorValueDTO]

    def __post_init__(self) -> None:
        if isinstance(self.centre_point, dict):
            self.centre_point = geom.shape(self.centre_point)
        if self.geometry is None:
            self.geometry = self.centre_point
        if isinstance(self.geometry, dict):
            self.geometry = geom.shape(self.geometry)

    def to_geojson_dict(self) -> dict[str, Any]:
        territory = asdict(self)
        for indicator in territory["indicators"]:
            del indicator["indicator_id"]
            del indicator["measurement_unit_id"]
            del indicator["date_type"]
            del indicator["territory_id"]
            del indicator["territory_name"]
            del indicator["created_at"]
            del indicator["updated_at"]
        return territory


@dataclass
class TerritoryWithNormativesDTO:
    """Territory DTO used to transfer short territory data with list of indicators."""

    territory_id: int
    name: str
    geometry: geom.Polygon | geom.MultiPolygon | geom.Point
    centre_point: geom.Point
    normatives: list[NormativeDTO]

    def __post_init__(self) -> None:
        if isinstance(self.centre_point, dict):
            self.centre_point = geom.shape(self.centre_point)
        if self.geometry is None:
            self.geometry = self.centre_point
        if isinstance(self.geometry, dict):
            self.geometry = geom.shape(self.geometry)

    def to_geojson_dict(self) -> dict[str, Any]:
        territory = asdict(self)
        for normative in territory["normatives"]:
            del normative["territory_id"]
            del normative["territory_name"]
            del normative["service_type_id"]
            del normative["urban_function_id"]
            del normative["created_at"]
            del normative["updated_at"]
            if normative["service_type_name"] is not None:
                normative["name"] = normative.pop("service_type_name")
                del normative["urban_function_name"]
            else:
                normative["name"] = normative.pop("urban_function_name")
                del normative["service_type_name"]

        return territory
