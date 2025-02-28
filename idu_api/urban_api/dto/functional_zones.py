"""Functional zones DTOs are defined here."""

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any

import shapely.geometry as geom
from shapely.wkb import loads as wkb_loads


@dataclass(frozen=True)
class FunctionalZoneTypeDTO:
    functional_zone_type_id: int
    name: str
    zone_nickname: str | None
    description: str | None


@dataclass
class FunctionalZoneDTO:  # pylint: disable=too-many-instance-attributes
    functional_zone_id: int
    territory_id: int
    territory_name: str
    functional_zone_type_id: int
    functional_zone_type_name: str
    functional_zone_type_nickname: str
    name: str | None
    geometry: geom.Polygon | geom.MultiPolygon
    year: int
    source: str
    properties: dict[str, Any]
    created_at: datetime
    updated_at: datetime

    def __post_init__(self) -> None:
        if isinstance(self.geometry, bytes):
            self.geometry = wkb_loads(self.geometry)

    def to_geojson_dict(self) -> dict:
        zone = asdict(self)
        zone["territory"] = {"id": zone.pop("territory_id"), "name": zone.pop("territory_name")}
        zone["functional_zone_type"] = {
            "id": zone.pop("functional_zone_type_id"),
            "name": zone.pop("functional_zone_type_name"),
            "nickname": zone.pop("functional_zone_type_nickname"),
        }
        return zone


@dataclass
class ScenarioFunctionalZoneDTO:  # pylint: disable=too-many-instance-attributes
    functional_zone_id: int
    scenario_id: int
    scenario_name: str
    functional_zone_type_id: int
    functional_zone_type_name: str
    functional_zone_type_nickname: str
    name: str | None
    geometry: geom.Polygon | geom.MultiPolygon
    year: int
    source: str
    properties: dict[str, Any]
    created_at: datetime
    updated_at: datetime

    def __post_init__(self) -> None:
        if isinstance(self.geometry, bytes):
            self.geometry = wkb_loads(self.geometry)

    def to_geojson_dict(self) -> dict:
        profile = asdict(self)
        profile["functional_zone_type"] = {
            "id": profile.pop("functional_zone_type_id"),
            "name": profile.pop("functional_zone_type_name"),
            "nickname": profile.pop("functional_zone_type_nickname"),
        }
        return profile


@dataclass(frozen=True)
class FunctionalZoneSourceDTO:
    year: int
    source: str
