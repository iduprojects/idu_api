"""Functional zones DTOs are defined here."""

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any

from shapely import geometry as geom


@dataclass(frozen=True)
class FunctionalZoneTypeDTO:
    functional_zone_type_id: int
    name: str
    zone_nickname: str | None
    description: str | None


@dataclass
class FunctionalZoneDataDTO:
    functional_zone_id: int
    territory_id: int
    territory_name: str
    functional_zone_type_id: int
    functional_zone_type_name: str
    name: str | None
    geometry: geom.Polygon | geom.MultiPolygon
    year: int | None
    source: str | None
    properties: dict[str, Any]
    created_at: datetime
    updated_at: datetime

    def __post_init__(self) -> None:
        if isinstance(self.geometry, dict):
            self.geometry = geom.shape(self.geometry)

    def to_geojson_dict(self) -> dict:
        zone = asdict(self)
        zone["territory"] = {"id": zone.pop("territory_id"), "name": zone.pop("territory_name")}
        zone["functional_zone_type"] = {
            "id": zone.pop("functional_zone_type_id"),
            "name": zone.pop("functional_zone_type_name"),
        }
        return zone


@dataclass
class ProjectProfileDTO:
    profile_id: int
    scenario_id: int
    scenario_name: str
    functional_zone_type_id: int
    functional_zone_type_name: str
    name: str | None
    geometry: geom.Polygon | geom.MultiPolygon
    year: int | None
    source: str | None
    properties: dict[str, Any]
    created_at: datetime
    updated_at: datetime

    def __post_init__(self) -> None:
        if isinstance(self.geometry, dict):
            self.geometry = geom.shape(self.geometry)

    def to_geojson_dict(self) -> dict:
        profile = asdict(self)
        profile["functional_zone_type"] = {
            "id": profile.pop("functional_zone_type_id"),
            "name": profile.pop("functional_zone_type_name"),
        }
        return profile


@dataclass(frozen=True)
class FunctionalZoneSourceDTO:
    year: int
    source: str
