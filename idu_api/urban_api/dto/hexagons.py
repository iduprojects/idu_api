"""Hexagons DTOs are defined here."""

from dataclasses import asdict, dataclass
from typing import Any

import shapely.geometry as geom

from idu_api.urban_api.dto.indicators import ProjectIndicatorValueDTO


@dataclass
class HexagonDTO:
    hexagon_id: int
    territory_id: int
    territory_name: str
    geometry: geom.Polygon | geom.MultiPolygon
    centre_point: geom.Point
    properties: dict[str, Any] | None

    def __post_init__(self) -> None:
        if isinstance(self.centre_point, dict):
            self.centre_point = geom.shape(self.centre_point)
        if isinstance(self.geometry, dict):
            self.geometry = geom.shape(self.geometry)

    def to_geojson_dict(self) -> dict:
        hexagon = asdict(self)
        del hexagon["territory_id"]
        del hexagon["territory_name"]
        return hexagon


@dataclass
class HexagonWithIndicatorsDTO:
    hexagon_id: int
    geometry: geom.Polygon | geom.MultiPolygon
    centre_point: geom.Point
    indicators: [ProjectIndicatorValueDTO]

    def __post_init__(self) -> None:
        if isinstance(self.centre_point, dict):
            self.centre_point = geom.shape(self.centre_point)
        if isinstance(self.geometry, dict):
            self.geometry = geom.shape(self.geometry)

    def to_geojson_dict(self) -> dict:
        return asdict(self)
