"""Hexagons DTOs are defined here."""

from dataclasses import dataclass
from typing import Any

import shapely.geometry as geom


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
