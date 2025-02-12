"""Territories handlers logic of getting entities from the database is defined here."""

import abc
from typing import Protocol

from shapely.geometry import LineString, MultiLineString, MultiPolygon, Point, Polygon

from idu_api.urban_api.schemas.geometries import GeoJSONResponse

Geom = Point | Polygon | MultiPolygon | LineString | MultiLineString


class SystemService(Protocol):
    """Service for system tasks."""

    @abc.abstractmethod
    async def fix_geometry(self, geom: Geom) -> Geom:
        """Returns fixed geometry response."""

    @abc.abstractmethod
    async def fix_geojson(self, geojson: GeoJSONResponse) -> GeoJSONResponse:
        """Returns fixed geojson response."""
