"""Territories handlers logic of getting entities from the database is defined here."""

import abc
from typing import Protocol

from shapely.geometry import LineString, MultiLineString, MultiPolygon, Point, Polygon

Geom = Point | Polygon | MultiPolygon | LineString | MultiLineString


class SystemService(Protocol):
    """Service for system tasks."""

    @abc.abstractmethod
    async def fix_geometry(self, geom: Geom) -> Geom:
        """Returns fixed shapely geometry."""

    @abc.abstractmethod
    async def fix_geojson(self, geoms: list[Geom]) -> list[Geom]:
        """Returns list of fixed shapely geometry."""
