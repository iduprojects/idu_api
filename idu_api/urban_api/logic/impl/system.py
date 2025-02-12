"""System handler inner logic is defined here."""

import structlog
from shapely.geometry import LineString, MultiLineString, MultiPolygon, Point, Polygon

from idu_api.common.db.connection.manager import PostgresConnectionManager
from idu_api.urban_api.logic.impl.helpers.system import fix_geojson_by_postgis, fix_geometry_by_postgis
from idu_api.urban_api.logic.system import SystemService

Geom = Point | Polygon | MultiPolygon | LineString | MultiLineString


class SystemServiceImpl(SystemService):
    """Service for system tasks.

    Based on async `PostgresConnectionManager`.
    """

    def __init__(self, connection_manager: PostgresConnectionManager, logger: structlog.stdlib.BoundLogger):
        self._connection_manager = connection_manager
        self._logger = logger

    async def fix_geometry(self, geom: Geom) -> Geom:
        async with self._connection_manager.get_ro_connection() as conn:
            return await fix_geometry_by_postgis(conn, geom, self._logger)

    async def fix_geojson(self, geoms: list[Geom]) -> list[Geom]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await fix_geojson_by_postgis(conn, geoms, self._logger)
