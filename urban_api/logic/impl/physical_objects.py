"""Physical objects handlers logic is defined here."""

from shapely.geometry import LineString, MultiPolygon, Point, Polygon
from sqlalchemy.ext.asyncio import AsyncConnection

from urban_api.dto.physical_objects import PhysicalObjectDataDTO
from urban_api.logic.impl.helpers.physical_objects import get_physical_objects_around, get_physical_objects_by_ids
from urban_api.logic.physical_objects import PhysicalObjectsService

Geom = Point | Polygon | MultiPolygon | LineString


class PhysicalObjectsServiceImpl(PhysicalObjectsService):
    """Service to manipulate physical objects entities.

    Based on async SQLAlchemy connection.
    """

    def __init__(self, conn: AsyncConnection):
        self._conn = conn

    async def get_physical_objects_by_ids(self, ids: list[int]) -> list[PhysicalObjectDataDTO]:
        return await get_physical_objects_by_ids(self._conn, ids)

    async def get_physical_objects_around(
        self, geometry: Geom, physical_object_type_id: int, buffer_meters: int
    ) -> list[PhysicalObjectDataDTO]:
        return await get_physical_objects_around(self._conn, geometry, physical_object_type_id, buffer_meters)
