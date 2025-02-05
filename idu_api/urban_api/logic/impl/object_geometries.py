"""Object geometries handlers logic of getting entities from the database is defined here."""

from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.urban_api.dto import ObjectGeometryDTO, PhysicalObjectDTO, UrbanObjectDTO
from idu_api.urban_api.logic.impl.helpers.object_geometries import (
    add_object_geometry_to_physical_object_to_db,
    delete_object_geometry_in_db,
    get_object_geometry_by_ids_from_db,
    get_physical_objects_by_object_geometry_id_from_db,
    patch_object_geometry_to_db,
    put_object_geometry_to_db,
)
from idu_api.urban_api.logic.object_geometries import ObjectGeometriesService
from idu_api.urban_api.schemas import ObjectGeometryPatch, ObjectGeometryPost, ObjectGeometryPut


class ObjectGeometriesServiceImpl(ObjectGeometriesService):
    """Service to manipulate object geometries.

    Based on async SQLAlchemy connection.
    """

    def __init__(self, conn: AsyncConnection):
        self._conn = conn

    async def get_object_geometry_by_ids(self, object_geometry_ids: list[int]) -> list[ObjectGeometryDTO]:
        return await get_object_geometry_by_ids_from_db(self._conn, object_geometry_ids)

    async def put_object_geometry(
        self, object_geometry: ObjectGeometryPut, object_geometry_id: int
    ) -> ObjectGeometryDTO:
        return await put_object_geometry_to_db(self._conn, object_geometry, object_geometry_id)

    async def patch_object_geometry(
        self, object_geometry: ObjectGeometryPatch, object_geometry_id: int
    ) -> ObjectGeometryDTO:
        return await patch_object_geometry_to_db(self._conn, object_geometry, object_geometry_id)

    async def delete_object_geometry(self, object_geometry_id: int) -> dict:
        return await delete_object_geometry_in_db(self._conn, object_geometry_id)

    async def add_object_geometry_to_physical_object(
        self, physical_object_id: int, object_geometry: ObjectGeometryPost
    ) -> UrbanObjectDTO:
        return await add_object_geometry_to_physical_object_to_db(self._conn, physical_object_id, object_geometry)

    async def get_physical_objects_by_object_geometry_id(self, object_geometry_id: int) -> list[PhysicalObjectDTO]:
        return await get_physical_objects_by_object_geometry_id_from_db(self._conn, object_geometry_id)
