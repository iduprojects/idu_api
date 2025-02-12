"""Object geometries handlers logic of getting entities from the database is defined here."""

from idu_api.common.db.connection.manager import PostgresConnectionManager
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

    Based on async `PostgresConnectionManager`.
    """

    def __init__(self, connection_manager: PostgresConnectionManager):
        self._connection_manager = connection_manager

    async def get_object_geometry_by_ids(self, object_geometry_ids: list[int]) -> list[ObjectGeometryDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_object_geometry_by_ids_from_db(conn, object_geometry_ids)

    async def put_object_geometry(
        self, object_geometry: ObjectGeometryPut, object_geometry_id: int
    ) -> ObjectGeometryDTO:
        async with self._connection_manager.get_connection() as conn:
            return await put_object_geometry_to_db(conn, object_geometry, object_geometry_id)

    async def patch_object_geometry(
        self, object_geometry: ObjectGeometryPatch, object_geometry_id: int
    ) -> ObjectGeometryDTO:
        async with self._connection_manager.get_connection() as conn:
            return await patch_object_geometry_to_db(conn, object_geometry, object_geometry_id)

    async def delete_object_geometry(self, object_geometry_id: int) -> dict:
        async with self._connection_manager.get_connection() as conn:
            return await delete_object_geometry_in_db(conn, object_geometry_id)

    async def add_object_geometry_to_physical_object(
        self, physical_object_id: int, object_geometry: ObjectGeometryPost
    ) -> UrbanObjectDTO:
        async with self._connection_manager.get_connection() as conn:
            return await add_object_geometry_to_physical_object_to_db(conn, physical_object_id, object_geometry)

    async def get_physical_objects_by_object_geometry_id(self, object_geometry_id: int) -> list[PhysicalObjectDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_physical_objects_by_object_geometry_id_from_db(conn, object_geometry_id)
