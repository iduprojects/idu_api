"""Urban objects handlers logic of getting entities from the database is defined here."""

from idu_api.common.db.connection.manager import PostgresConnectionManager
from idu_api.urban_api.dto import UrbanObjectDTO
from idu_api.urban_api.logic.impl.helpers.urban_objects import (
    delete_urban_object_by_id_from_db,
    get_urban_objects_by_ids_from_db,
    get_urban_objects_by_object_geometry_id_from_db,
    get_urban_objects_by_physical_object_id_from_db,
    get_urban_objects_by_service_id_from_db,
    get_urban_objects_by_territory_id_from_db,
    patch_urban_object_to_db,
)
from idu_api.urban_api.logic.urban_objects import UrbanObjectsService
from idu_api.urban_api.schemas import UrbanObjectPatch


class UrbanObjectsServiceImpl(UrbanObjectsService):
    """Service to manipulate urban objects.

    Based on async `PostgresConnectionManager`.
    """

    def __init__(self, connection_manager: PostgresConnectionManager):
        self._connection_manager = connection_manager

    async def get_urban_object_by_id(self, urban_object_id: int) -> UrbanObjectDTO:
        async with self._connection_manager.get_ro_connection() as conn:
            return (await get_urban_objects_by_ids_from_db(conn, [urban_object_id]))[0]

    async def get_urban_object_by_physical_object_id(self, physical_object_id: int) -> list[UrbanObjectDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_urban_objects_by_physical_object_id_from_db(conn, physical_object_id)

    async def get_urban_object_by_object_geometry_id(self, object_geometry_id: int) -> list[UrbanObjectDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_urban_objects_by_object_geometry_id_from_db(conn, object_geometry_id)

    async def get_urban_object_by_service_id(self, service_id: int) -> list[UrbanObjectDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_urban_objects_by_service_id_from_db(conn, service_id)

    async def delete_urban_object_by_id(self, urban_object_id: int) -> dict:
        async with self._connection_manager.get_connection() as conn:
            return await delete_urban_object_by_id_from_db(conn, urban_object_id)

    async def get_urban_objects_by_territory_id(
        self, territory_id: int, service_type_id: int | None, physical_object_type_id: int | None
    ) -> list[UrbanObjectDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_urban_objects_by_territory_id_from_db(
                conn, territory_id, service_type_id, physical_object_type_id
            )

    async def patch_urban_object_to_db(self, urban_object: UrbanObjectPatch, urban_object_id: int) -> UrbanObjectDTO:
        async with self._connection_manager.get_connection() as conn:
            return await patch_urban_object_to_db(conn, urban_object, urban_object_id)
