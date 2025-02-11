"""Service handlers logic of getting entities from the database is defined here."""

from idu_api.common.db.connection.manager import PostgresConnectionManager
from idu_api.urban_api.dto import ServiceDTO, UrbanObjectDTO
from idu_api.urban_api.logic.impl.helpers.services import (
    add_service_to_db,
    add_service_to_object_in_db,
    delete_service_from_db,
    get_service_by_id_from_db,
    patch_service_to_db,
    put_service_to_db,
)
from idu_api.urban_api.logic.services import ServicesDataService
from idu_api.urban_api.schemas import ServicePatch, ServicePost, ServicePut


class ServicesDataServiceImpl(ServicesDataService):
    """Service to manipulate service objects.

    Based on async `PostgresConnectionManager`.
    """

    def __init__(self, connection_manager: PostgresConnectionManager):
        self._connection_manager = connection_manager

    async def get_service_by_id(self, service_id: int) -> ServiceDTO:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_service_by_id_from_db(conn, service_id)

    async def add_service(self, service: ServicePost) -> ServiceDTO:
        async with self._connection_manager.get_connection() as conn:
            return await add_service_to_db(conn, service)

    async def put_service(self, service: ServicePut, service_id: int) -> ServiceDTO:
        async with self._connection_manager.get_connection() as conn:
            return await put_service_to_db(conn, service, service_id)

    async def patch_service(self, service: ServicePatch, service_id: int) -> ServiceDTO:
        async with self._connection_manager.get_connection() as conn:
            return await patch_service_to_db(conn, service, service_id)

    async def delete_service(self, service_id: int) -> dict:
        async with self._connection_manager.get_connection() as conn:
            return await delete_service_from_db(conn, service_id)

    async def add_service_to_object(
        self, service_id: int, physical_object_id: int, object_geometry_id: int
    ) -> UrbanObjectDTO:
        async with self._connection_manager.get_connection() as conn:
            return await add_service_to_object_in_db(conn, service_id, physical_object_id, object_geometry_id)
