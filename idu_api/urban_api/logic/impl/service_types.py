"""Service types/urban functions handlers logic of getting entities from the database is defined here."""

from idu_api.common.db.connection.manager import PostgresConnectionManager
from idu_api.urban_api.dto import ServiceTypeDTO, ServiceTypesHierarchyDTO, UrbanFunctionDTO
from idu_api.urban_api.logic.impl.helpers.service_types import (
    add_service_type_to_db,
    add_urban_function_to_db,
    delete_service_type_from_db,
    delete_urban_function_from_db,
    get_service_types_from_db,
    get_service_types_hierarchy_from_db,
    get_urban_functions_by_parent_id_from_db,
    patch_service_type_to_db,
    patch_urban_function_to_db,
    put_service_type_to_db,
    put_urban_function_to_db,
)
from idu_api.urban_api.logic.service_types import ServiceTypesService
from idu_api.urban_api.schemas import (
    ServiceTypePatch,
    ServiceTypePost,
    ServiceTypePut,
    UrbanFunctionPatch,
    UrbanFunctionPost,
    UrbanFunctionPut,
)


class ServiceTypesServiceImpl(ServiceTypesService):
    """Service to manipulate service types objects.

    Based on async `PostgresConnectionManager`.
    """

    def __init__(self, connection_manager: PostgresConnectionManager):
        self._connection_manager = connection_manager

    async def get_service_types(self, urban_function_id: int | None) -> list[ServiceTypeDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_service_types_from_db(conn, urban_function_id)

    async def add_service_type(self, service_type: ServiceTypePost) -> ServiceTypeDTO:
        async with self._connection_manager.get_connection() as conn:
            return await add_service_type_to_db(conn, service_type)

    async def put_service_type(self, service_type: ServiceTypePut):
        async with self._connection_manager.get_connection() as conn:
            return await put_service_type_to_db(conn, service_type)

    async def patch_service_type(self, service_type_id: int, service_type: ServiceTypePatch):
        async with self._connection_manager.get_connection() as conn:
            return await patch_service_type_to_db(conn, service_type_id, service_type)

    async def delete_service_type(self, service_type_id: int):
        async with self._connection_manager.get_connection() as conn:
            return await delete_service_type_from_db(conn, service_type_id)

    async def get_urban_functions_by_parent_id(
        self,
        parent_id: int | None,
        name: str | None,
        get_all_subtree: bool,
    ) -> list[UrbanFunctionDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_urban_functions_by_parent_id_from_db(conn, parent_id, name, get_all_subtree)

    async def add_urban_function(self, urban_function: UrbanFunctionPost) -> UrbanFunctionDTO:
        async with self._connection_manager.get_connection() as conn:
            return await add_urban_function_to_db(conn, urban_function)

    async def put_urban_function(self, urban_function: UrbanFunctionPut):
        async with self._connection_manager.get_connection() as conn:
            return await put_urban_function_to_db(conn, urban_function)

    async def patch_urban_function(self, urban_function_id: int, urban_function: UrbanFunctionPatch):
        async with self._connection_manager.get_connection() as conn:
            return await patch_urban_function_to_db(conn, urban_function_id, urban_function)

    async def delete_urban_function(self, urban_function_id: int):
        async with self._connection_manager.get_connection() as conn:
            return await delete_urban_function_from_db(conn, urban_function_id)

    async def get_service_types_hierarchy(self, service_type_ids: str | None) -> list[ServiceTypesHierarchyDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_service_types_hierarchy_from_db(conn, service_type_ids)
