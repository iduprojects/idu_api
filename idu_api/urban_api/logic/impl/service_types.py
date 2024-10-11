"""Service types/urban functions handlers logic of getting entities from the database is defined here."""

from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.urban_api.dto import ServiceTypesDTO, ServiceTypesHierarchyDTO, UrbanFunctionDTO
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
    ServiceTypesPatch,
    ServiceTypesPost,
    ServiceTypesPut,
    UrbanFunctionPatch,
    UrbanFunctionPost,
    UrbanFunctionPut,
)


class ServiceTypesServiceImpl(ServiceTypesService):
    """Service to manipulate service types objects.

    Based on async SQLAlchemy connection.
    """

    def __init__(self, conn: AsyncConnection):
        self._conn = conn

    async def get_service_types(self, urban_function_id: int | None) -> list[ServiceTypesDTO]:
        return await get_service_types_from_db(self._conn, urban_function_id)

    async def add_service_type(self, service_type: ServiceTypesPost) -> ServiceTypesDTO:
        return await add_service_type_to_db(self._conn, service_type)

    async def put_service_type(self, service_type_id: int, service_type: ServiceTypesPut):
        return await put_service_type_to_db(self._conn, service_type_id, service_type)

    async def patch_service_type(self, service_type_id: int, service_type: ServiceTypesPatch):
        return await patch_service_type_to_db(self._conn, service_type_id, service_type)

    async def delete_service_type(self, service_type_id: int):
        return await delete_service_type_from_db(self._conn, service_type_id)

    async def get_urban_functions_by_parent_id(
        self,
        parent_id: int | None,
        name: str | None,
        get_all_subtree: bool,
    ) -> list[UrbanFunctionDTO]:
        return await get_urban_functions_by_parent_id_from_db(self._conn, parent_id, name, get_all_subtree)

    async def add_urban_function(self, urban_function: UrbanFunctionPost) -> UrbanFunctionDTO:
        return await add_urban_function_to_db(self._conn, urban_function)

    async def put_urban_function(self, urban_function_id: int, urban_function: UrbanFunctionPut):
        return await put_urban_function_to_db(self._conn, urban_function_id, urban_function)

    async def patch_urban_function(self, urban_function_id: int, urban_function: UrbanFunctionPatch):
        return await patch_urban_function_to_db(self._conn, urban_function_id, urban_function)

    async def delete_urban_function(self, urban_function_id: int):
        return await delete_urban_function_from_db(self._conn, urban_function_id)

    async def get_service_types_hierarchy(self, service_type_ids: str | None) -> list[ServiceTypesHierarchyDTO]:
        return await get_service_types_hierarchy_from_db(self._conn, service_type_ids)
