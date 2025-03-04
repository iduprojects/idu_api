"""Physical object types handlers logic is defined here."""

from idu_api.common.db.connection.manager import PostgresConnectionManager
from idu_api.urban_api.dto import (
    PhysicalObjectFunctionDTO,
    PhysicalObjectTypeDTO,
    PhysicalObjectTypesHierarchyDTO,
)
from idu_api.urban_api.logic.impl.helpers.physical_object_types import (
    add_physical_object_function_to_db,
    add_physical_object_type_to_db,
    delete_physical_object_function_from_db,
    delete_physical_object_type_from_db,
    get_physical_object_functions_by_parent_id_from_db,
    get_physical_object_types_from_db,
    get_physical_object_types_hierarchy_from_db,
    patch_physical_object_function_to_db,
    patch_physical_object_type_to_db,
    put_physical_object_function_to_db,
)
from idu_api.urban_api.logic.physical_object_types import PhysicalObjectTypesService
from idu_api.urban_api.schemas import (
    PhysicalObjectFunctionPatch,
    PhysicalObjectFunctionPost,
    PhysicalObjectFunctionPut,
    PhysicalObjectTypePatch,
    PhysicalObjectTypePost,
)


class PhysicalObjectTypesServiceImpl(PhysicalObjectTypesService):
    """Service to manipulate physical objects entities.

    Based on async `PostgresConnectionManager`.
    """

    def __init__(self, connection_manager: PostgresConnectionManager):
        self._connection_manager = connection_manager

    async def get_physical_object_types(self) -> list[PhysicalObjectTypeDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_physical_object_types_from_db(conn)

    async def add_physical_object_type(self, physical_object_type: PhysicalObjectTypePost) -> PhysicalObjectTypeDTO:
        async with self._connection_manager.get_connection() as conn:
            return await add_physical_object_type_to_db(conn, physical_object_type)

    async def patch_physical_object_type(
        self, physical_object_type_id: int, physical_object_type: PhysicalObjectTypePatch
    ) -> PhysicalObjectTypeDTO:
        async with self._connection_manager.get_connection() as conn:
            return await patch_physical_object_type_to_db(conn, physical_object_type_id, physical_object_type)

    async def delete_physical_object_type(self, physical_object_type_id: int) -> dict:
        async with self._connection_manager.get_connection() as conn:
            return await delete_physical_object_type_from_db(conn, physical_object_type_id)

    async def get_physical_object_functions_by_parent_id(
        self,
        parent_id: int | None,
        name: str | None,
        get_all_subtree: bool,
    ) -> list[PhysicalObjectFunctionDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_physical_object_functions_by_parent_id_from_db(conn, parent_id, name, get_all_subtree)

    async def add_physical_object_function(
        self, physical_object_function: PhysicalObjectFunctionPost
    ) -> PhysicalObjectFunctionDTO:
        async with self._connection_manager.get_connection() as conn:
            return await add_physical_object_function_to_db(conn, physical_object_function)

    async def put_physical_object_function(
        self, physical_object_function: PhysicalObjectFunctionPut
    ) -> PhysicalObjectFunctionDTO:
        async with self._connection_manager.get_connection() as conn:
            return await put_physical_object_function_to_db(conn, physical_object_function)

    async def patch_physical_object_function(
        self, physical_object_function_id: int, physical_object_function: PhysicalObjectFunctionPatch
    ) -> PhysicalObjectFunctionDTO:
        async with self._connection_manager.get_connection() as conn:
            return await patch_physical_object_function_to_db(
                conn, physical_object_function_id, physical_object_function
            )

    async def delete_physical_object_function(self, physical_object_function_id: int) -> dict:
        async with self._connection_manager.get_connection() as conn:
            return await delete_physical_object_function_from_db(conn, physical_object_function_id)

    async def get_physical_object_types_hierarchy(self, ids: set[int] | None) -> list[PhysicalObjectTypesHierarchyDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_physical_object_types_hierarchy_from_db(conn, ids)
