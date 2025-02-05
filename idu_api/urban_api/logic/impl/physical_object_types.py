"""Physical object types handlers logic is defined here."""

from sqlalchemy.ext.asyncio import AsyncConnection

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

    Based on async SQLAlchemy connection.
    """

    def __init__(self, conn: AsyncConnection):
        self._conn = conn

    async def get_physical_object_types(self) -> list[PhysicalObjectTypeDTO]:
        return await get_physical_object_types_from_db(self._conn)

    async def add_physical_object_type(self, physical_object_type: PhysicalObjectTypePost) -> PhysicalObjectTypeDTO:
        return await add_physical_object_type_to_db(self._conn, physical_object_type)

    async def patch_physical_object_type(
        self, physical_object_type_id: int, physical_object_type: PhysicalObjectTypePatch
    ) -> PhysicalObjectTypeDTO:
        return await patch_physical_object_type_to_db(self._conn, physical_object_type_id, physical_object_type)

    async def delete_physical_object_type(self, physical_object_type_id: int) -> dict:
        return await delete_physical_object_type_from_db(self._conn, physical_object_type_id)

    async def get_physical_object_functions_by_parent_id(
        self,
        parent_id: int | None,
        name: str | None,
        get_all_subtree: bool,
    ) -> list[PhysicalObjectFunctionDTO]:
        return await get_physical_object_functions_by_parent_id_from_db(self._conn, parent_id, name, get_all_subtree)

    async def add_physical_object_function(
        self, physical_object_function: PhysicalObjectFunctionPost
    ) -> PhysicalObjectFunctionDTO:
        return await add_physical_object_function_to_db(self._conn, physical_object_function)

    async def put_physical_object_function(
        self, physical_object_function: PhysicalObjectFunctionPut
    ) -> PhysicalObjectFunctionDTO:
        return await put_physical_object_function_to_db(self._conn, physical_object_function)

    async def patch_physical_object_function(
        self, physical_object_function_id: int, physical_object_function: PhysicalObjectFunctionPatch
    ) -> PhysicalObjectFunctionDTO:
        return await patch_physical_object_function_to_db(
            self._conn, physical_object_function_id, physical_object_function
        )

    async def delete_physical_object_function(self, physical_object_function_id: int) -> dict:
        return await delete_physical_object_function_from_db(self._conn, physical_object_function_id)

    async def get_physical_object_types_hierarchy(
        self, physical_object_type_ids: str | None
    ) -> list[PhysicalObjectTypesHierarchyDTO]:
        return await get_physical_object_types_hierarchy_from_db(self._conn, physical_object_type_ids)
