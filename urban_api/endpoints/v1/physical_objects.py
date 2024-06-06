"""
Physical object endpoints are defined here.
"""

from typing import List

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncConnection
from starlette import status

from urban_api.db.connection import get_connection
from urban_api.logic.physical_objects import (
    add_physical_object_type_to_db,
    get_physical_object_types_from_db,
)
from urban_api.schemas import (
    PhysicalObjectsTypes,
    PhysicalObjectsTypesPost,
)


from .routers import physical_objects_router


@physical_objects_router.get(
    "/physical_object_types",
    response_model=List[PhysicalObjectsTypes],
    status_code=status.HTTP_200_OK,
)
async def get_physical_object_types(
        connection: AsyncConnection = Depends(get_connection)
) -> List[PhysicalObjectsTypes]:
    """
    Summary:
        Get physical object types list

    Description:
        Get a list of all physical object types
    """

    physical_object_types = await get_physical_object_types_from_db(connection)

    return [PhysicalObjectsTypes.from_dto(object_type) for object_type in physical_object_types]


@physical_objects_router.post(
    "/physical_object_types",
    response_model=PhysicalObjectsTypes,
    status_code=status.HTTP_201_CREATED,
)
async def add_physical_object_type(
    physical_object_type: PhysicalObjectsTypesPost, connection: AsyncConnection = Depends(get_connection)
) -> PhysicalObjectsTypes:
    """
    Summary:
        Add physical object type

    Description:
        Add a physical object type
    """

    physical_object_type_dto = await add_physical_object_type_to_db(physical_object_type, connection)

    return PhysicalObjectsTypes.from_dto(physical_object_type_dto)
