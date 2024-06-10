"""
Service types endpoints are defined here.
"""

from typing import List, Optional

from fastapi import Depends, Query
from sqlalchemy.ext.asyncio import AsyncConnection
from starlette import status

from urban_api.db.connection import get_connection
from urban_api.logic.service_types import (
    add_service_type_normative_to_db,
    add_service_type_to_db,
    add_urban_function_to_db,
    get_service_types_from_db,
    get_service_types_normatives_from_db,
    get_urban_functions_by_parent_id_from_db,
)
from urban_api.schemas import (
    ServiceTypes,
    ServiceTypesNormativesData,
    ServiceTypesNormativesDataPost,
    ServiceTypesPost,
    UrbanFunction,
    UrbanFunctionPost,
)

from .routers import service_types_router


@service_types_router.get(
    "/service_types",
    response_model=List[ServiceTypes],
    status_code=status.HTTP_200_OK,
)
async def get_service_types(
    urban_function_id: Optional[int] = Query(None, description="To filter by urban function"),
    connection: AsyncConnection = Depends(get_connection),
) -> List[ServiceTypes]:
    """
    Summary:
        Get service types list

    Description:
        Get a list of all service types
    """

    service_types = await get_service_types_from_db(urban_function_id, connection)

    return [ServiceTypes.from_dto(service_type) for service_type in service_types]


@service_types_router.post(
    "/service_types",
    response_model=ServiceTypes,
    status_code=status.HTTP_201_CREATED,
)
async def add_service_type(
    service_type: ServiceTypesPost, connection: AsyncConnection = Depends(get_connection)
) -> ServiceTypes:
    """
    Summary:
        Add service type

    Description:
        Add a service type
    """

    service_type_dto = await add_service_type_to_db(service_type, connection)

    return ServiceTypes.from_dto(service_type_dto)


@service_types_router.get(
    "/urban_functions_by_parent",
    response_model=List[UrbanFunction],
    status_code=status.HTTP_200_OK,
)
async def get_urban_functions_by_parent_id(
    parent_id: Optional[int] = Query(
        None, description="Parent urban function id to filter, should be skipped for top level urban functions"
    ),
    name: Optional[str] = Query(None, description="Search by urban function name"),
    get_all_subtree: bool = Query(False, description="Getting full subtree of urban functions"),
    connection: AsyncConnection = Depends(get_connection),
) -> List[UrbanFunction]:
    """
    Summary:
        Get indicators dictionary

    Description:
        Get a list of indicators by parent id
    """

    urban_functions = await get_urban_functions_by_parent_id_from_db(parent_id, name, connection, get_all_subtree)

    return [UrbanFunction.from_dto(urban_function) for urban_function in urban_functions]


@service_types_router.post(
    "/urban_functions",
    response_model=UrbanFunction,
    status_code=status.HTTP_201_CREATED,
)
async def add_urban_function(
    urban_function: UrbanFunctionPost, connection: AsyncConnection = Depends(get_connection)
) -> UrbanFunction:
    """
    Summary:
        Add a new urban function

    Description:
        Add a new urban function
    """

    urban_function_dto = await add_urban_function_to_db(urban_function, connection)

    return UrbanFunction.from_dto(urban_function_dto)


@service_types_router.get(
    "/service_types_normatives",
    response_model=List[ServiceTypesNormativesData],
    status_code=status.HTTP_200_OK,
)
async def get_service_types_normatives(
    service_type_id: Optional[int] = Query(None, description="To filter by service type"),
    urban_function_id: Optional[int] = Query(None, description="To filter by urban function"),
    territory_id: Optional[int] = Query(None, description="To filter by territory"),
    connection: AsyncConnection = Depends(get_connection),
) -> List[ServiceTypesNormativesData]:
    """
    Summary:
        Get service types normatives list

    Description:
        Get a list of all service types normatives
    """

    service_types_normatives = await get_service_types_normatives_from_db(
        service_type_id, urban_function_id, territory_id, connection
    )

    return [ServiceTypesNormativesData.from_dto(normative) for normative in service_types_normatives]


@service_types_router.post(
    "/service_types_normatives",
    response_model=ServiceTypesNormativesData,
    status_code=status.HTTP_201_CREATED,
)
async def add_service_type_normative(
    normative: ServiceTypesNormativesDataPost, connection: AsyncConnection = Depends(get_connection)
) -> ServiceTypesNormativesData:
    """
    Summary:
        Add service type normative

    Description:
        Add a service type normative
    """

    service_type_normative_dto = await add_service_type_normative_to_db(normative, connection)

    return ServiceTypesNormativesData.from_dto(service_type_normative_dto)
