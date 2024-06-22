"""Service types handlers are defined here.
"""

from fastapi import Query, Request
from sqlalchemy.ext.asyncio import AsyncConnection
from starlette import status

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
    response_model=list[ServiceTypes],
    status_code=status.HTTP_200_OK,
)
async def get_service_types(
    request: Request,
    urban_function_id: int | None = Query(None, description="To filter by urban function"),
) -> list[ServiceTypes]:
    """Get service types list."""
    conn: AsyncConnection = request.state.conn

    service_types = await get_service_types_from_db(conn, urban_function_id)

    return [ServiceTypes.from_dto(service_type) for service_type in service_types]


@service_types_router.post(
    "/service_types",
    response_model=ServiceTypes,
    status_code=status.HTTP_201_CREATED,
)
async def add_service_type(request: Request, service_type: ServiceTypesPost) -> ServiceTypes:
    """Add service type."""
    conn: AsyncConnection = request.state.conn

    service_type_dto = await add_service_type_to_db(conn, service_type)

    return ServiceTypes.from_dto(service_type_dto)


@service_types_router.get(
    "/urban_functions_by_parent",
    response_model=list[UrbanFunction],
    status_code=status.HTTP_200_OK,
)
async def get_urban_functions_by_parent_id(
    request: Request,
    parent_id: int | None = Query(
        None, description="Parent urban function id to filter, should be skipped for top level urban functions"
    ),
    name: str | None = Query(None, description="Search by urban function name"),
    get_all_subtree: bool = Query(False, description="Getting full subtree of urban functions"),
) -> list[UrbanFunction]:
    """Get indicators by parent id (skip it to get upper level)."""
    conn: AsyncConnection = request.state.conn

    urban_functions = await get_urban_functions_by_parent_id_from_db(conn, parent_id, name, get_all_subtree)

    return [UrbanFunction.from_dto(urban_function) for urban_function in urban_functions]


@service_types_router.post(
    "/urban_functions",
    response_model=UrbanFunction,
    status_code=status.HTTP_201_CREATED,
)
async def add_urban_function(request: Request, urban_function: UrbanFunctionPost) -> UrbanFunction:
    """Add a new urban function."""
    conn: AsyncConnection = request.state.conn

    urban_function_dto = await add_urban_function_to_db(conn, urban_function)

    return UrbanFunction.from_dto(urban_function_dto)


@service_types_router.get(
    "/service_types_normatives",
    response_model=list[ServiceTypesNormativesData],
    status_code=status.HTTP_200_OK,
)
async def get_service_types_normatives(
    request: Request,
    service_type_id: int | None = Query(None, description="To filter by service type"),
    urban_function_id: int | None = Query(None, description="To filter by urban function"),
    territory_id: int | None = Query(None, description="To filter by territory"),
) -> list[ServiceTypesNormativesData]:
    """Get service types normatives list."""
    conn: AsyncConnection = request.state.conn

    service_types_normatives = await get_service_types_normatives_from_db(
        conn, service_type_id, urban_function_id, territory_id
    )

    return [ServiceTypesNormativesData.from_dto(normative) for normative in service_types_normatives]


@service_types_router.post(
    "/service_types_normatives",
    response_model=ServiceTypesNormativesData,
    status_code=status.HTTP_201_CREATED,
)
async def add_service_type_normative(
    request: Request, normative: ServiceTypesNormativesDataPost
) -> ServiceTypesNormativesData:
    """Add service type normative."""
    conn: AsyncConnection = request.state.conn

    service_type_normative_dto = await add_service_type_normative_to_db(conn, normative)

    return ServiceTypesNormativesData.from_dto(service_type_normative_dto)
