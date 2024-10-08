"""Service types handlers are defined here."""

from fastapi import Query, Request
from starlette import status

from idu_api.urban_api.logic.service_types import ServiceTypesService
from idu_api.urban_api.schemas import (
    ServiceTypes,
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
    service_types_service: ServiceTypesService = request.state.service_types_service

    service_types = await service_types_service.get_service_types(urban_function_id)

    return [ServiceTypes.from_dto(service_type) for service_type in service_types]


@service_types_router.post(
    "/service_types",
    response_model=ServiceTypes,
    status_code=status.HTTP_201_CREATED,
)
async def add_service_type(request: Request, service_type: ServiceTypesPost) -> ServiceTypes:
    """Add a new service type."""
    service_types_service: ServiceTypesService = request.state.service_types_service

    service_type_dto = await service_types_service.add_service_type(service_type)

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
    service_types_service: ServiceTypesService = request.state.service_types_service

    urban_functions = await service_types_service.get_urban_functions_by_parent_id(parent_id, name, get_all_subtree)

    return [UrbanFunction.from_dto(urban_function) for urban_function in urban_functions]


@service_types_router.post(
    "/urban_functions",
    response_model=UrbanFunction,
    status_code=status.HTTP_201_CREATED,
)
async def add_urban_function(request: Request, urban_function: UrbanFunctionPost) -> UrbanFunction:
    """Add a new urban function."""
    service_types_service: ServiceTypesService = request.state.service_types_service

    urban_function_dto = await service_types_service.add_urban_function(urban_function)

    return UrbanFunction.from_dto(urban_function_dto)
