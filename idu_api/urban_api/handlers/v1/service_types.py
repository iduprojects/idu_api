"""Service types handlers are defined here."""

from fastapi import Path, Query, Request
from starlette import status

from idu_api.urban_api.logic.service_types import ServiceTypesService
from idu_api.urban_api.schemas import (
    ServiceTypes,
    ServiceTypesPatch,
    ServiceTypesPost,
    ServiceTypesPut,
    UrbanFunction,
    UrbanFunctionPatch,
    UrbanFunctionPost,
    UrbanFunctionPut,
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


@service_types_router.put(
    "/service_types/{service_type_id}",
    response_model=ServiceTypes,
    status_code=status.HTTP_200_OK,
)
async def put_service_type(
    request: Request,
    service_type: ServiceTypesPut,
    service_type_id: int = Path(..., description="service type identifier"),
) -> ServiceTypes:
    """Update service type by all its attributes."""
    service_types_service: ServiceTypesService = request.state.service_types_service

    service_type_dto = await service_types_service.put_service_type(service_type_id, service_type)

    return ServiceTypes.from_dto(service_type_dto)


@service_types_router.patch(
    "/service_types/{service_type_id}",
    response_model=ServiceTypes,
    status_code=status.HTTP_200_OK,
)
async def patch_service_type(
    request: Request,
    service_type: ServiceTypesPatch,
    service_type_id: int = Path(..., description="service type identifier"),
) -> ServiceTypes:
    """Update service type by only given attributes."""
    service_types_service: ServiceTypesService = request.state.service_types_service

    service_type_dto = await service_types_service.patch_service_type(service_type_id, service_type)

    return ServiceTypes.from_dto(service_type_dto)


@service_types_router.delete(
    "/service_types/{service_type_id}",
    response_model=dict,
    status_code=status.HTTP_200_OK,
)
async def delete_service_type(
    request: Request, service_type_id: int = Path(..., description="service type identifier")
) -> dict:
    """Delete service type by id."""
    service_types_service: ServiceTypesService = request.state.service_types_service

    return await service_types_service.delete_service_type(service_type_id)


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


@service_types_router.put(
    "/urban_functions/{urban_function_id}",
    response_model=UrbanFunction,
    status_code=status.HTTP_200_OK,
)
async def put_urban_function(
    request: Request,
    urban_function: UrbanFunctionPut,
    urban_function_id: int = Path(..., description="urban function identifier"),
) -> UrbanFunction:
    """Update urban function by all its attributes."""
    service_types_service: ServiceTypesService = request.state.service_types_service

    urban_function_dto = await service_types_service.put_urban_function(urban_function_id, urban_function)

    return UrbanFunction.from_dto(urban_function_dto)


@service_types_router.patch(
    "/urban_functions/{urban_function_id}",
    response_model=UrbanFunction,
    status_code=status.HTTP_200_OK,
)
async def patch_urban_function(
    request: Request,
    urban_function: UrbanFunctionPatch,
    urban_function_id: int = Path(..., description="urban function identifier"),
) -> UrbanFunction:
    """Update urban function by only given attributes."""
    service_types_service: ServiceTypesService = request.state.service_types_service

    urban_function_dto = await service_types_service.patch_urban_function(urban_function_id, urban_function)

    return UrbanFunction.from_dto(urban_function_dto)


@service_types_router.delete(
    "/urban_functions/{urban_function_id}",
    response_model=dict,
    status_code=status.HTTP_200_OK,
)
async def delete_urban_function(
    request: Request, urban_function_id: int = Path(..., description="service type identifier")
) -> dict:
    """Delete urban function by id."""
    service_types_service: ServiceTypesService = request.state.service_types_service

    return await service_types_service.delete_urban_function(urban_function_id)
