"""Service types handlers are defined here."""

from fastapi import HTTPException, Path, Query, Request
from starlette import status

from idu_api.urban_api.logic.service_types import ServiceTypesService
from idu_api.urban_api.schemas import (
    OkResponse,
    PhysicalObjectType,
    ServiceType,
    ServiceTypePatch,
    ServiceTypePost,
    ServiceTypePut,
    ServiceTypesHierarchy,
    SocGroupWithServiceTypes,
    UrbanFunction,
    UrbanFunctionPatch,
    UrbanFunctionPost,
    UrbanFunctionPut,
)

from .routers import service_types_router


@service_types_router.get(
    "/service_types",
    response_model=list[ServiceType],
    status_code=status.HTTP_200_OK,
)
async def get_service_types(
    request: Request,
    urban_function_id: int | None = Query(None, description="to filter by urban function", gt=0),
    name: str | None = Query(None, description="to filter by name (case-insensitive)"),
) -> list[ServiceType]:
    """
    ## Get all service types.

    ### Parameters:
    - **urban_function_id** (int, Query): Filter results by urban function.
    - **name** (str | None, Query): Filters service types by a case-insensitive substring match.

    ### Returns:
    - **list[ServiceType]**: A list of all service types.
    """
    service_types_service: ServiceTypesService = request.state.service_types_service

    service_types = await service_types_service.get_service_types(urban_function_id, name)

    return [ServiceType.from_dto(service_type) for service_type in service_types]


@service_types_router.post(
    "/service_types",
    response_model=ServiceType,
    status_code=status.HTTP_201_CREATED,
)
async def add_service_type(request: Request, service_type: ServiceTypePost) -> ServiceType:
    """
    ## Create a new service type.

    ### Parameters:
    - **service_type** (ServiceTypePost, Body): Data for the new service type.

    ### Returns:
    - **ServiceType**: The created service type.

    ### Errors:
    - **404 Not Found**: If related entity does not exist.
    - **409 Conflict**: If a service type with the such name already exists.
    """
    service_types_service: ServiceTypesService = request.state.service_types_service

    service_type_dto = await service_types_service.add_service_type(service_type)

    return ServiceType.from_dto(service_type_dto)


@service_types_router.put(
    "/service_types",
    response_model=ServiceType,
    status_code=status.HTTP_200_OK,
)
async def put_service_type(
    request: Request,
    service_type: ServiceTypePut,
) -> ServiceType:
    """
    ## Update or create a service type.

    **NOTE:** If a service type with the such name already exists, it will be updated.
    Otherwise, a new service type will be created.

    ### Parameters:
    - **service_type** (ServiceTypePut, Body): Data for updating or creating a service type.

    ### Returns:
    - **ServiceType**: The updated or created service type.

    ### Errors:
    - **404 Not Found**: If related entity does not exist.
    """
    service_types_service: ServiceTypesService = request.state.service_types_service

    service_type_dto = await service_types_service.put_service_type(service_type)

    return ServiceType.from_dto(service_type_dto)


@service_types_router.patch(
    "/service_types/{service_type_id}",
    response_model=ServiceType,
    status_code=status.HTTP_200_OK,
)
async def patch_service_type(
    request: Request,
    service_type: ServiceTypePatch,
    service_type_id: int = Path(..., description="service type identifier", gt=0),
) -> ServiceType:
    """
    ## Partially update a service type.

    ### Parameters:
    - **service_type_id** (int, Path): Unique identifier of the service type.
    - **service_type** (ServiceTypePatch, Body): Fields to update in the service type.

    ### Returns:
    - **ServiceType**: The updated service type with modified attributes.

    ### Errors:
    - **404 Not Found**: If the service type (or related entity) does not exist.
    - **409 Conflict**: If a service type with the such name already exists.
    """
    service_types_service: ServiceTypesService = request.state.service_types_service

    service_type_dto = await service_types_service.patch_service_type(service_type_id, service_type)

    return ServiceType.from_dto(service_type_dto)


@service_types_router.delete(
    "/service_types/{service_type_id}",
    response_model=OkResponse,
    status_code=status.HTTP_200_OK,
)
async def delete_service_type(
    request: Request, service_type_id: int = Path(..., description="service type identifier", gt=0)
) -> OkResponse:
    """
    ## Delete a service type by its identifier.

    ### Parameters:
    - **service_type_id** (int, Path): Unique identifier of the service type.

    ### Returns:
    - **OkResponse**: A confirmation message of the deletion.

    ### Errors:
    - **404 Not Found**: If the service type does not exist.
    """
    service_types_service: ServiceTypesService = request.state.service_types_service

    await service_types_service.delete_service_type(service_type_id)

    return OkResponse()


@service_types_router.get(
    "/urban_functions_by_parent",
    response_model=list[UrbanFunction],
    status_code=status.HTTP_200_OK,
)
async def get_urban_functions_by_parent_id(
    request: Request,
    parent_id: int | None = Query(
        None, description="parent urban function id to filter, should be skipped for top level urban functions", gt=0
    ),
    name: str | None = Query(None, description="search by urban function name"),
    get_all_subtree: bool = Query(False, description="getting full subtree of urban functions"),
) -> list[UrbanFunction]:
    """
    ## Get urban functions by parent identifier.

    ### Parameters:
    - **parent_id** (int | None, Query): Unique identifier of the parent urban function. If skipped, it returns the highest level functions.
    - **name** (str | None, Query): Filters results by function name.
    - **get_all_subtree** (bool, Query): If True, retrieves all subtree of urban functions.

    ### Returns:
    - **list[UrbanFunction]**: A list of urban functions matching the filters.

    ### Errors:
    - **404 Not Found**: If the urban function does not exist.
    """
    service_types_service: ServiceTypesService = request.state.service_types_service

    urban_functions = await service_types_service.get_urban_functions_by_parent_id(parent_id, name, get_all_subtree)

    return [UrbanFunction.from_dto(urban_function) for urban_function in urban_functions]


@service_types_router.post(
    "/urban_functions",
    response_model=UrbanFunction,
    status_code=status.HTTP_201_CREATED,
)
async def add_urban_function(request: Request, urban_function: UrbanFunctionPost) -> UrbanFunction:
    """
    ## Create a new urban function.

    ### Parameters:
    - **urban_function** (UrbanFunctionPost, Body): Data for the new urban function.

    ### Returns:
    - **UrbanFunction**: The created urban function.

    ### Errors:
    - **404 Not Found**: If related entity does not exist.
    - **409 Conflict**: If an urban function with the such name already exists.
    """
    service_types_service: ServiceTypesService = request.state.service_types_service

    urban_function_dto = await service_types_service.add_urban_function(urban_function)

    return UrbanFunction.from_dto(urban_function_dto)


@service_types_router.put(
    "/urban_functions",
    response_model=UrbanFunction,
    status_code=status.HTTP_200_OK,
)
async def put_urban_function(
    request: Request,
    urban_function: UrbanFunctionPut,
) -> UrbanFunction:
    """
    ## Update or create an urban function.

    **NOTE:** If an urban function with the such name already exists, it will be updated.
    Otherwise, a new urban function will be created.

    ### Parameters:
    - **urban_function** (UrbanFunctionPut, Body): Data for updating or creating a function.

    ### Returns:
    - **UrbanFunction**: The updated or created urban function.

    ### Errors:
    - **404 Not Found**: If related entity does not exist.
    """
    service_types_service: ServiceTypesService = request.state.service_types_service

    urban_function_dto = await service_types_service.put_urban_function(urban_function)

    return UrbanFunction.from_dto(urban_function_dto)


@service_types_router.patch(
    "/urban_functions/{urban_function_id}",
    response_model=UrbanFunction,
    status_code=status.HTTP_200_OK,
)
async def patch_urban_function(
    request: Request,
    urban_function: UrbanFunctionPatch,
    urban_function_id: int = Path(..., description="urban function identifier", gt=0),
) -> UrbanFunction:
    """
    ## Partially update an urban function.

    ### Parameters:
    - **urban_function_id** (int, Path): Unique identifier of the urban function.
    - **urban_function** (UrbanFunctionPatch, Body): Fields to update in the function.

    ### Returns:
    - **UrbanFunction**: The updated urban function with modified attributes.

    ### Errors:
    - **404 Not Found**: If the urban function (or related entity) does not exist.
    - **409 Conflict**: If an urban function with the such name already exists.
    """
    service_types_service: ServiceTypesService = request.state.service_types_service

    urban_function_dto = await service_types_service.patch_urban_function(urban_function_id, urban_function)

    return UrbanFunction.from_dto(urban_function_dto)


@service_types_router.delete(
    "/urban_functions/{urban_function_id}",
    response_model=OkResponse,
    status_code=status.HTTP_200_OK,
)
async def delete_urban_function(
    request: Request, urban_function_id: int = Path(..., description="urban function identifier", gt=0)
) -> OkResponse:
    """
    ## Delete an urban function by its identifier.

    **WARNING:** This method also removes all child elements of the function.

    ### Parameters:
    - **urban_function_id** (int, Path): Unique identifier of the urban function.

    ### Returns:
    - **OkResponse**: A confirmation message of the deletion.

    ### Errors:
    - **404 Not Found**: If the urban function does not exist.
    """
    service_types_service: ServiceTypesService = request.state.service_types_service

    await service_types_service.delete_urban_function(urban_function_id)

    return OkResponse()


@service_types_router.get(
    "/service_types/hierarchy",
    response_model=list[ServiceTypesHierarchy],
    status_code=status.HTTP_200_OK,
)
async def get_service_types_hierarchy(
    request: Request,
    service_types_ids: str | None = Query(None, description="list of service type ids separated by comma"),
) -> list[ServiceTypesHierarchy]:
    """
    ## Get the hierarchy of service types.

    ### Parameters:
    - **service_types_ids** (str | None, Query): Comma-separated list of service type identifiers.
    If None, it will return hierarchy for all service types.

    ### Returns:
    - **list[ServicesTypesHierarchy]**: A hierarchical representation of service types.

    ### Errors:
    - **404 Not Found**: If the service types do not exist.
    """
    service_types_service: ServiceTypesService = request.state.service_types_service

    ids: set[int] | None = None
    if service_types_ids is not None:
        try:
            ids = {int(type_id.strip()) for type_id in service_types_ids.split(",")}
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    hierarchy = await service_types_service.get_service_types_hierarchy(ids)

    return [ServiceTypesHierarchy.from_dto(node) for node in hierarchy]


@service_types_router.get(
    "/service_types/{service_type_id}/physical_object_types",
    response_model=list[PhysicalObjectType],
    status_code=status.HTTP_200_OK,
)
async def get_physical_object_types(
    request: Request,
    service_type_id: int = Path(..., description="physical object type identifier", gt=0),
) -> list[PhysicalObjectType]:
    """
    ## Get all available physical object types for given service type.

    ### Parameters:
    - **service_type_id** (int, Path): Unique identifier of the service type.

    ### Returns:
    - **list[PhysicalObjectType]**: A list of physical object types.
    """
    service_types_service: ServiceTypesService = request.state.service_types_service

    types = await service_types_service.get_physical_object_types_by_service_type(service_type_id)

    return [PhysicalObjectType.from_dto(object_type) for object_type in types]


@service_types_router.get(
    "/service_types/{service_type_id}/social_groups",
    response_model=list[SocGroupWithServiceTypes],
    status_code=status.HTTP_200_OK,
)
async def get_social_groups(
    request: Request,
    service_type_id: int = Path(..., description="physical object type identifier", gt=0),
) -> list[SocGroupWithServiceTypes]:
    """
    ## Get all social groups for which given service type is important.

    ### Parameters:
    - **service_type_id** (int, Path): Unique identifier of the service type.

    ### Returns:
    - **list[SocGroupWithServiceTypes]**: A list of social groups with associated service types.
    """
    service_types_service: ServiceTypesService = request.state.service_types_service

    soc_groups = await service_types_service.get_social_groups_by_service_type_id(service_type_id)

    return [SocGroupWithServiceTypes.from_dto(group) for group in soc_groups]
