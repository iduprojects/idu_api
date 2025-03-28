"""Physical object type handlers are defined here."""

from fastapi import HTTPException, Path, Query, Request
from starlette import status

from idu_api.urban_api.logic.physical_object_types import PhysicalObjectTypesService
from idu_api.urban_api.schemas import (
    OkResponse,
    PhysicalObjectFunction,
    PhysicalObjectFunctionPatch,
    PhysicalObjectFunctionPost,
    PhysicalObjectFunctionPut,
    PhysicalObjectsTypesHierarchy,
    PhysicalObjectType,
    PhysicalObjectTypePatch,
    PhysicalObjectTypePost,
    ServiceType,
)

from .routers import physical_object_types_router


@physical_object_types_router.get(
    "/physical_object_types",
    response_model=list[PhysicalObjectType],
    status_code=status.HTTP_200_OK,
)
async def get_physical_object_types(
    request: Request,
    physical_object_function_id: int | None = Query(None, description="to filter by physical object function", gt=0),
    name: str | None = Query(None, description="to filter by name (case-insensitive)"),
) -> list[PhysicalObjectType]:
    """
    ## Get all physical object types.

    ### Parameters:
    - **physical_object_function_id** (int, Query): Filter results by physical object function.
    - **name** (str | None, Query): Filters service types by a case-insensitive substring match.

    ### Returns:
    - **list[PhysicalObjectType]**: A list of all physical object types.
    """
    physical_object_types_service: PhysicalObjectTypesService = request.state.physical_object_types_service

    physical_object_types = await physical_object_types_service.get_physical_object_types(
        physical_object_function_id, name
    )

    return [PhysicalObjectType.from_dto(object_type) for object_type in physical_object_types]


@physical_object_types_router.post(
    "/physical_object_types",
    response_model=PhysicalObjectType,
    status_code=status.HTTP_201_CREATED,
)
async def add_physical_object_type(
    request: Request, physical_object_type: PhysicalObjectTypePost
) -> PhysicalObjectType:
    """
    ## Create a new physical object type.

    ### Parameters:
    - **physical_object_type** (PhysicalObjectTypePost, Body): Data for the new physical object type.

    ### Returns:
    - **PhysicalObjectType**: The created physical object type.

    ### Errors:
    - **404 Not Found**: If the physical object function does not exist.
    - **409 Conflict**: If a physical object type with the such name already exists.
    """
    physical_object_types_service: PhysicalObjectTypesService = request.state.physical_object_types_service

    physical_object_type_dto = await physical_object_types_service.add_physical_object_type(physical_object_type)

    return PhysicalObjectType.from_dto(physical_object_type_dto)


@physical_object_types_router.patch(
    "/physical_object_types/{physical_object_type_id}",
    response_model=PhysicalObjectType,
    status_code=status.HTTP_200_OK,
)
async def patch_physical_object_type(
    request: Request,
    physical_object_type: PhysicalObjectTypePatch,
    physical_object_type_id: int = Path(..., description="physical object type identifier", gt=0),
) -> PhysicalObjectType:
    """
    ## Partially update a physical object type.

    ### Parameters:
    - **physical_object_type_id** (int, Path): Unique identifier of the physical object type.
    - **physical_object_type** (PhysicalObjectTypePatch, Body): Fields to update in the physical object type.

    ### Returns:
    - **PhysicalObjectType**: The updated physical object type with modified attributes.

    ### Errors:
    - **404 Not Found**: If the physical object type (or related entity) does not exist.
    - **409 Conflict**: If a physical object type with the such name already exists.
    """
    physical_object_types_service: PhysicalObjectTypesService = request.state.physical_object_types_service

    physical_object_type_dto = await physical_object_types_service.patch_physical_object_type(
        physical_object_type_id, physical_object_type
    )

    return PhysicalObjectType.from_dto(physical_object_type_dto)


@physical_object_types_router.delete(
    "/physical_object_types/{physical_object_type_id}",
    response_model=OkResponse,
    status_code=status.HTTP_200_OK,
)
async def delete_physical_object_type(
    request: Request, physical_object_type_id: int = Path(..., description="physical object type identifier", gt=0)
) -> OkResponse:
    """
    ## Delete a physical object type by its identifier.

    ### Parameters:
    - **physical_object_type_id** (int, Path): Unique identifier of the physical object type.

    ### Returns:
    - **OkResponse**: A confirmation message of the deletion.

    ### Errors:
    - **404 Not Found**: If the physical object type does not exist.
    """
    physical_object_types_service: PhysicalObjectTypesService = request.state.physical_object_types_service

    await physical_object_types_service.delete_physical_object_type(physical_object_type_id)

    return OkResponse()


@physical_object_types_router.get(
    "/physical_object_functions_by_parent",
    response_model=list[PhysicalObjectFunction],
    status_code=status.HTTP_200_OK,
)
async def get_physical_object_functions_by_parent_id(
    request: Request,
    parent_id: int | None = Query(
        None,
        description="parent physical object function id to filter, "
        "should be skipped for top level physical object functions",
        gt=0,
    ),
    name: str | None = Query(None, description="search by physical object function name"),
    get_all_subtree: bool = Query(False, description="getting full subtree of physical object functions"),
) -> list[PhysicalObjectFunction]:
    """
    ## Get physical object functions by parent identifier.

    ### Parameters:
    - **parent_id** (int | None, Query): Unique identifier of the parent physical object function. If skipped, it returns the highest level functions.
    - **name** (str | None, Query): Filters results by function name.
    - **get_all_subtree** (bool, Query): If True, retrieves all subtree of physical object functions.

    ### Returns:
    - **list[PhysicalObjectFunction]**: A list of physical object functions matching the filters.

    ### Errors:
    - **404 Not Found**: If the physical object function does not exist.
    """
    physical_object_types_service: PhysicalObjectTypesService = request.state.physical_object_types_service

    physical_object_functions = await physical_object_types_service.get_physical_object_functions_by_parent_id(
        parent_id, name, get_all_subtree
    )

    return [
        PhysicalObjectFunction.from_dto(physical_object_function)
        for physical_object_function in physical_object_functions
    ]


@physical_object_types_router.post(
    "/physical_object_functions",
    response_model=PhysicalObjectFunction,
    status_code=status.HTTP_201_CREATED,
)
async def add_physical_object_function(
    request: Request, physical_object_function: PhysicalObjectFunctionPost
) -> PhysicalObjectFunction:
    """
    ## Create a new physical object function.

    ### Parameters:
    - **physical_object_function** (PhysicalObjectFunctionPost, Body): Data for the new physical object function.

    ### Returns:
    - **PhysicalObjectFunction**: The created physical object function.

    ### Errors:
    - **404 Not Found**: If related entity does not exist.
    - **409 Conflict**: If a physical object function with the such name already exists.
    """
    physical_object_types_service: PhysicalObjectTypesService = request.state.physical_object_types_service

    physical_object_function_dto = await physical_object_types_service.add_physical_object_function(
        physical_object_function
    )

    return PhysicalObjectFunction.from_dto(physical_object_function_dto)


@physical_object_types_router.put(
    "/physical_object_functions",
    response_model=PhysicalObjectFunction,
    status_code=status.HTTP_200_OK,
)
async def put_physical_object_function(
    request: Request,
    physical_object_function: PhysicalObjectFunctionPut,
) -> PhysicalObjectFunction:
    """
    ## Update or create a physical object function.

    **NOTE:** If a physical object function with the such name already exists, it will be updated.
    Otherwise, a new physical object function will be created.

    ### Parameters:
    - **physical_object_function** (PhysicalObjectFunctionPut, Body): Data for updating or creating a function.

    ### Returns:
    - **PhysicalObjectFunction**: The updated or created physical object function.

    ### Errors:
    - **404 Not Found**: If related entity does not exist.
    """
    physical_object_types_service: PhysicalObjectTypesService = request.state.physical_object_types_service

    physical_object_function_dto = await physical_object_types_service.put_physical_object_function(
        physical_object_function
    )

    return PhysicalObjectFunction.from_dto(physical_object_function_dto)


@physical_object_types_router.patch(
    "/physical_object_functions/{physical_object_function_id}",
    response_model=PhysicalObjectFunction,
    status_code=status.HTTP_200_OK,
)
async def patch_physical_object_function(
    request: Request,
    physical_object_function: PhysicalObjectFunctionPatch,
    physical_object_function_id: int = Path(..., description="physical object function identifier", gt=0),
) -> PhysicalObjectFunction:
    """
    ## Partially update a physical object function.

    ### Parameters:
    - **physical_object_function_id** (int, Path): Unique identifier of the physical object function.
    - **physical_object_function** (PhysicalObjectFunctionPatch, Body): Fields to update in the function.

    ### Returns:
    - **PhysicalObjectFunction**: The updated physical object function with modified attributes.

    ### Errors:
    - **404 Not Found**: If the physical object function (or related entity) does not exist.
    - **409 Conflict**: If a physical object function with the such name already exists.
    """
    physical_object_types_service: PhysicalObjectTypesService = request.state.physical_object_types_service

    physical_object_function_dto = await physical_object_types_service.patch_physical_object_function(
        physical_object_function_id, physical_object_function
    )

    return PhysicalObjectFunction.from_dto(physical_object_function_dto)


@physical_object_types_router.delete(
    "/physical_object_functions/{physical_object_function_id}",
    response_model=OkResponse,
    status_code=status.HTTP_200_OK,
)
async def delete_physical_object_function(
    request: Request,
    physical_object_function_id: int = Path(..., description="physical object function identifier", gt=0),
) -> OkResponse:
    """
    ## Delete a physical object function by its identifier.

    **WARNING:** This method also removes all child elements of the function.

    ### Parameters:
    - **physical_object_function_id** (int, Path): Unique identifier of the physical object function.

    ### Returns:
    - **OkResponse**: A confirmation message of the deletion.

    ### Errors:
    - **404 Not Found**: If the physical object function does not exist.
    """
    physical_object_types_service: PhysicalObjectTypesService = request.state.physical_object_types_service

    await physical_object_types_service.delete_physical_object_function(physical_object_function_id)

    return OkResponse()


@physical_object_types_router.get(
    "/physical_object_types/hierarchy",
    response_model=list[PhysicalObjectsTypesHierarchy],
    status_code=status.HTTP_200_OK,
)
async def get_physical_object_types_hierarchy(
    request: Request,
    physical_object_types_ids: str | None = Query(
        None, description="list of physical object type ids separated by comma"
    ),
) -> list[PhysicalObjectsTypesHierarchy]:
    """
    ## Get the hierarchy of physical object types.

    ### Parameters:
    - **physical_object_types_ids** (str | None, Query): Comma-separated list of physical object type identifiers.
    If skipped, it will return hierarchy for all physical object types.

    ### Returns:
    - **list[PhysicalObjectsTypesHierarchy]**: A hierarchical representation of physical object types.

    ### Errors:
    - **400 Bad Request**: If given identifiers are invalid.
    - **404 Not Found**: If the physical object types do not exist.
    """
    physical_object_types_service: PhysicalObjectTypesService = request.state.physical_object_types_service

    ids: set[int] | None = None
    if physical_object_types_ids is not None:
        try:
            ids = {int(type_id.strip()) for type_id in physical_object_types_ids.split(",")}
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    hierarchy = await physical_object_types_service.get_physical_object_types_hierarchy(ids)

    return [PhysicalObjectsTypesHierarchy.from_dto(node) for node in hierarchy]


@physical_object_types_router.get(
    "/physical_object_types/{physical_object_type_id}/service_types",
    response_model=list[ServiceType],
    status_code=status.HTTP_200_OK,
)
async def get_service_types(
    request: Request,
    physical_object_type_id: int = Path(..., description="physical object type identifier", gt=0),
) -> list[ServiceType]:
    """
    ## Get all available service types for given physical object type.

    ### Returns:
    - **list[ServiceType]**: A list of service types.
    """
    physical_object_types_service: PhysicalObjectTypesService = request.state.physical_object_types_service

    service_types = await physical_object_types_service.get_service_types_by_physical_object_type(
        physical_object_type_id
    )

    return [ServiceType.from_dto(service_type) for service_type in service_types]
