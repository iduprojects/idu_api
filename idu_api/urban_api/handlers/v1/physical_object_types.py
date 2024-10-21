"""Physical object type handlers are defined here."""

from fastapi import Path, Query, Request
from starlette import status

from idu_api.urban_api.logic.physical_object_types import PhysicalObjectTypesService
from idu_api.urban_api.schemas import (
    PhysicalObjectFunction,
    PhysicalObjectFunctionPatch,
    PhysicalObjectFunctionPost,
    PhysicalObjectFunctionPut,
    PhysicalObjectsTypes,
    PhysicalObjectsTypesHierarchy,
    PhysicalObjectsTypesPatch,
    PhysicalObjectsTypesPost,
)

from .routers import physical_object_types_router


@physical_object_types_router.get(
    "/physical_object_types",
    response_model=list[PhysicalObjectsTypes],
    status_code=status.HTTP_200_OK,
)
async def get_physical_object_types(request: Request) -> list[PhysicalObjectsTypes]:
    """Get all physical object types."""
    physical_object_types_service: PhysicalObjectTypesService = request.state.physical_object_types_service

    physical_object_types = await physical_object_types_service.get_physical_object_types()

    return [PhysicalObjectsTypes.from_dto(object_type) for object_type in physical_object_types]


@physical_object_types_router.post(
    "/physical_object_types",
    response_model=PhysicalObjectsTypes,
    status_code=status.HTTP_201_CREATED,
)
async def add_physical_object_type(
    request: Request, physical_object_type: PhysicalObjectsTypesPost
) -> PhysicalObjectsTypes:
    """Add a physical object type."""
    physical_object_types_service: PhysicalObjectTypesService = request.state.physical_object_types_service

    physical_object_type_dto = await physical_object_types_service.add_physical_object_type(physical_object_type)

    return PhysicalObjectsTypes.from_dto(physical_object_type_dto)


@physical_object_types_router.patch(
    "/physical_object_types/{physical_object_type_id}",
    response_model=PhysicalObjectsTypes,
    status_code=status.HTTP_200_OK,
)
async def patch_physical_object_type(
    request: Request,
    physical_object_type: PhysicalObjectsTypesPatch,
    physical_object_type_id: int = Path(..., description="physical object type identifier"),
) -> PhysicalObjectsTypes:
    """Update physical object type by only given attributes."""
    physical_object_types_service: PhysicalObjectTypesService = request.state.physical_object_types_service

    physical_object_type_dto = await physical_object_types_service.patch_physical_object_type(
        physical_object_type_id, physical_object_type
    )

    return PhysicalObjectsTypes.from_dto(physical_object_type_dto)


@physical_object_types_router.delete(
    "/physical_object_types/{physical_object_type_id}",
    response_model=dict,
    status_code=status.HTTP_200_OK,
)
async def delete_physical_object_type(
    request: Request, physical_object_type_id: int = Path(..., description="physical object type identifier")
) -> dict:
    """Delete physical object type by id."""
    physical_object_types_service: PhysicalObjectTypesService = request.state.physical_object_types_service

    return await physical_object_types_service.delete_physical_object_type(physical_object_type_id)


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
    ),
    name: str | None = Query(None, description="search by physical object function name"),
    get_all_subtree: bool = Query(False, description="getting full subtree of physical object functions"),
) -> list[PhysicalObjectFunction]:
    """Get physical object functions by parent id (skip it to get upper level)."""
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
    """Add a new physical object function."""
    physical_object_types_service: PhysicalObjectTypesService = request.state.physical_object_types_service

    physical_object_function_dto = await physical_object_types_service.add_physical_object_function(
        physical_object_function
    )

    return PhysicalObjectFunction.from_dto(physical_object_function_dto)


@physical_object_types_router.put(
    "/physical_object_functions/{physical_object_function_id}",
    response_model=PhysicalObjectFunction,
    status_code=status.HTTP_200_OK,
)
async def put_physical_object_function(
    request: Request,
    physical_object_function: PhysicalObjectFunctionPut,
    physical_object_function_id: int = Path(..., description="physical object function identifier"),
) -> PhysicalObjectFunction:
    """Update physical object function by all its attributes."""
    physical_object_types_service: PhysicalObjectTypesService = request.state.physical_object_types_service

    physical_object_function_dto = await physical_object_types_service.put_physical_object_function(
        physical_object_function_id, physical_object_function
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
    physical_object_function_id: int = Path(..., description="physical object function identifier"),
) -> PhysicalObjectFunction:
    """Update physical object function by only given attributes."""
    physical_object_types_service: PhysicalObjectTypesService = request.state.physical_object_types_service

    physical_object_function_dto = await physical_object_types_service.patch_physical_object_function(
        physical_object_function_id, physical_object_function
    )

    return PhysicalObjectFunction.from_dto(physical_object_function_dto)


@physical_object_types_router.delete(
    "/physical_object_functions/{physical_object_function_id}",
    response_model=dict,
    status_code=status.HTTP_200_OK,
)
async def delete_physical_object_function(
    request: Request, physical_object_function_id: int = Path(..., description="physical object function identifier")
) -> dict:
    """Delete physical object function by id.

    It also removes all child elements of this physical object function!!!
    """
    physical_object_types_service: PhysicalObjectTypesService = request.state.physical_object_types_service

    return await physical_object_types_service.delete_physical_object_function(physical_object_function_id)


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
    """Get physical object types hierarchy (from top-level physical object function to physical object type
    based on a list of required physical object type ids.

    If the list of identifiers was not passed, it returns the full hierarchy.
    """
    physical_object_types_service: PhysicalObjectTypesService = request.state.physical_object_types_service

    hierarchy = await physical_object_types_service.get_physical_object_types_hierarchy(physical_object_types_ids)

    return [PhysicalObjectsTypesHierarchy.from_dto(node) for node in hierarchy]
