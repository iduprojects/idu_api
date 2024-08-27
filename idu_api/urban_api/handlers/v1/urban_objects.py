"""Urban object handlers are defined here."""

from fastapi import Path, Query, Request
from starlette import status

from idu_api.urban_api.logic.urban_objects import UrbanObjectsService
from idu_api.urban_api.schemas.urban_objects import UrbanObject

from .routers import urban_objects_router


@urban_objects_router.get(
    "/urban_objects/{urban_object_id}",
    response_model=UrbanObject,
    status_code=status.HTTP_200_OK,
)
async def get_urban_object_by_id(
    request: Request,
    urban_object_id: int = Path(..., description="Urban object id"),
) -> UrbanObject:
    """Get an urban object by id."""
    urban_objects_service: UrbanObjectsService = request.state.urban_objects_service

    urban_object = await urban_objects_service.get_urban_object_by_id(urban_object_id)

    return UrbanObject.from_dto(urban_object)


@urban_objects_router.get(
    "/urban_objects_by_physical_object",
    response_model=list[UrbanObject],
    status_code=status.HTTP_200_OK,
)
async def get_urban_objects_by_physical_object_id(
    request: Request,
    physical_object_id: int = Query(..., description="Physical object id"),
) -> list[UrbanObject]:
    """Get a list of urban objects by physical object id."""
    urban_objects_service: UrbanObjectsService = request.state.urban_objects_service

    urban_objects = await urban_objects_service.get_urban_object_by_physical_object_id(physical_object_id)

    return [UrbanObject.from_dto(urban_object) for urban_object in urban_objects]


@urban_objects_router.get(
    "/urban_objects_by_object_geometry",
    response_model=list[UrbanObject],
    status_code=status.HTTP_200_OK,
)
async def get_urban_objects_by_object_geometry_id(
    request: Request,
    object_geometry_id: int = Query(..., description="Object geometry id"),
) -> list[UrbanObject]:
    """Get a list of urban objects by object geometry id."""
    urban_objects_service: UrbanObjectsService = request.state.urban_objects_service

    urban_objects = await urban_objects_service.get_urban_object_by_object_geometry_id(object_geometry_id)

    return [UrbanObject.from_dto(urban_object) for urban_object in urban_objects]


@urban_objects_router.get(
    "/urban_objects_by_service_id",
    response_model=list[UrbanObject],
    status_code=status.HTTP_200_OK,
)
async def get_urban_objects_by_service_id(
    request: Request,
    service_id: int = Query(..., description="Service id"),
) -> list[UrbanObject]:
    """Get a list of urban objects by service id."""
    urban_objects_service: UrbanObjectsService = request.state.urban_objects_service

    urban_objects = await urban_objects_service.get_urban_object_by_service_id(service_id)

    return [UrbanObject.from_dto(urban_object) for urban_object in urban_objects]


@urban_objects_router.delete(
    "/urban_objects/{urban_object_id}",
    response_model=dict,
    status_code=status.HTTP_200_OK,
)
async def delete_urban_object_by_id(
    request: Request, urban_object_id: int = Path(..., description="Urban object id", gt=0)
) -> dict:
    """Delete urban object by given identifier."""
    urban_objects_service: UrbanObjectsService = request.state.urban_objects_service

    return await urban_objects_service.delete_urban_object_by_id(urban_object_id)
