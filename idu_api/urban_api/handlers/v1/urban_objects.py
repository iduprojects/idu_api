"""Urban object handlers are defined here."""

from fastapi import Path, Query, Request
from starlette import status

from idu_api.urban_api.logic.urban_objects import UrbanObjectsService
from idu_api.urban_api.schemas import OkResponse, UrbanObject, UrbanObjectPatch

from .routers import urban_objects_router


@urban_objects_router.get(
    "/urban_objects/{urban_object_id}",
    response_model=UrbanObject,
    status_code=status.HTTP_200_OK,
)
async def get_urban_object_by_id(
    request: Request,
    urban_object_id: int = Path(..., description="urban object identifier"),
) -> UrbanObject:
    """
    ## Get an urban object by its identifier.

    ### Parameters:
    - **urban_object_id** (int, Path): Unique identifier of the urban object.

    ### Returns:
    - **UrbanObject**: The requested urban object (physical object + geometry + service).

    ### Errors:
    - **404 Not Found**: If the urban object does not exist.
    """
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
    physical_object_id: int = Query(..., description="physical object identifier", gt=0),
) -> list[UrbanObject]:
    """
    ## Get a list of urban objects by physical object identifier.

    ### Parameters:
    - **physical_object_id** (int, Query): Unique identifier of the physical object.

    ### Returns:
    - **list[UrbanObject]**: A list of urban objects associated with the specified physical object.

    ### Errors:
    - **404 Not Found**: If the physical object does not exist.
    """
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
    object_geometry_id: int = Query(..., description="object geometry identifier", gt=0),
) -> list[UrbanObject]:
    """
    ## Get a list of urban objects by object geometry identifier.

    ### Parameters:
    - **object_geometry_id** (int, Query): Unique identifier of object geometry.

    ### Returns:
    - **list[UrbanObject]**: A list of urban objects associated with the specified object geometry.

    ### Errors:
    - **404 Not Found**: If the object geometry does not exist.
    """
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
    service_id: int = Query(..., description="service identifier", gt=0),
) -> list[UrbanObject]:
    """
    ## Get a list of urban objects by service identifier.

    ### Parameters:
    - **service_id** (int, Query): Unique identifier of the service.

    ### Returns:
    - **list[UrbanObject]**: A list of urban objects associated with the specified service.

    ### Errors:
    - **404 Not Found**: If the service does not exist.
    """
    urban_objects_service: UrbanObjectsService = request.state.urban_objects_service

    urban_objects = await urban_objects_service.get_urban_object_by_service_id(service_id)

    return [UrbanObject.from_dto(urban_object) for urban_object in urban_objects]


@urban_objects_router.delete(
    "/urban_objects/{urban_object_id}",
    response_model=OkResponse,
    status_code=status.HTTP_200_OK,
)
async def delete_urban_object_by_id(
    request: Request, urban_object_id: int = Path(..., description="urban object identifier", gt=0)
) -> OkResponse:
    """
    ## Delete an urban object by its identifier.

    ### Parameters:
    - **urban_object_id** (int, Path): Unique identifier of the urban object.

    ### Returns:
    - **OkResponse**: A confirmation message of the deletion.

    ### Errors:
    - **404 Not Found**: If the urban object does not exist.
    """
    urban_objects_service: UrbanObjectsService = request.state.urban_objects_service

    await urban_objects_service.delete_urban_object_by_id(urban_object_id)

    return OkResponse()


@urban_objects_router.get(
    "/urban_objects_by_territory_id",
    response_model=list[UrbanObject],
    status_code=status.HTTP_200_OK,
)
async def get_urban_objects_by_territory_id(
    request: Request,
    territory_id: int = Query(..., description="parent territory identifier", gt=0),
    service_type_id: int | None = Query(None, description="service type identifier", gt=0),
    physical_object_type_id: int | None = Query(None, description="physical object type identifier", gt=0),
) -> list[UrbanObject]:
    """
    ## Get a list of urban objects by territory identifier.

    ### Parameters:
    - **territory_id** (int, Query): Unique identifier of the territory.
    - **service_type_id** (int | None, Query): Optional filter by service type identifier.
    - **physical_object_type_id** (int | None, Query): Optional filter by physical object type identifier.

    ### Returns:
    - **list[UrbanObject]**: A list of urban objects associated with the specified territory.

    ### Errors:
    - **404 Not Found**: If the territory does not exist.
    """
    urban_objects_service: UrbanObjectsService = request.state.urban_objects_service

    urban_objects = await urban_objects_service.get_urban_objects_by_territory_id(
        territory_id, service_type_id, physical_object_type_id
    )

    return [UrbanObject.from_dto(urban_object) for urban_object in urban_objects]


@urban_objects_router.patch(
    "/urban_objects/{urban_object_id}",
    response_model=UrbanObject,
    status_code=status.HTTP_200_OK,
)
async def patch_urban_object(
    request: Request,
    urban_object: UrbanObjectPatch,
    urban_object_id: int = Path(..., description="urban object identifier", gt=0),
) -> list[UrbanObject]:
    """
    ## Update specific fields of an urban object.

    ### Parameters:
    - **urban_object_id** (int, Path): Unique identifier of the urban object.

    ### Body:
    - **urban_object** (UrbanObjectPatch): The partial urban object data to update.

    ### Returns:
    - **UrbanObject**: The updated urban object (physical object + geometry + service).

    ### Errors:
    - **404 Not Found**: If the urban object does not exist.
    - **409 Conflict**: If an urban object with such `physical_object_id`, `object_geometry_id` and `service_id` already exists.
    """
    urban_objects_service: UrbanObjectsService = request.state.urban_objects_service

    urban_object_dto = await urban_objects_service.patch_urban_object_to_db(urban_object, urban_object_id)

    return UrbanObject.from_dto(urban_object_dto)
