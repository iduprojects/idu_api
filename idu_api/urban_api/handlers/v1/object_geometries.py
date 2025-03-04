"""Object geometries handlers are defined here."""

from fastapi import Body, HTTPException, Path, Query, Request
from starlette import status

from idu_api.urban_api.logic.object_geometries import ObjectGeometriesService
from idu_api.urban_api.schemas import (
    ObjectGeometry,
    ObjectGeometryPatch,
    ObjectGeometryPost,
    ObjectGeometryPut,
    OkResponse,
    PhysicalObject,
    UrbanObject,
)

from .routers import object_geometries_router


@object_geometries_router.get(
    "/object_geometries",
    response_model=list[ObjectGeometry],
    status_code=status.HTTP_200_OK,
)
async def get_object_geometries_by_ids(
    request: Request, object_geometries_ids: str = Query(..., description="list of identifiers separated by comma")
) -> list[ObjectGeometry]:
    """
    ## Get a list of object geometries by their identifiers.

    ### Parameters:
    - **object_geometries_ids** (str, Query): Comma-separated list of object geometry identifiers.

    ### Returns:
    - **list[ObjectGeometry]**: A list of object geometries matching the given identifiers.

    ### Errors:
    - **400 Bad Request**: If the provided identifiers are invalid.
    - **404 Not Found**: If at least one object geometry was not found.
    """
    object_geometries_service: ObjectGeometriesService = request.state.object_geometries_service

    try:
        object_geometries_ids = {int(geometry.strip()) for geometry in object_geometries_ids.split(",")}
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    object_geometries = await object_geometries_service.get_object_geometry_by_ids(object_geometries_ids)

    return [ObjectGeometry.from_dto(object_geometry) for object_geometry in object_geometries]


@object_geometries_router.put(
    "/object_geometries/{object_geometry_id}",
    response_model=ObjectGeometry,
    status_code=status.HTTP_200_OK,
    deprecated=True,
)
async def put_object_geometry(
    request: Request,
    object_geometry: ObjectGeometryPut,
    object_geometry_id: int = Path(..., description="object geometry identifier", gt=0),
) -> ObjectGeometry:
    """
    ## Update an object geometry by replacing all attributes.

    **WARNING:** This method has been deprecated since version 0.34.0 and will be removed in version 1.0.
    Instead, use PATCH method.

    ### Parameters:
    - **object_geometry_id** (int, Path): Unique identifier of the object geometry.
    - **object_geometry** (ObjectGeometryPut, Body): New data for the object geometry.

    ### Returns:
    - **ObjectGeometry**: The updated object geometry.

    ### Errors:
    - **404 Not Found**: If the object geometry does not exist.
    """
    object_geometries_service: ObjectGeometriesService = request.state.object_geometries_service

    object_geometry_dto = await object_geometries_service.put_object_geometry(object_geometry, object_geometry_id)

    return ObjectGeometry.from_dto(object_geometry_dto)


@object_geometries_router.patch(
    "/object_geometries/{object_geometry_id}",
    response_model=ObjectGeometry,
    status_code=status.HTTP_200_OK,
)
async def patch_object_geometry(
    request: Request,
    object_geometry: ObjectGeometryPatch,
    object_geometry_id: int = Path(..., description="object geometry identifier", gt=0),
) -> ObjectGeometry:
    """
    ## Partially update an object geometry.

    ### Parameters:
    - **object_geometry_id** (int, Path): Unique identifier of the object geometry.
    - **object_geometry** (ObjectGeometryPatch, Body): Fields to update in the object geometry.

    ### Returns:
    - **ObjectGeometry**: The updated object geometry with modified attributes.

    ### Errors:
    - **404 Not Found**: If the object geometry (or related entity) does not exist.
    """
    object_geometries_service: ObjectGeometriesService = request.state.object_geometries_service

    object_geometry_dto = await object_geometries_service.patch_object_geometry(object_geometry, object_geometry_id)

    return ObjectGeometry.from_dto(object_geometry_dto)


@object_geometries_router.delete(
    "/object_geometries/{object_geometry_id}",
    response_model=OkResponse,
    status_code=status.HTTP_200_OK,
)
async def delete_object_geometry(
    request: Request,
    object_geometry_id: int = Path(..., description="object geometry identifier", gt=0),
) -> OkResponse:
    """
    ## Delete an object geometry by its identifier.

    ### Parameters:
    - **object_geometry_id** (int, Path): Unique identifier of the object geometry.

    ### Returns:
    - **OkResponse**: A confirmation message of the deletion.

    ### Errors:
    - **404 Not Found**: If the object geometry (or related entity) does not exist.
    """
    object_geometries_service: ObjectGeometriesService = request.state.object_geometries_service

    await object_geometries_service.delete_object_geometry(object_geometry_id)

    return OkResponse()


@object_geometries_router.post(
    "/object_geometries/{physical_object_id}",
    response_model=UrbanObject,
    status_code=status.HTTP_201_CREATED,
)
async def add_object_geometry_to_physical_object(
    request: Request,
    physical_object_id: int = Path(..., description="Physical object id"),
    object_geometry: ObjectGeometryPost = Body(..., description="Object Geometry"),
) -> UrbanObject:
    """
    ## Add an object geometry to a physical object.

    ### Parameters:
    - **physical_object_id** (int, Path): Unique identifier of the physical object.
    - **object_geometry** (ObjectGeometryPost, Body): Data for the new object geometry.

    ### Returns:
    - **UrbanObject**: The created urban object (existing physical object + new geometry).

    ### Errors:
    - **404 Not Found**: If the physical object (or related entity) does not exist.
    """
    object_geometries_service: ObjectGeometriesService = request.state.object_geometries_service

    urban_object = await object_geometries_service.add_object_geometry_to_physical_object(
        physical_object_id, object_geometry
    )

    return UrbanObject.from_dto(urban_object)


@object_geometries_router.get(
    "/object_geometries/{object_geometry_id}/physical_objects",
    response_model=list[PhysicalObject],
    status_code=status.HTTP_200_OK,
)
async def get_physical_objects_by_geometry_id(
    request: Request,
    object_geometry_id: int = Path(..., description="object geometry identifier", gt=0),
) -> list[PhysicalObject]:
    """
    ## Get physical objects associated with a specific object geometry.

    ### Parameters:
    - **object_geometry_id** (int, Path): Unique identifier of the object geometry.

    ### Returns:
    - **list[PhysicalObject]**: A list of physical objects associated with the given geometry.

    ### Errors:
    - **404 Not Found**: If the object geometry does not exist.
    """
    object_geometries_service: ObjectGeometriesService = request.state.object_geometries_service

    physical_objects = await object_geometries_service.get_physical_objects_by_object_geometry_id(object_geometry_id)

    return [PhysicalObject.from_dto(physical_object) for physical_object in physical_objects]
