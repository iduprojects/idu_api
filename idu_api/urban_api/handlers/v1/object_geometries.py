"""Object geometries handlers are defined here."""

from fastapi import Body, Path, Query, Request
from starlette import status

from idu_api.urban_api.logic.object_geometries import ObjectGeometriesService
from idu_api.urban_api.schemas import (
    ObjectGeometries,
    ObjectGeometriesPatch,
    ObjectGeometriesPost,
    ObjectGeometriesPut,
    PhysicalObjectsData,
    UrbanObject,
)

from .routers import object_geometries_router


@object_geometries_router.get(
    "/object_geometries",
    response_model=list[ObjectGeometries],
    status_code=status.HTTP_200_OK,
)
async def get_object_geometries_by_ids(
    request: Request, object_geometries_ids: str = Query(..., description="list of identifiers separated by comma")
) -> list[ObjectGeometries]:
    """Get list of object geometries data."""
    object_geometries_service: ObjectGeometriesService = request.state.object_geometries_service

    object_geometries_ids = [int(geometry.strip()) for geometry in object_geometries_ids.split(",")]

    object_geometries = await object_geometries_service.get_object_geometry_by_ids(object_geometries_ids)

    return [ObjectGeometries.from_dto(object_geometry) for object_geometry in object_geometries]


@object_geometries_router.put(
    "/object_geometries/{object_geometry_id}",
    response_model=ObjectGeometries,
    status_code=status.HTTP_200_OK,
)
async def put_object_geometry(
    request: Request,
    object_geometry: ObjectGeometriesPut,
    object_geometry_id: int = Path(..., description="Object geometry id"),
) -> ObjectGeometries:
    """Update object geometry - all attributes."""
    object_geometries_service: ObjectGeometriesService = request.state.object_geometries_service

    object_geometry_dto = await object_geometries_service.put_object_geometry(object_geometry, object_geometry_id)

    return ObjectGeometries.from_dto(object_geometry_dto)


@object_geometries_router.patch(
    "/object_geometries/{object_geometry_id}",
    response_model=ObjectGeometries,
    status_code=status.HTTP_200_OK,
)
async def patch_object_geometry(
    request: Request,
    object_geometry: ObjectGeometriesPatch,
    object_geometry_id: int = Path(..., description="Object geometry id"),
) -> ObjectGeometries:
    """Update object geometry - only given attributes."""
    object_geometries_service: ObjectGeometriesService = request.state.object_geometries_service

    object_geometry_dto = await object_geometries_service.patch_object_geometry(object_geometry, object_geometry_id)

    return ObjectGeometries.from_dto(object_geometry_dto)


@object_geometries_router.delete(
    "/object_geometries/{object_geometry_id}",
    response_model=dict,
    status_code=status.HTTP_200_OK,
)
async def delete_object_geometry(
    request: Request,
    object_geometry_id: int = Path(..., description="Object geometry id"),
) -> dict:
    """Delete object geometry by given id."""
    object_geometries_service: ObjectGeometriesService = request.state.object_geometries_service

    return await object_geometries_service.delete_object_geometry(object_geometry_id)


@object_geometries_router.post(
    "/object_geometries/{physical_object_id}",
    response_model=UrbanObject,
    status_code=status.HTTP_200_OK,
)
async def add_object_geometry_to_physical_object(
    request: Request,
    physical_object_id: int = Path(..., description="Physical object id"),
    object_geometry: ObjectGeometriesPost = Body(..., description="Object Geometry"),
) -> UrbanObject:
    """Add object geometry to physical object"""
    object_geometries_service: ObjectGeometriesService = request.state.object_geometries_service

    urban_object = await object_geometries_service.add_object_geometry_to_physical_object(
        physical_object_id, object_geometry
    )

    return UrbanObject.from_dto(urban_object)


@object_geometries_router.get(
    "/object_geometries/{object_geometry_id}/physical_objects",
    response_model=list[PhysicalObjectsData],
    status_code=status.HTTP_200_OK,
)
async def get_physical_objects_by_geometry_id(
    request: Request,
    object_geometry_id: int = Path(..., description="Object geometry id"),
) -> list[PhysicalObjectsData]:
    """Get physical objects for the given object geometry identifier."""
    object_geometries_service: ObjectGeometriesService = request.state.object_geometries_service

    physical_objects = await object_geometries_service.get_physical_objects_by_object_geometry_id(object_geometry_id)

    return [PhysicalObjectsData.from_dto(physical_object) for physical_object in physical_objects]
