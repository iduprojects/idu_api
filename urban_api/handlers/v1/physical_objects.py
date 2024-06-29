"""Physical object handlers are defined here."""

from fastapi import Path, Query, Request
from sqlalchemy.ext.asyncio import AsyncConnection
from starlette import status

from urban_api.logic.physical_objects import (
    PhysicalObjectsService,
    add_living_building_to_db,
    add_physical_object_type_to_db,
    add_physical_object_with_geometry_to_db,
    get_physical_object_geometries_from_db,
    get_physical_object_types_from_db,
    get_services_by_physical_object_id_from_db,
    get_services_with_geometry_by_physical_object_id_from_db,
    patch_living_building_to_db,
    patch_physical_object_to_db,
    put_living_building_to_db,
    put_physical_object_to_db,
)
from urban_api.schemas import (
    LivingBuildingsData,
    LivingBuildingsDataPatch,
    LivingBuildingsDataPost,
    LivingBuildingsDataPut,
    ObjectGeometries,
    PhysicalObjectsData,
    PhysicalObjectsDataPatch,
    PhysicalObjectsDataPost,
    PhysicalObjectsDataPut,
    PhysicalObjectsTypes,
    PhysicalObjectsTypesPost,
    ServicesData,
    ServicesDataWithGeometry,
)
from urban_api.schemas.geometries import Geometry
from urban_api.schemas.physical_objects import PhysicalObjectWithGeometry

from .routers import physical_objects_router


@physical_objects_router.get(
    "/physical_object_types",
    response_model=list[PhysicalObjectsTypes],
    status_code=status.HTTP_200_OK,
)
async def get_physical_object_types(request: Request) -> list[PhysicalObjectsTypes]:
    """Get all physical object types."""
    conn: AsyncConnection = request.state.conn

    physical_object_types = await get_physical_object_types_from_db(conn)

    return [PhysicalObjectsTypes.from_dto(object_type) for object_type in physical_object_types]


@physical_objects_router.post(
    "/physical_object_types",
    response_model=PhysicalObjectsTypes,
    status_code=status.HTTP_201_CREATED,
)
async def add_physical_object_type(
    request: Request, physical_object_type: PhysicalObjectsTypesPost
) -> PhysicalObjectsTypes:
    """Add a physical object type."""
    conn: AsyncConnection = request.state.conn

    physical_object_type_dto = await add_physical_object_type_to_db(conn, physical_object_type)

    return PhysicalObjectsTypes.from_dto(physical_object_type_dto)


@physical_objects_router.post(
    "/physical_objects",
    status_code=status.HTTP_201_CREATED,
)
async def add_physical_object_with_geometry(
    request: Request, physical_object: PhysicalObjectsDataPost
) -> dict[str, int]:
    """Add a physical object with geometry."""
    conn: AsyncConnection = request.state.conn

    return await add_physical_object_with_geometry_to_db(conn, physical_object)


@physical_objects_router.put(
    "/physical_objects/{physical_object_id}",
    response_model=PhysicalObjectsData,
    status_code=status.HTTP_200_OK,
)
async def put_physical_object(
    request: Request,
    physical_object: PhysicalObjectsDataPut,
    physical_object_id: int = Path(..., description="Physical object id"),
) -> PhysicalObjectsData:
    """Update physical object - all attributes."""
    conn: AsyncConnection = request.state.conn

    physical_object_dto = await put_physical_object_to_db(conn, physical_object, physical_object_id)

    return PhysicalObjectsData.from_dto(physical_object_dto)


@physical_objects_router.patch(
    "/physical_objects/{physical_object_id}",
    response_model=PhysicalObjectsData,
    status_code=status.HTTP_200_OK,
)
async def patch_physical_object(
    request: Request,
    physical_object: PhysicalObjectsDataPatch,
    physical_object_id: int = Path(..., description="Physical object id"),
) -> PhysicalObjectsData:
    """Update physical objects - only given fields."""
    conn: AsyncConnection = request.state.conn

    physical_object_dto = await patch_physical_object_to_db(conn, physical_object, physical_object_id)

    return PhysicalObjectsData.from_dto(physical_object_dto)


@physical_objects_router.post(
    "/living_buildings",
    response_model=LivingBuildingsData,
    status_code=status.HTTP_201_CREATED,
)
async def add_living_building(request: Request, living_building: LivingBuildingsDataPost) -> LivingBuildingsData:
    """Add new living building"""
    conn: AsyncConnection = request.state.conn

    living_building_dto = await add_living_building_to_db(conn, living_building)

    return LivingBuildingsData.from_dto(living_building_dto)


@physical_objects_router.put(
    "/living_buildings/{living_building_id}",
    response_model=LivingBuildingsData,
    status_code=status.HTTP_200_OK,
)
async def put_living_building(
    request: Request,
    living_building: LivingBuildingsDataPut,
    living_building_id: int = Path(..., description="Living building id"),
) -> LivingBuildingsData:
    """Update living building - all attributes."""
    conn: AsyncConnection = request.state.conn

    living_building_dto = await put_living_building_to_db(conn, living_building, living_building_id)

    return LivingBuildingsData.from_dto(living_building_dto)


@physical_objects_router.patch(
    "/living_buildings/{living_building_id}",
    response_model=LivingBuildingsData,
    status_code=status.HTTP_200_OK,
)
async def patch_living_building(
    request: Request,
    living_building: LivingBuildingsDataPatch,
    living_building_id: int = Path(..., description="Living building id"),
) -> LivingBuildingsData:
    """Update living buildings - only given attributes."""
    conn: AsyncConnection = request.state.conn

    living_building_dto = await patch_living_building_to_db(conn, living_building, living_building_id)

    return LivingBuildingsData.from_dto(living_building_dto)


@physical_objects_router.get(
    "/physical_objects/{physical_object_id}/services",
    response_model=list[ServicesData],
    status_code=status.HTTP_200_OK,
)
async def get_services_by_physical_object_id(
    request: Request,
    physical_object_id: int = Path(..., description="Physical object id"),
    service_type_id: int = Query(None, description="To filter by service type"),
    territory_type_id: int = Query(None, description="To filter by territory type"),
) -> list[ServicesData]:
    """Get all services inside a given physical object."""
    conn: AsyncConnection = request.state.conn

    services = await get_services_by_physical_object_id_from_db(
        conn, physical_object_id, service_type_id, territory_type_id
    )

    return [ServicesData.from_dto(service) for service in services]


@physical_objects_router.get(
    "/physical_objects/{physical_object_id}/services_with_geometry",
    response_model=list[ServicesDataWithGeometry],
    status_code=status.HTTP_200_OK,
)
async def get_services_with_geometry_by_physical_object_id(
    request: Request,
    physical_object_id: int = Path(..., description="Physical object id"),
    service_type_id: int = Query(None, description="To filter by service type"),
    territory_type_id: int = Query(None, description="To filter by territory type"),
) -> list[ServicesDataWithGeometry]:
    """Get all services without geometries inside a given physical object."""
    conn: AsyncConnection = request.state.conn

    services = await get_services_with_geometry_by_physical_object_id_from_db(
        conn, physical_object_id, service_type_id, territory_type_id
    )

    return [ServicesDataWithGeometry.from_dto(service) for service in services]


@physical_objects_router.get(
    "/physical_objects/{physical_object_id}/geometries",
    response_model=list[ObjectGeometries],
    status_code=status.HTTP_200_OK,
)
async def get_physical_object_geometries(
    request: Request,
    physical_object_id: int = Path(..., description="Physical object id"),
) -> list[ObjectGeometries]:
    """Get geometries for a given physical object."""
    conn: AsyncConnection = request.state.conn

    geometries = await get_physical_object_geometries_from_db(conn, physical_object_id)

    return [ObjectGeometries.from_dto(geometry) for geometry in geometries]


@physical_objects_router.post(
    "/physical_objects/around",
    response_model=list[PhysicalObjectWithGeometry],
    status_code=status.HTTP_200_OK,
)
async def get_physical_objects_around_geometry(
    request: Request,
    geometry: Geometry,
    physical_object_type_id: int | None = Query(None, description="Physical object type id", gt=0),
) -> list[PhysicalObjectWithGeometry]:
    """Get physical_objects for territory.

    physical_object_type could be specified in parameters.
    """
    physical_object_service: PhysicalObjectsService = request.state.physical_objects_service

    physical_objects_with_geometry_dto = await physical_object_service.get_physical_objects_around(
        geometry.as_shapely_geometry(), physical_object_type_id, 50
    )
    return [PhysicalObjectWithGeometry.from_dto(obj) for obj in physical_objects_with_geometry_dto]
