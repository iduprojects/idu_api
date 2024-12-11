"""Physical object handlers are defined here."""

from fastapi import Body, Path, Query, Request
from starlette import status

from idu_api.urban_api.logic.physical_objects import PhysicalObjectsService
from idu_api.urban_api.schemas import (
    LivingBuildingsData,
    LivingBuildingsDataPatch,
    LivingBuildingsDataPost,
    LivingBuildingsDataPut,
    ObjectGeometries,
    PhysicalObjectsData,
    PhysicalObjectsDataPatch,
    PhysicalObjectsDataPost,
    PhysicalObjectsDataPut,
    PhysicalObjectsWithTerritory,
    PhysicalObjectWithGeometryPost,
    ServicesData,
    ServicesDataWithGeometry,
    UrbanObject,
)
from idu_api.urban_api.schemas.geometries import AllPossibleGeometry
from idu_api.urban_api.schemas.physical_objects import PhysicalObjectWithGeometry

from .routers import physical_objects_router


@physical_objects_router.post(
    "/physical_objects",
    response_model=UrbanObject,
    status_code=status.HTTP_201_CREATED,
)
async def add_physical_object_with_geometry(
    request: Request, physical_object: PhysicalObjectWithGeometryPost
) -> UrbanObject:
    """Add a physical object with geometry."""
    physical_objects_service: PhysicalObjectsService = request.state.physical_objects_service

    urban_object = await physical_objects_service.add_physical_object_with_geometry(physical_object)

    return UrbanObject.from_dto(urban_object)


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
    physical_objects_service: PhysicalObjectsService = request.state.physical_objects_service

    physical_object_dto = await physical_objects_service.put_physical_object(physical_object, physical_object_id)

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
    physical_objects_service: PhysicalObjectsService = request.state.physical_objects_service

    physical_object_dto = await physical_objects_service.patch_physical_object(physical_object, physical_object_id)

    return PhysicalObjectsData.from_dto(physical_object_dto)


@physical_objects_router.delete(
    "/physical_objects/{physical_object_id}",
    response_model=dict,
    status_code=status.HTTP_200_OK,
)
async def delete_physical_object(
    request: Request,
    physical_object_id: int = Path(..., description="Physical object id"),
) -> dict:
    """Delete physical object by given id."""
    physical_objects_service: PhysicalObjectsService = request.state.physical_objects_service

    return await physical_objects_service.delete_physical_object(physical_object_id)


@physical_objects_router.post(
    "/living_buildings",
    response_model=LivingBuildingsData,
    status_code=status.HTTP_201_CREATED,
)
async def add_living_building(request: Request, living_building: LivingBuildingsDataPost) -> LivingBuildingsData:
    """Add new living building"""
    physical_objects_service: PhysicalObjectsService = request.state.physical_objects_service

    living_building_dto = await physical_objects_service.add_living_building(living_building)

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
    physical_objects_service: PhysicalObjectsService = request.state.physical_objects_service

    living_building_dto = await physical_objects_service.put_living_building(living_building, living_building_id)

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
    physical_objects_service: PhysicalObjectsService = request.state.physical_objects_service

    living_building_dto = await physical_objects_service.patch_living_building(living_building, living_building_id)

    return LivingBuildingsData.from_dto(living_building_dto)


@physical_objects_router.delete(
    "/living_buildings/{living_building_id}",
    response_model=dict,
    status_code=status.HTTP_200_OK,
)
async def delete_living_building(
    request: Request,
    living_building_id: int = Path(..., description="Living building id"),
) -> dict:
    """Delete living building by given id."""
    physical_objects_service: PhysicalObjectsService = request.state.physical_objects_service

    return await physical_objects_service.delete_living_building(living_building_id)


@physical_objects_router.get(
    "/physical_objects/{physical_object_id}/living_buildings",
    response_model=list[LivingBuildingsData],
    status_code=status.HTTP_200_OK,
)
async def get_living_buildings_by_physical_object_id(
    request: Request,
    physical_object_id: int = Path(..., description="Physical object id"),
) -> list[LivingBuildingsData]:
    """Get all living buildings inside a given physical object."""
    physical_objects_service: PhysicalObjectsService = request.state.physical_objects_service

    buildings = await physical_objects_service.get_living_buildings_by_physical_object_id(physical_object_id)

    return [LivingBuildingsData.from_dto(building) for building in buildings]


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
    physical_objects_service: PhysicalObjectsService = request.state.physical_objects_service

    services = await physical_objects_service.get_services_by_physical_object_id(
        physical_object_id, service_type_id, territory_type_id
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
    physical_objects_service: PhysicalObjectsService = request.state.physical_objects_service

    services = await physical_objects_service.get_services_with_geometry_by_physical_object_id(
        physical_object_id, service_type_id, territory_type_id
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
    physical_objects_service: PhysicalObjectsService = request.state.physical_objects_service

    geometries = await physical_objects_service.get_physical_object_geometries(physical_object_id)

    return [ObjectGeometries.from_dto(geometry) for geometry in geometries]


@physical_objects_router.post(
    "/physical_objects/around",
    response_model=list[PhysicalObjectWithGeometry],
    status_code=status.HTTP_200_OK,
)
async def get_physical_objects_around_geometry(
    request: Request,
    geometry: AllPossibleGeometry,
    physical_object_type_id: int | None = Query(None, description="Physical object type id", gt=0),
) -> list[PhysicalObjectWithGeometry]:
    """Get physical_objects for territory.

    physical_object_type could be specified in parameters.
    """
    physical_objects_service: PhysicalObjectsService = request.state.physical_objects_service

    physical_objects_with_geometry_dto = await physical_objects_service.get_physical_objects_around(
        geometry.as_shapely_geometry(), physical_object_type_id, 50
    )
    return [PhysicalObjectWithGeometry.from_dto(obj) for obj in physical_objects_with_geometry_dto]


@physical_objects_router.post(
    "/physical_objects/{object_geometry_id}",
    response_model=UrbanObject,
    status_code=status.HTTP_200_OK,
)
async def add_physical_object_to_object_geometry(
    request: Request,
    object_geometry_id: int = Path(..., description="Object geometry id"),
    physical_object: PhysicalObjectsDataPost = Body(..., description="Physical object"),
) -> UrbanObject:
    """Add physical object to object geometry"""
    physical_objects_service: PhysicalObjectsService = request.state.physical_objects_service

    urban_object = await physical_objects_service.add_physical_object_to_object_geometry(
        object_geometry_id, physical_object
    )

    return UrbanObject.from_dto(urban_object)


@physical_objects_router.get(
    "/physical_object/{physical_object_id}",
    response_model=PhysicalObjectsWithTerritory,
    status_code=status.HTTP_200_OK,
)
async def get_physical_object_by_id_with_territory(
    request: Request,
    physical_object_id: int = Path(..., description="Physical object id", gt=0),
) -> PhysicalObjectsWithTerritory:
    """Get physical object by id with parent territory"""
    physical_objects_service: PhysicalObjectsService = request.state.physical_objects_service

    physical_object = await physical_objects_service.get_physical_object_with_territories_by_id(physical_object_id)

    return PhysicalObjectsWithTerritory.from_dto(physical_object)
