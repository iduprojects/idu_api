"""
Physical object endpoints are defined here.
"""

from typing import Dict, List

from fastapi import Depends, Path, Query
from sqlalchemy.ext.asyncio import AsyncConnection
from starlette import status

from urban_api.db.connection import get_connection
from urban_api.logic.physical_objects import (
    add_living_building_to_db,
    add_physical_object_type_to_db,
    add_physical_object_with_geometry_to_db,
    get_physical_object_geometries_from_db,
    get_physical_object_types_from_db,
    get_services_by_physical_object_id_from_db,
    get_services_with_geometry_by_physical_object_id_from_db,
)
from urban_api.schemas import (
    LivingBuildingsData,
    LivingBuildingsDataPost,
    ObjectGeometries,
    PhysicalObjectsDataPost,
    PhysicalObjectsTypes,
    PhysicalObjectsTypesPost,
    ServicesData,
    ServicesDataWithGeometry,
)

from .routers import physical_objects_router


@physical_objects_router.get(
    "/physical_object_types",
    response_model=List[PhysicalObjectsTypes],
    status_code=status.HTTP_200_OK,
)
async def get_physical_object_types(
    connection: AsyncConnection = Depends(get_connection),
) -> List[PhysicalObjectsTypes]:
    """
    Summary:
        Get physical object types list

    Description:
        Get a list of all physical object types
    """

    physical_object_types = await get_physical_object_types_from_db(connection)

    return [PhysicalObjectsTypes.from_dto(object_type) for object_type in physical_object_types]


@physical_objects_router.post(
    "/physical_object_types",
    response_model=PhysicalObjectsTypes,
    status_code=status.HTTP_201_CREATED,
)
async def add_physical_object_type(
    physical_object_type: PhysicalObjectsTypesPost, connection: AsyncConnection = Depends(get_connection)
) -> PhysicalObjectsTypes:
    """
    Summary:
        Add physical object type

    Description:
        Add a physical object type
    """

    physical_object_type_dto = await add_physical_object_type_to_db(physical_object_type, connection)

    return PhysicalObjectsTypes.from_dto(physical_object_type_dto)


@physical_objects_router.post(
    "/physical_objects",
    status_code=status.HTTP_201_CREATED,
)
async def add_physical_object_with_geometry(
    physical_object: PhysicalObjectsDataPost, connection: AsyncConnection = Depends(get_connection)
) -> Dict[str, int]:
    """
    Summary:
        Add physical object with geometry

    Description:
        Add a physical object with geometry
    """

    return await add_physical_object_with_geometry_to_db(physical_object, connection)


@physical_objects_router.post(
    "/living_buildings",
    response_model=LivingBuildingsData,
    status_code=status.HTTP_201_CREATED,
)
async def add_living_building(
    living_building: LivingBuildingsDataPost, connection: AsyncConnection = Depends(get_connection)
) -> LivingBuildingsData:
    """
    Summary:
        Add living building

    Description:
        Add a living building
    """

    living_building_dto = await add_living_building_to_db(living_building, connection)

    return LivingBuildingsData.from_dto(living_building_dto)


@physical_objects_router.get(
    "/physical_objects/{physical_object_id}/services",
    response_model=List[ServicesData],
    status_code=status.HTTP_200_OK,
)
async def get_services_by_physical_object_id(
    physical_object_id: int = Path(..., description="Physical object id"),
    service_type_id: int = Query(None, description="To filter by service type"),
    territory_type_id: int = Query(None, description="To filter by territory type"),
    connection: AsyncConnection = Depends(get_connection),
) -> List[ServicesData]:
    """
    Summary:
        Get services list by physical object id

    Description:
        Get a list of all services by physical object id
    """

    services = await get_services_by_physical_object_id_from_db(
        physical_object_id, service_type_id, territory_type_id, connection
    )

    return [ServicesData.from_dto(service) for service in services]


@physical_objects_router.get(
    "/physical_objects/{physical_object_id}/services_with_geometry",
    response_model=List[ServicesDataWithGeometry],
    status_code=status.HTTP_200_OK,
)
async def get_services_with_geometry_by_physical_object_id(
    physical_object_id: int = Path(..., description="Physical object id"),
    service_type_id: int = Query(None, description="To filter by service type"),
    territory_type_id: int = Query(None, description="To filter by territory type"),
    connection: AsyncConnection = Depends(get_connection),
) -> List[ServicesDataWithGeometry]:
    """
    Summary:
        Get services with geometry list by physical object id

    Description:
        Get a list of all services with geometry by physical object id
    """

    services = await get_services_with_geometry_by_physical_object_id_from_db(
        physical_object_id, service_type_id, territory_type_id, connection
    )

    return [ServicesDataWithGeometry.from_dto(service) for service in services]


@physical_objects_router.get(
    "/physical_objects/{physical_object_id}/geometries",
    response_model=List[ObjectGeometries],
    status_code=status.HTTP_200_OK,
)
async def get_physical_object_geometries(
    physical_object_id: int = Path(..., description="Physical object id"),
    connection: AsyncConnection = Depends(get_connection),
) -> List[ObjectGeometries]:
    """
    Summary:
        Get geometries by physical object id

    Description:
        Get a list of all geometries by physical object id
    """

    geometries = await get_physical_object_geometries_from_db(physical_object_id, connection)

    return [ObjectGeometries.from_dto(geometry) for geometry in geometries]
