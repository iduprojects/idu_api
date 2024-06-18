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


@physical_objects_router.put(
    "/physical_objects/{physical_object_id}",
    response_model=PhysicalObjectsData,
    status_code=status.HTTP_200_OK,
)
async def put_physical_object(
    physical_object: PhysicalObjectsDataPut,
    physical_object_id: int = Path(..., description="Physical object id"),
    connection: AsyncConnection = Depends(get_connection),
) -> PhysicalObjectsData:
    """
    Summary:
        Put physical object

    Description:
        Put a physical object
    """

    physical_object_dto = await put_physical_object_to_db(physical_object, physical_object_id, connection)

    return PhysicalObjectsData.from_dto(physical_object_dto)


@physical_objects_router.patch(
    "/physical_objects/{physical_object_id}",
    response_model=PhysicalObjectsData,
    status_code=status.HTTP_200_OK,
)
async def patch_physical_object(
    physical_object: PhysicalObjectsDataPatch,
    physical_object_id: int = Path(..., description="Physical object id"),
    connection: AsyncConnection = Depends(get_connection),
) -> PhysicalObjectsData:
    """
    Summary:
        Patch physical object

    Description:
        Patch physical object
    """

    physical_object_dto = await patch_physical_object_to_db(physical_object, physical_object_id, connection)

    return PhysicalObjectsData.from_dto(physical_object_dto)


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


@physical_objects_router.put(
    "/living_buildings/{living_building_id}",
    response_model=LivingBuildingsData,
    status_code=status.HTTP_200_OK,
)
async def put_living_building(
    living_building: LivingBuildingsDataPut,
    living_building_id: int = Path(..., description="Living building id"),
    connection: AsyncConnection = Depends(get_connection),
) -> LivingBuildingsData:
    """
    Summary:
        Put living building

    Description:
        Put a living building
    """

    living_building_dto = await put_living_building_to_db(living_building, living_building_id, connection)

    return LivingBuildingsData.from_dto(living_building_dto)


@physical_objects_router.patch(
    "/living_buildings/{living_building_id}",
    response_model=LivingBuildingsData,
    status_code=status.HTTP_200_OK,
)
async def patch_living_building(
    living_building: LivingBuildingsDataPatch,
    living_building_id: int = Path(..., description="Living building id"),
    connection: AsyncConnection = Depends(get_connection),
) -> LivingBuildingsData:
    """
    Summary:
        Patch living building

    Description:
        Patch living building
    """

    living_building_dto = await patch_living_building_to_db(living_building, living_building_id, connection)

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
