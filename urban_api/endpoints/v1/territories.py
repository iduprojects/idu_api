"""
Territory endpoints are defined here.
"""

from datetime import datetime
from typing import List, Optional

from fastapi import Depends, Path, Query
from sqlalchemy.ext.asyncio import AsyncConnection
from starlette import status

from urban_api.db.connection import get_connection
from urban_api.logic.territories import (
    add_territory_to_db,
    add_territory_type_to_db,
    get_functional_zones_by_territory_id_from_db,
    get_indicator_values_by_territory_id_from_db,
    get_indicators_by_territory_id_from_db,
    get_living_buildings_with_geometry_by_territory_id_from_db,
    get_physical_objects_by_territory_id_from_db,
    get_physical_objects_with_geometry_by_territory_id_from_db,
    get_services_by_territory_id_from_db,
    get_services_capacity_by_territory_id_from_db,
    get_services_with_geometry_by_territory_id_from_db,
    get_territories_by_parent_id_from_db,
    get_territories_without_geometry_by_parent_id_from_db,
    get_territory_by_id_from_db,
    get_territory_types_from_db,
)
from urban_api.schemas import (
    FunctionalZoneData,
    Indicators,
    IndicatorValue,
    LivingBuildingsWithGeometry,
    PhysicalObjectsData,
    PhysicalObjectWithGeometry,
    ServicesData,
    ServicesDataWithGeometry,
    TerritoriesData,
    TerritoriesDataPost,
    TerritoryTypes,
    TerritoryTypesPost,
    TerritoryWithoutGeometry,
)

from .routers import territories_router


@territories_router.get(
    "/territory_types",
    response_model=List[TerritoryTypes],
    status_code=status.HTTP_200_OK,
)
async def get_territory_types(connection: AsyncConnection = Depends(get_connection)) -> List[TerritoryTypes]:
    """
    Summary:
        Get territory types list

    Description:
        Get a list of all territory types
    """

    territory_types = await get_territory_types_from_db(connection)

    return [TerritoryTypes.from_dto(territory_type) for territory_type in territory_types]


@territories_router.post(
    "/territory_types",
    response_model=TerritoryTypes,
    status_code=status.HTTP_201_CREATED,
)
async def add_territory_type(
    territory_type: TerritoryTypesPost, connection: AsyncConnection = Depends(get_connection)
) -> TerritoryTypes:
    """
    Summary:
        Add territory type

    Description:
        Add a territory type
    """

    territory_type_dto = await add_territory_type_to_db(territory_type, connection)

    return TerritoryTypes.from_dto(territory_type_dto)


@territories_router.get(
    "/territory",
    response_model=TerritoriesData,
    status_code=status.HTTP_200_OK,
)
async def get_territory_by_id(
    territory_id: int = Query(..., description="territory id", gt=0),
    connection: AsyncConnection = Depends(get_connection),
) -> TerritoriesData:
    """
    Summary:
        Get single territory

    Description:
        Get a territory by id
    """

    territory = await get_territory_by_id_from_db(territory_id, connection)

    return TerritoriesData.from_dto(territory)


@territories_router.post(
    "/territory",
    response_model=TerritoriesData,
    status_code=status.HTTP_201_CREATED,
)
async def add_territory(
    territory: TerritoriesDataPost, connection: AsyncConnection = Depends(get_connection)
) -> TerritoriesData:
    """
    Summary:
        Add territory type

    Description:
        Add a territory type
    """

    territory_dto = await add_territory_to_db(territory, connection)

    return TerritoriesData.from_dto(territory_dto)


@territories_router.get(
    "/territory/{id}/services",
    response_model=List[ServicesData],
    status_code=status.HTTP_200_OK,
)
async def get_services_by_territory_id(
    territory_id: int = Path(..., description="territory id", gt=0),
    service_type: Optional[int] = Query(None, description="Service type id", gt=0),
    connection: AsyncConnection = Depends(get_connection),
) -> list[ServicesData]:
    """
    Summary:
        Get services for territory

    Description:
        Get services for territory by id, service type could be specified in parameters
    """

    services = await get_services_by_territory_id_from_db(territory_id, connection, service_type_id=service_type)

    return [ServicesData.from_dto(service) for service in services]


@territories_router.get(
    "/territory/{id}/services_with_geometry",
    response_model=List[ServicesDataWithGeometry],
    status_code=status.HTTP_200_OK,
)
async def get_services_with_geometry_by_territory_id(
    territory_id: int = Path(..., description="territory id", gt=0),
    service_type: Optional[int] = Query(None, description="Service type id", gt=0),
    connection: AsyncConnection = Depends(get_connection),
) -> List[ServicesDataWithGeometry]:
    """
    Summary:
        Get services for territory

    Description:
        Get services for territory by id, service type could be specified in parameters
    """

    services = await get_services_with_geometry_by_territory_id_from_db(
        territory_id, connection, service_type_id=service_type
    )

    return [ServicesDataWithGeometry.from_dto(service) for service in services]


@territories_router.get(
    "/territory/{id}/services_capacity",
    response_model=int,
    status_code=status.HTTP_200_OK,
)
async def get_services_with_geometry_by_territory_idu(
    territory_id: int = Path(..., description="territory id", gt=0),
    service_type: Optional[int] = Query(None, description="Service type id", gt=0),
    connection: AsyncConnection = Depends(get_connection),
) -> int:
    """
    Summary:
        Get services capacity for territory

    Description:
        Get aggregated capacity of services for territory
    """

    capacity = await get_services_capacity_by_territory_id_from_db(
        territory_id, connection, service_type_id=service_type
    )

    return capacity


@territories_router.get(
    "/territory/{id}/indicators",
    response_model=List[Indicators],
    status_code=status.HTTP_200_OK,
)
async def get_indicators_by_territory_id(
    territory_id: int = Path(..., description="territory id", gt=0),
    connection: AsyncConnection = Depends(get_connection),
) -> List[Indicators]:
    """
    Summary:
        Get indicators for territory

    Description:
        Get indicators for territory by id
    """

    indicators = await get_indicators_by_territory_id_from_db(territory_id, connection)

    return [Indicators.from_dto(indicator) for indicator in indicators]


@territories_router.get(
    "/territory/{id}/indicators_values",
    response_model=List[IndicatorValue],
    status_code=status.HTTP_200_OK,
)
async def get_indicator_values_by_territory_id(
    territory_id: int = Path(..., description="territory id", gt=0),
    date_type: Optional[str] = Query(None, description="Date type"),
    date_value: Optional[datetime] = Query(None, description="Time value"),
    connection: AsyncConnection = Depends(get_connection),
) -> List[IndicatorValue]:
    """
    Summary:
        Get indicators values for territory

    Description:
        Get indicators values for territory by id, time period could be specified in parameters
    """

    indicator_values = await get_indicator_values_by_territory_id_from_db(
        territory_id, connection, date_type, date_value
    )

    return [IndicatorValue.from_dto(value) for value in indicator_values]


@territories_router.get(
    "/territory/{id}/physical_objects",
    response_model=List[PhysicalObjectsData],
    status_code=status.HTTP_200_OK,
)
async def get_physical_objects_by_territory_id(
    territory_id: int = Path(..., description="territory id", gt=0),
    physical_object_type: Optional[int] = Query(None, description="Physical object type id", gt=0),
    connection: AsyncConnection = Depends(get_connection),
) -> List[PhysicalObjectsData]:
    """
    Summary:
        Get physical_objects for territory

    Description:
        Get physical_objects for territory, physical_object_type could be specified in parameters
    """

    physical_objects = await get_physical_objects_by_territory_id_from_db(
        territory_id, connection, physical_object_type
    )

    return [PhysicalObjectsData.from_dto(physical_object) for physical_object in physical_objects]


@territories_router.get(
    "/territory/{id}/physical_objects_with_geometry",
    response_model=List[PhysicalObjectWithGeometry],
    status_code=status.HTTP_200_OK,
)
async def get_physical_objects_with_geometry_by_territory_id(
    territory_id: int = Path(..., description="territory id", gt=0),
    physical_object_type: Optional[int] = Query(None, description="Physical object type id", gt=0),
    connection: AsyncConnection = Depends(get_connection),
) -> List[PhysicalObjectWithGeometry]:
    """
    Summary:
        Get physical_objects with geometry for territory

    Description:
        Get physical_objects for territory, physical_object_type could be specified in parameters
    """
    physical_objects_with_geometry_dto = await get_physical_objects_with_geometry_by_territory_id_from_db(
        territory_id, connection, physical_object_type
    )

    return [PhysicalObjectWithGeometry.from_dto(obj) for obj in physical_objects_with_geometry_dto]


@territories_router.get(
    "/territory/{id}/living_buildings_with_geometry",
    response_model=List[LivingBuildingsWithGeometry],
    status_code=status.HTTP_200_OK,
)
async def get_living_buildings_with_geometry_by_territory_id(
    territory_id: int = Path(..., description="territory id", gt=0),
    connection: AsyncConnection = Depends(get_connection),
) -> List[LivingBuildingsWithGeometry]:
    """
    Summary:
        Get living buildings with geometry for territory

    Description:
        Get living buildings for territory
    """

    buildings = await get_living_buildings_with_geometry_by_territory_id_from_db(territory_id, connection)

    return [LivingBuildingsWithGeometry.from_dto(building) for building in buildings]


@territories_router.get(
    "/territory/{id}/functional_zones",
    response_model=List[FunctionalZoneData],
    status_code=status.HTTP_200_OK,
)
async def get_functional_zones_for_territory(
    territory_id: int = Path(..., description="territory id", gt=0),
    functional_zone_type_id: Optional[int] = Query(None, description="functional_zone_type_id", gt=0),
    connection: AsyncConnection = Depends(get_connection),
) -> List[FunctionalZoneData]:
    """
    Summary:
        Get functional zones for territory

    Description:
        Get functional zones for territory, functional_zone_type could be specified in parameters
    """

    zones = await get_functional_zones_by_territory_id_from_db(territory_id, connection, functional_zone_type_id)

    return [FunctionalZoneData.from_dto(zone) for zone in zones]


@territories_router.get(
    "/territories",
    response_model=List[TerritoriesData],
    status_code=status.HTTP_200_OK,
)
async def get_territory_by_parent_id(
    parent_id: int = Query(..., description="Parent territory id to filter, should be 0 for top level territories"),
    get_all_levels: bool = Query(
        False, description="Getting full subtree of territories (unsafe for high level parents)"
    ),
    territory_type_id: Optional[int] = Query(None, description="Specifying territory type"),
    connection: AsyncConnection = Depends(get_connection),
) -> List[TerritoriesData]:
    """
    Summary:
        Get territories by parent id

    Description:
        Get a territory or list of territories by parent, territory type could be specified in parameters
    """

    territories = await get_territories_by_parent_id_from_db(parent_id, connection, get_all_levels, territory_type_id)

    return [TerritoriesData.from_dto(territory) for territory in territories]


@territories_router.get(
    "/territories_without_geometry",
    response_model=List[TerritoryWithoutGeometry],
    status_code=status.HTTP_200_OK,
)
async def get_territory_without_geometry_by_parent_id(
    parent_id: int = Query(..., description="Parent territory id to filter, should be 0 for top level territories"),
    get_all_levels: bool = Query(
        False, description="Getting full subtree of territories (unsafe for high level parents)"
    ),
    connection: AsyncConnection = Depends(get_connection),
) -> List[TerritoryWithoutGeometry]:
    """
    Summary:
        Get territories by parent id

    Description:
        Get a territory or list of territories without geometry by parent
    """

    territories = await get_territories_without_geometry_by_parent_id_from_db(parent_id, connection, get_all_levels)

    return [TerritoryWithoutGeometry.from_dto(territory) for territory in territories]
