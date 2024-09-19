from typing import Annotated, Dict

from idu_api.city_api import app
from idu_api.city_api.common.feature import Feature
from idu_api.city_api.dto.physical_objects import PhysicalObjectsDTO
from idu_api.city_api.dto.services import CityServiceDTO
from idu_api.city_api.dto.services_count import ServiceCountDTO
from idu_api.city_api.schemas.service_types import ServiceTypesData
from idu_api.city_api.schemas.territory import CATerritoriesData, CATerritoriesWithoutGeometryData
from idu_api.city_api.schemas.territory_hierarchy import TerritoryHierarchyData
from idu_api.city_api.services.objects.physical_objects import PhysicalObjectsService
from idu_api.city_api.services.objects.services import ServicesService
from idu_api.city_api.services.territories.blocks import BlocksService
from idu_api.city_api.services.territories.cities import CitiesService
from idu_api.urban_api.dto import TerritoryDTO
from idu_api.urban_api.schemas.geometries import Geometry
from fastapi import Request, Path, Query

tag = ["city-controller"]


@app.get("/city/{city}/city_service_types", tags=tag)
async def get_city_service_types_by_city_id(
        request: Request,
        city: Annotated[int | str, Path(description="city id or name")],
) -> list[ServiceTypesData]:
    services_service: ServicesService = request.state.services_service
    result: list[ServiceCountDTO] = await services_service.get_services_types_by_territory_id(city, city)
    return [await ServiceTypesData.from_dto(service) for service in result]


@app.get("/city/{city}/city_service_types/counts", tags=tag)
async def get_city_service_types_count_by_city_id(
        request: Request,
        city: Annotated[int | str, Path(description="city id or name")],
) -> Dict[str, int]:
    services_service: ServicesService = request.state.services_service
    result: list[ServiceCountDTO] = await services_service.get_services_types_by_territory_id(city, city)
    counts = {}
    for service in result:
        counts[service.name] = service.count
    return counts


@app.get("/city/{city}/geometry", tags=tag)
async def get_city_geometry(
        request: Request,
        city: Annotated[int | str, Path(description="city id or name")]
) -> Geometry:
    geometries_service: CitiesService = request.state.cities_service
    result: TerritoryDTO = await geometries_service.get_city_by_id(city)
    return Geometry.from_shapely_geometry(result.geometry)


@app.get("/city/{city}/level", tags=tag)
async def get_territories_by_city_id_level_and_type(
        request: Request,
        city: Annotated[int | str, Path(description="city id or name")],
        level: Annotated[int, Query(ge=0, description="level of required territories")],
        type: Annotated[int, Query(ge=0, description="type of required territories")],
        no_geometry: Annotated[bool, Query(description="get only centers")] = False
) -> list[CATerritoriesData | CATerritoriesWithoutGeometryData]:
    cities_service: CitiesService = request.state.cities_service
    if not no_geometry:
        return [
            await CATerritoriesData.from_dto(territory)
            for territory in await cities_service.get_territories_by_city_id_level_and_type(
                city, level, type, no_geometry=no_geometry
            )
        ]
    else:
        return [
            await CATerritoriesWithoutGeometryData.from_dto(territory)
            for territory in await cities_service.get_territories_by_city_id_level_and_type(
                city, level, type, no_geometry=no_geometry
            )
        ]


@app.get("/city/{city}/type-hierarchy", tags=tag)
async def get_territory_hierarchy_by_city_id(
        request: Request,
        city: Annotated[int | str, Path(description="city id or name")],
) -> list[TerritoryHierarchyData]:
    cities_service: CitiesService = request.state.cities_service
    return [
        await TerritoryHierarchyData.from_dto(territory)
        for territory in (await cities_service.get_hierarchy_by_city_id(city))
    ]


@app.get("/city/{city}/blocks", tags=tag)
async def get_city_blocks_by_id(
        request: Request,
        city: Annotated[int | str, Path(description="city id or name")],
        no_geometry: Annotated[bool, Query(description="only centers")] = False
) -> list[CATerritoriesData | CATerritoriesWithoutGeometryData]:
    blocks_service: BlocksService = request.state.blocks_service

    if not no_geometry:
        return [
            await CATerritoriesData.from_dto(block)
            for block in await blocks_service.get_blocks_by_territory_id(city, no_geometry=no_geometry)
        ]
    else:
        return [
            await CATerritoriesWithoutGeometryData.from_dto(block)
            for block in await blocks_service.get_blocks_by_territory_id(city, no_geometry=no_geometry)
        ]


@app.get("/city/{city}/territories", tags=tag)
async def get_city_territories_hierarchy(
        request: Request,
        city: Annotated[int | str, Path(description="city id")],
        no_geometry: Annotated[bool, Query(description="only centers")] = False
) -> dict:
    cities_service: CitiesService = request.state.cities_service
    return await cities_service.get_city_hierarchy(city, no_geometry)



@app.get("/city/{city}/physical_objects", tags=tag)
async def get_physical_objects_by_city_id(
        request: Request,
        city: Annotated[int | str, Path(description="city id")],
):
    physical_objects_service: PhysicalObjectsService = request.state.physical_objects_service
    result: list[PhysicalObjectsDTO] = await physical_objects_service.get_physical_objects_by_territory_id(
        city,
        city
    )
    return await Feature.generate_feature_collection(
        result,
        "geometry",
        "centre_point",
        {},
        ["geometry", "centre_point"]
    )


@app.get("/city/{city}/services", tags=tag)
async def get_services_by_city_id(
        request: Request,
        city: Annotated[int | str, Path(description="city id")],
):
    services_service: ServicesService = request.state.services_service
    result: list[CityServiceDTO] = await services_service.get_services_by_territory_id(city, city)
    return await Feature.generate_feature_collection(
        result,
        "geometry",
        "centre_point",
        {},
        ["geometry", "centre_point"]
    )
