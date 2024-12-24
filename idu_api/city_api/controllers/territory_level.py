from typing import Annotated

from fastapi import Path, Query
from fastapi.requests import Request

from idu_api.city_api.common.feature import Feature
from idu_api.city_api.dto.physical_objects import PhysicalObjectsDTO
from idu_api.city_api.dto.services import CityServiceDTO
from idu_api.city_api.main import app
from idu_api.city_api.schemas.territory import CATerritoriesData, CATerritoriesWithoutGeometryData
from idu_api.city_api.schemas.territory_hierarchy import TerritoryHierarchyData
from idu_api.city_api.services.objects.physical_objects import PhysicalObjectsService
from idu_api.city_api.services.objects.services import ServicesService
from idu_api.city_api.services.territories.blocks import BlocksService
from idu_api.city_api.services.territories.cities import CitiesService

tag = ["territory-level-controller"]


@app.get("/city/{city}/level/{territory}/level", tags=tag)
async def get_territories_by_city_id_level_and_type(
    request: Request,
    city: Annotated[int | str, Path(description="city id or name")],  # pylint: disable=unused-argument
    territory: Annotated[int | str, Path(description="territory id or name")],
    level: Annotated[int, Query(ge=0, description="level of required territories")],
    type: Annotated[int, Query(ge=0, description="type of required territories")],  # pylint: disable=redefined-builtin
    no_geometry: Annotated[bool, Query(description="get only centers")] = False,
) -> list[CATerritoriesData | CATerritoriesWithoutGeometryData]:
    cities_service: CitiesService = request.state.cities_service
    if not no_geometry:
        return [
            await CATerritoriesData.from_dto(territory_entity)
            for territory_entity in await cities_service.get_territories_by_city_id_level_and_type(
                territory, level, type, no_geometry=no_geometry
            )
        ]
    return [
        await CATerritoriesWithoutGeometryData.from_dto(territory_entity)
        for territory_entity in await cities_service.get_territories_by_city_id_level_and_type(
            territory, level, type, no_geometry=no_geometry
        )
    ]


@app.get("/city/{city}/level/{territory}/type-hierarchy", tags=tag)
async def get_type_hierarchy_by_territory_id(
    request: Request,
    city: Annotated[int | str, Path(description="city id or name")],  # pylint: disable=unused-argument
    territory: Annotated[int | str, Path(description="city id or name")],
) -> list[TerritoryHierarchyData]:
    cities_service: CitiesService = request.state.cities_service
    return [
        await TerritoryHierarchyData.from_dto(territory_entity)
        for territory_entity in (await cities_service.get_hierarchy_by_city_id(territory))
    ]


@app.get("/city/{city}/level/{territory}/blocks", tags=tag)
async def get_level_blocks_by_id(
    request: Request,
    city: Annotated[int | str, Path(description="city id or name")],  # pylint: disable=unused-argument
    territory: Annotated[int | str, Path(description="territory id or name")],
    no_geometry: Annotated[bool, Query(description="no geometry and centers")] = False,
) -> list[CATerritoriesData | CATerritoriesWithoutGeometryData]:
    blocks_service: BlocksService = request.state.blocks_service

    if not no_geometry:
        return [
            await CATerritoriesData.from_dto(block)
            for block in await blocks_service.get_blocks_by_territory_id(territory, no_geometry=no_geometry)
        ]
    return [
        await CATerritoriesWithoutGeometryData.from_dto(block)
        for block in await blocks_service.get_blocks_by_territory_id(territory, no_geometry=no_geometry)
    ]


@app.get("/city/{city}/level/{territory}/physical_objects", tags=tag)
async def get_physical_objects_by_level_with_city_id(
    request: Request,
    city: Annotated[int | str, Path(description="city id or name")],
    territory: Annotated[int | str, Path(description="territory id or name")],
):
    physical_objects_service: PhysicalObjectsService = request.state.physical_objects_service
    result: list[PhysicalObjectsDTO] = await physical_objects_service.get_physical_objects_by_territory_id(
        city, territory
    )
    return await Feature.generate_feature_collection(
        result, "geometry", "centre_point", {}, ["geometry", "centre_point"]
    )


@app.get("/city/{city}/level/{territory}/services", tags=tag)
async def get_services_by_level_with_city_id(
    request: Request,
    city: Annotated[int | str, Path(description="city id or name")],
    territory: Annotated[int | str, Path(description="territory id or name")],
):
    services_service: ServicesService = request.state.services_service
    result: list[CityServiceDTO] = await services_service.get_services_by_territory_id(city, territory)
    return await Feature.generate_feature_collection(
        result, "geometry", "centre_point", {}, ["geometry", "centre_point"]
    )
