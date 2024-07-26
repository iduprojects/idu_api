from typing import Annotated, Dict

from idu_api.city_api import app
from idu_api.city_api.common.feature import Feature
from idu_api.city_api.dto.administrative_units import AdministrativeUnitsDTO
from idu_api.city_api.dto.munipalities import MunicipalitiesDTO
from idu_api.city_api.dto.physical_objects import PhysicalObjectsDTO
from idu_api.city_api.dto.services import CityServiceDTO
from idu_api.city_api.dto.services_count import ServiceCountDTO
from idu_api.city_api.schemas.adminstrative_units import AdministrativeUnitsData
from idu_api.city_api.schemas.municipalities import MunicipalitiesData
from idu_api.city_api.schemas.service_types import ServiceTypesData
from idu_api.city_api.services.objects.physical_objects import PhysicalObjectsService
from idu_api.city_api.services.objects.services import ServicesService
from idu_api.city_api.services.territories.administrative_units import AdministrativeUnitsService
from idu_api.city_api.services.territories.cities import CitiesService
from idu_api.city_api.services.territories.municipalities import MunicipalitiesService
from idu_api.urban_api.dto import TerritoryDTO
from idu_api.urban_api.schemas.geometries import Geometry
from fastapi import Request, Path, Query

tag = ["city-controller"]


@app.get("/city/{city}/city_service_types", tags=tag)
async def get_city_service_types_by_city_id(
        request: Request,
        city: Annotated[int, Path(description="city id or name")],
) -> list[ServiceTypesData]:
    services_service: ServicesService = request.state.services_service
    result: list[ServiceCountDTO] = await services_service.get_services_types_by_territory_id(city, city)
    return [await ServiceTypesData.from_dto(service) for service in result]


@app.get("/city/{city}/city_service_types/counts", tags=tag)
async def get_city_service_types_count_by_city_id(
        request: Request,
        city: Annotated[int, Path(description="city id or name")],
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
        city: Annotated[int, Path(gt=0, description="city id")]
) -> Geometry:
    geometries_service: CitiesService = request.state.cities_service
    result: TerritoryDTO = await geometries_service.get_city_by_id(city)
    return Geometry.from_shapely_geometry(result.geometry)


@app.get("/city/{city}/administrative_units", tags=tag)
async def get_administrative_units_by_city_id(
        request: Request,
        city: Annotated[int, Path(gt=0, description="city id")],
) -> list[AdministrativeUnitsData]:
    administrative_units_service: AdministrativeUnitsService = request.state.administrative_units_service

    result: list[AdministrativeUnitsDTO] = \
        await administrative_units_service.get_administrative_units_by_city_id(city)
    return [await AdministrativeUnitsData.from_dto(adm_unit) for adm_unit in result]


@app.get("/city/{city}/municipalities", tags=tag)
async def get_municipalities_by_city_id(
        request: Request,
        city: Annotated[int, Path(gt=0, description="city id")],
) -> list[MunicipalitiesData]:
    municipalities_service: MunicipalitiesService = request.state.municipalities_service

    result: list[MunicipalitiesDTO] = \
        await municipalities_service.get_municipalities_by_city_id(city)
    return [await MunicipalitiesData.from_dto(municipality) for municipality in result]


@app.get("/city/{city}/physical_objects", tags=tag)
async def get_physical_objects_by_city_id(
        request: Request,
        city: Annotated[int, Path(gt=0, description="city id")],
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
        city: Annotated[int, Path(gt=0, description="city id")],
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
