from typing import Annotated, Dict

from fastapi.params import Query
from geojson import FeatureCollection

from idu_api.city_api import app
from idu_api.city_api.common.feature import Feature
from idu_api.city_api.dto.administrative_units import AdministrativeUnitsDTO
from idu_api.city_api.dto.munipalities import MunicipalitiesDTO
from idu_api.city_api.dto.physical_objects import PhysicalObjectsDTO
from idu_api.city_api.dto.services import CityServiceDTO
from idu_api.city_api.dto.services_count import ServiceCountDTO
from idu_api.city_api.schemas.municipalities import MunicipalitiesData
from idu_api.city_api.schemas.service_types_counts import ServiceTypesCountsData
from idu_api.city_api.services.objects.physical_objects import PhysicalObjectsService
from idu_api.city_api.services.objects.services import ServicesService
from idu_api.city_api.services.territories.administrative_units import AdministrativeUnitsService
from idu_api.city_api.services.territories.municipalities import MunicipalitiesService
from idu_api.urban_api.schemas.geometries import Geometry
from fastapi import Request, Path

tag = ["administrative-unit-controller"]


@app.get("/city/{city}/administrative_unit/{administrative_unit}/city_service_types/counts", tags=tag)
async def get_administrative_unit_with_city_id(
        request: Request,
        city: Annotated[int, Path(description="city id or name")],
        administrative_unit: Annotated[int, Path(description="administrative unit id or name")]
) -> Dict[str, int]:
    services_service: ServicesService = request.state.services_service
    result: list[ServiceCountDTO] = await services_service.get_services_types_by_territory_id(city, administrative_unit)
    counts = {}
    for service in result:
        counts[service.name] = service.count
    return counts


@app.get("/city/{city}/administrative_unit/{administrative_unit}/geometry", tags=tag)
async def get_administrative_unit_with_city_id(
        request: Request,
        city: Annotated[int, Path(description="city id or name")],
        administrative_unit: Annotated[int, Path(description="administrative unit id or name")]
) -> Geometry:
    adm_units_service: AdministrativeUnitsService = request.state.administrative_units_service
    result: list[AdministrativeUnitsDTO] = await adm_units_service.get_administrative_units_by_city_id(
        city,
        [administrative_unit]
    )
    return Geometry.from_shapely_geometry(result[0].geometry)


@app.get("/city/{city}/administrative_unit/{administrative_unit}/municipalities", tags=tag)
async def get_administrative_unit_with_city_id(
        request: Request,
        city: Annotated[int, Path(description="city id or name")],
        administrative_unit: Annotated[int, Path(description="administrative unit id or name")],
) -> list[MunicipalitiesData]:
    municipalities_service: MunicipalitiesService = request.state.municipalities_service
    result: list[MunicipalitiesDTO] = await municipalities_service.get_municipalities_by_administrative_unit_id(
        city,
        administrative_unit,
    )
    return [await MunicipalitiesData.from_dto(municipality) for municipality in result]


@app.get("/city/{city}/administrative_unit/{administrative_unit}/physical_objects", tags=tag)
async def get_physical_objects_by_adm_unit_with_city_id(
        request: Request,
        city: Annotated[int, Path(description="city id or name")],
        administrative_unit: Annotated[int, Path(description="administrative unit id or name")],
):
    physical_objects_service: PhysicalObjectsService = request.state.physical_objects_service
    result: list[PhysicalObjectsDTO] = await physical_objects_service.get_physical_objects_by_territory_id(
        city,
        administrative_unit
    )
    return await Feature.generate_feature_collection(
        result,
        "geometry",
        "centre_point",
        {},
        ["geometry", "centre_point"]
    )


@app.get("/city/{city}/administrative_unit/{administrative_unit}/services", tags=tag)
async def get_services_by_adm_unit_with_city_id(
        request: Request,
        city: Annotated[int, Path(description="city id or name")],
        administrative_unit: Annotated[int, Path(description="administrative unit id or name")],
):
    services_service: ServicesService = request.state.services_service
    result: list[CityServiceDTO] = await services_service.get_services_by_territory_id(city, administrative_unit)
    return await Feature.generate_feature_collection(
        result,
        "geometry",
        "centre_point",
        {},
        ["geometry", "centre_point"]
    )
