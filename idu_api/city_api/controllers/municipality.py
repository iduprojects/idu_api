from typing import Annotated, Dict

from idu_api.city_api import app
from fastapi import Request, Path

from idu_api.city_api.common.feature import Feature
from idu_api.city_api.dto.physical_objects import PhysicalObjectsDTO
from idu_api.city_api.dto.services import CityServiceDTO
from idu_api.city_api.dto.services_count import ServiceCountDTO
from idu_api.city_api.services.objects.physical_objects import PhysicalObjectsService
from idu_api.city_api.services.objects.services import ServicesService
from idu_api.city_api.services.territories.municipalities import MunicipalitiesService
from idu_api.urban_api.dto import TerritoryDTO
from idu_api.urban_api.schemas.geometries import Geometry

tag = ["municipality-controller"]


@app.get("/city/{city}/municipality/{municipality}/city_service_types/counts", tags=tag)
async def get_city_service_types_count_by_municipality_with_city_id(
        request: Request,
        city: Annotated[int, Path(description="city id or name")],
        municipality: Annotated[int, Path(description="municipality id or name")],
) -> Dict[str, int]:
    services_service: ServicesService = request.state.services_service
    result: list[ServiceCountDTO] = await services_service.get_services_types_by_territory_id(city, municipality)
    counts = {}
    for service in result:
        counts[service.name] = service.count
    return counts


@app.get("/city/{city}/municipality/{municipality}/geometry", tags=tag)
async def get_city_geometry(
        request: Request,
        city: Annotated[int, Path(gt=0, description="city id")],
        municipality: Annotated[int, Path(description="municipality id or name")],
) -> Geometry:
    municipalities_service: MunicipalitiesService = request.state.municipalities_service
    result: TerritoryDTO = await municipalities_service.get_municipality_by_id(municipality)
    return Geometry.from_shapely_geometry(result.geometry)


@app.get("/city/{city}/municipality/{municipality}/physical_objects", tags=tag)
async def get_physical_objects_by_municipality_with_city_id(
        request: Request,
        city: Annotated[int, Path(description="city id or name")],
        municipality: Annotated[int, Path(description="municipality id or name")],
):
    physical_objects_service: PhysicalObjectsService = request.state.physical_objects_service
    result: list[PhysicalObjectsDTO] = await physical_objects_service.get_physical_objects_by_territory_id(
        city,
        municipality
    )
    return await Feature.generate_feature_collection(
        result,
        "geometry",
        "centre_point",
        {},
        ["geometry", "centre_point"]
    )


@app.get("/city/{city}/municipality/{municipality}/services", tags=tag)
async def get_services_by_municipality_with_city_id(
        request: Request,
        city: Annotated[int, Path(description="city id or name")],
        municipality: Annotated[int, Path(description="municipality id or name")],
):
    services_service: ServicesService = request.state.services_service
    result: list[CityServiceDTO] = await services_service.get_services_by_territory_id(city, municipality)
    return await Feature.generate_feature_collection(
        result,
        "geometry",
        "centre_point",
        {},
        ["geometry", "centre_point"]
    )
