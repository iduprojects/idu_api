from typing import Annotated

from idu_api.city_api import app
from idu_api.city_api.dto.administrative_units import AdministrativeUnitsDTO, AdministrativeUnitsWithoutGeometryDTO
from idu_api.city_api.dto.munipalities import MunicipalitiesDTO, MunicipalitiesWithoutGeometryDTO
from idu_api.city_api.schemas.adminstrative_units import AdministrativeUnitsData, AdministrativeUnitsWithoutGeometryData
from idu_api.city_api.schemas.municipalities import MunicipalitiesData, MunicipalitiesWithoutGeometryData
from idu_api.city_api.services.territories.administrative_units import AdministrativeUnitsService
from idu_api.city_api.services.territories.cities import CitiesService
from idu_api.city_api.services.territories.municipalities import MunicipalitiesService
from idu_api.urban_api.dto import TerritoryDTO
from idu_api.urban_api.schemas.geometries import Geometry
from fastapi import Request, Path, Query

tag = ["city-controller"]


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
