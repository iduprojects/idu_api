from typing import Annotated

from idu_api.city_api import app
from idu_api.city_api.dto.administrative_units import AdministrativeUnitsDTO
from idu_api.city_api.schemas.adminstrative_units import AdministrativeUnitsData
from idu_api.city_api.services.territories.administrative_units import AdministrativeUnitsService
from fastapi import Request, Path


@app.get("/city/{city}/administrative_units", tags=["city-controller"])
async def get_administrative_units_by_city_id(
        request: Request,
        city: Annotated[int, Path(gt=0, description="city id")]
) -> list[AdministrativeUnitsData]:
    administrative_units_service: AdministrativeUnitsService = request.state.administrative_units_service

    result: list[AdministrativeUnitsDTO] = await administrative_units_service.get_administrative_units_by_city_id(city)
    return [await AdministrativeUnitsData.from_dto(adm_unit) for adm_unit in result]