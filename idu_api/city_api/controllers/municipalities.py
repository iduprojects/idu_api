from typing import Annotated

from idu_api.city_api import app
from fastapi import Request, Path

from idu_api.city_api.dto.munipalities import MunicipalitiesDTO
from idu_api.city_api.schemas.municipalities import MunicipalitiesData
from idu_api.city_api.services.territories.municipalities import MunicipalitiesService


@app.get("/city/{city}/municipalities", tags=["city-controller"])
async def get_municipalities_by_city_id(
        request: Request,
        city: Annotated[int, Path(gt=0, description="city id")]
) -> list[MunicipalitiesData]:
    municipalities_service: MunicipalitiesService = request.state.municipalities_service

    result: list[MunicipalitiesDTO] = await municipalities_service.get_municipalities_by_city_id(city)
    return [await MunicipalitiesData.from_dto(municipality) for municipality in result]
