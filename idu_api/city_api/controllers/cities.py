from typing import Annotated

from idu_api.city_api import app
from idu_api.city_api.services.territories.geometries import GeometriesService
from idu_api.urban_api.dto import TerritoryDTO
from idu_api.urban_api.schemas.geometries import Geometry
from fastapi import Request, Path


@app.get("/city/{city}/geometry", tags=["city-controller"])
async def get_city_geometry(
        request: Request,
        city: Annotated[int, Path(gt=0, description="city id")]
) -> Geometry:
    geometries_service: GeometriesService = request.state.geometries_service
    result: TerritoryDTO = await geometries_service.get_city_by_id(city)
    return Geometry.from_shapely_geometry(result.geometry)
