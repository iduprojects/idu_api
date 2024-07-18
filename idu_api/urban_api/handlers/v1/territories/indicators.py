"""Territories indicators handlers are defined here."""

from datetime import datetime

from fastapi import Path, Query, Request
from geojson_pydantic import Feature
from geojson_pydantic.geometries import Geometry
from starlette import status

from idu_api.urban_api.logic.territories import TerritoriesService
from idu_api.urban_api.schemas import Indicator, IndicatorValue, TerritoryWithIndicator, TerritoryWithIndicators
from idu_api.urban_api.schemas.enums import DateType
from idu_api.urban_api.schemas.geometries import GeoJSONResponse

from .routers import territories_router


@territories_router.get(
    "/territory/{territory_id}/indicators",
    response_model=list[Indicator],
    status_code=status.HTTP_200_OK,
)
async def get_indicators_by_territory_id(
    request: Request, territory_id: int = Path(description="territory id", gt=0)
) -> list[Indicator]:
    """Get indicators for a given territory."""
    territories_service: TerritoriesService = request.state.territories_service

    indicators = await territories_service.get_indicators_by_territory_id(territory_id)

    return [Indicator.from_dto(indicator) for indicator in indicators]


@territories_router.get(
    "/territory/{territory_id}/indicators_values",
    response_model=list[IndicatorValue],
    status_code=status.HTTP_200_OK,
)
async def get_indicator_values_by_territory_id(
    request: Request,
    territory_id: int = Path(description="territory id", gt=0),
    date_type: DateType | None = Query(None, description="Date type"),
    date_value: datetime | None = Query(None, description="Time value"),
) -> list[IndicatorValue]:
    """Get indicators values for a given territory and date period"""
    territories_service: TerritoriesService = request.state.territories_service

    indicator_values = await territories_service.get_indicator_values_by_territory_id(
        territory_id, date_type, date_value
    )

    return [IndicatorValue.from_dto(value) for value in indicator_values]


@territories_router.get(
    "/territory/indicator_values",
    response_model=GeoJSONResponse[Feature[Geometry, TerritoryWithIndicator]],
    status_code=status.HTTP_200_OK,
)
async def get_indicator_values_by_parent_id(
    request: Request,
    indicator_id: int = Query(description="indicator id", gt=0),
    parent_id: int | None = Query(None, description="parent territory id", gt=0),
    date_type: DateType = Query(description="Date type"),
    date_value: datetime = Query(description="Time value"),
) -> GeoJSONResponse[Feature[Geometry, TerritoryWithIndicator]]:
    """Get FeatureCollection with child territories and indicator values in properties
    by parent id, indicator id and time period. parent id should be null or skipped for high-level territories."""
    territories_service: TerritoriesService = request.state.territories_service

    territories = await territories_service.get_indicator_values_by_parent_id(
        parent_id, date_type, date_value, indicator_id
    )

    return await GeoJSONResponse.from_list([territory.to_geojson_dict() for territory in territories])


@territories_router.get(
    "/territory/indicators_values",
    response_model=GeoJSONResponse[Feature[Geometry, TerritoryWithIndicators]],
    status_code=status.HTTP_200_OK,
)
async def get_indicators_values_by_parent_id(
    request: Request,
    parent_id: int | None = Query(None, description="parent territory id", gt=0),
    date_type: DateType = Query(description="Date type"),
    date_value: datetime = Query(description="Time value"),
) -> GeoJSONResponse[Feature[Geometry, TerritoryWithIndicators]]:
    """Get FeatureCollection with child territories and all indicators values in properties
    by parent id and time period. parent id should be null or skipped for high-level territories."""
    territories_service: TerritoriesService = request.state.territories_service

    territories = await territories_service.get_indicators_values_by_parent_id(parent_id, date_type, date_value)

    return await GeoJSONResponse.from_list([territory.to_geojson_dict() for territory in territories])
