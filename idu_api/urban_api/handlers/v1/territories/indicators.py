"""Indicators territories-related handlers are defined here."""

from datetime import datetime

from fastapi import Path, Query, Request
from geojson_pydantic import Feature
from geojson_pydantic.geometries import Geometry
from starlette import status

from idu_api.urban_api.logic.territories import TerritoriesService
from idu_api.urban_api.schemas import Indicator, IndicatorValue, TerritoryWithIndicators
from idu_api.urban_api.schemas.enums import ValueType
from idu_api.urban_api.schemas.geometries import GeoJSONResponse

from .routers import territories_router


@territories_router.get(
    "/territory/{territory_id}/indicators",
    response_model=list[Indicator],
    status_code=status.HTTP_200_OK,
)
async def get_indicators_by_territory_id(
    request: Request,
    territory_id: int = Path(..., description="territory id", gt=0),
) -> list[Indicator]:
    """Get indicators for a given territory.

    is_city can be passed to filter results.
    """
    territories_service: TerritoriesService = request.state.territories_service

    indicators = await territories_service.get_indicators_by_territory_id(territory_id)

    return [Indicator.from_dto(indicator) for indicator in indicators]


@territories_router.get(
    "/territory/{territory_id}/indicator_values",
    response_model=list[IndicatorValue],
    status_code=status.HTTP_200_OK,
)
async def get_indicator_values_by_territory_id(
    request: Request,
    territory_id: int = Path(..., description="territory id", gt=0),
    indicator_ids: str | None = Query(None, description="list of identifiers separated by comma"),
    indicators_group_id: int | None = Query(None, description="to filter by indicator group (identifier)"),
    start_date: datetime | None = Query(None, description="lowest date included"),
    end_date: datetime | None = Query(None, description="highest date included"),
    value_type: ValueType = Query(None, description="to filter by value type"),
    information_source: str | None = Query(None, description="to filter by source"),
    last_only: bool = Query(False, description="to get last indicators"),
) -> list[IndicatorValue]:
    """Get indicator values for a given territory, value type, source and time period.

    Could be specified by last_only to get only last indicator values.
    """
    territories_service: TerritoriesService = request.state.territories_service

    value_type_field = value_type.value if value_type is not None else None

    indicator_values = await territories_service.get_indicator_values_by_territory_id(
        territory_id,
        indicator_ids,
        indicators_group_id,
        start_date,
        end_date,
        value_type_field,
        information_source,
        last_only,
    )

    return [IndicatorValue.from_dto(value) for value in indicator_values]


@territories_router.get(
    "/territory/indicator_values",
    response_model=GeoJSONResponse[Feature[Geometry, TerritoryWithIndicators]],
    status_code=status.HTTP_200_OK,
)
async def get_indicator_values_by_parent_id(
    request: Request,
    parent_id: int | None = Query(None, description="parent territory id", gt=0),
    indicator_ids: str | None = Query(None, description="list id separated by commas"),
    indicators_group_id: int | None = Query(None, description="to filter by indicator group (identifier)"),
    start_date: datetime | None = Query(None, description="left edge"),
    end_date: datetime | None = Query(None, description="right edge"),
    value_type: ValueType = Query(None, description="to filter by value type"),
    information_source: str | None = Query(None, description="to filter by source"),
    last_only: bool = Query(False, description="to get last indicators"),
) -> GeoJSONResponse[Feature[Geometry, TerritoryWithIndicators]]:
    """Get FeatureCollection with child territories and indicator values in properties.

    Parent id should be null or skipped for high-level territories.
    Could be specified by last_only flag to get only last indicator values.
    """
    territories_service: TerritoriesService = request.state.territories_service

    value_type_field = value_type.value if value_type is not None else None

    territories = await territories_service.get_indicator_values_by_parent_id(
        parent_id,
        indicator_ids,
        indicators_group_id,
        start_date,
        end_date,
        value_type_field,
        information_source,
        last_only,
    )

    return await GeoJSONResponse.from_list([territory.to_geojson_dict() for territory in territories])
