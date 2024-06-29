"""Territories indicators handlers are defined here."""

from datetime import datetime

from fastapi import Path, Query, Request
from starlette import status

from urban_api.logic.territories import TerritoriesService
from urban_api.schemas import Indicator, IndicatorValue
from urban_api.schemas.enums import DateType

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
