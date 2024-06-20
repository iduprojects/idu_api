"""Territories indicators endpoints are defined here."""

from datetime import datetime

from fastapi import Depends, Path, Query
from sqlalchemy.ext.asyncio import AsyncConnection
from starlette import status

from urban_api.db.connection import get_connection
from urban_api.logic.territories import (
    get_indicator_values_by_territory_id_from_db,
    get_indicators_by_territory_id_from_db,
)
from urban_api.schemas import Indicator, IndicatorValue
from urban_api.schemas.enums import DateType

from .routers import territories_router


@territories_router.get(
    "/territory/{territory_id}/indicators",
    response_model=list[Indicator],
    status_code=status.HTTP_200_OK,
)
async def get_indicators_by_territory_id(
    territory_id: int = Path(description="territory id", gt=0),
    connection: AsyncConnection = Depends(get_connection),
) -> list[Indicator]:
    """
    Summary:
        Get indicators for territory

    Description:
        Get indicators for territory by id
    """

    indicators = await get_indicators_by_territory_id_from_db(territory_id, connection)

    return [Indicator.from_dto(indicator) for indicator in indicators]


@territories_router.get(
    "/territory/{territory_id}/indicators_values",
    response_model=list[IndicatorValue],
    status_code=status.HTTP_200_OK,
)
async def get_indicator_values_by_territory_id(
    territory_id: int = Path(description="territory id", gt=0),
    date_type: DateType | None = Query(None, description="Date type"),
    date_value: datetime | None = Query(None, description="Time value"),
    connection: AsyncConnection = Depends(get_connection),
) -> list[IndicatorValue]:
    """
    Summary:
        Get indicators values for territory

    Description:
        Get indicators values for territory by id, time period could be specified in parameters
    """

    indicator_values = await get_indicator_values_by_territory_id_from_db(
        territory_id, connection, date_type, date_value
    )

    return [IndicatorValue.from_dto(value) for value in indicator_values]
