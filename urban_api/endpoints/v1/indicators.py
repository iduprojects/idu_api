"""
Indicators endpoints are defined here.
"""

from datetime import datetime
from typing import List, Optional

from fastapi import Depends, Path, Query
from sqlalchemy.ext.asyncio import AsyncConnection
from starlette import status

from urban_api.db.connection import get_connection
from urban_api.logic.indicators import (
    add_indicator_to_db,
    add_indicator_value_to_db,
    add_measurement_unit_to_db,
    get_indicator_by_id_from_db,
    get_indicator_value_by_id_from_db,
    get_indicator_values_by_id_from_db,
    get_indicators_by_parent_id_from_db,
    get_measurement_units_from_db,
)
from urban_api.schemas import Indicators, IndicatorsPost, IndicatorValue, MeasurementUnit, MeasurementUnitPost

from .routers import indicators_router


@indicators_router.get(
    "/measurement_units",
    response_model=List[MeasurementUnit],
    status_code=status.HTTP_200_OK,
)
async def get_measurement_units(connection: AsyncConnection = Depends(get_connection)) -> List[MeasurementUnit]:
    """
    Summary:
        Get existing measurement units

    Description:
        Get a list of all measurement units added
    """

    measurement_units = await get_measurement_units_from_db(connection)

    return [MeasurementUnit.from_dto(measurement_unit) for measurement_unit in measurement_units]


@indicators_router.post(
    "/measurement_units",
    response_model=MeasurementUnit,
    status_code=status.HTTP_201_CREATED,
)
async def add_measurement_unit(
    measurement_unit: MeasurementUnitPost, connection: AsyncConnection = Depends(get_connection)
) -> MeasurementUnit:
    """
    Summary:
        Add a new measurement unit

    Description:
        Add a new measurement unit by name
    """

    unit = await add_measurement_unit_to_db(measurement_unit, connection)

    return MeasurementUnit.from_dto(unit)


@indicators_router.get(
    "/indicators_by_parent",
    response_model=List[Indicators],
    status_code=status.HTTP_200_OK,
)
async def get_indicators_by_parent_id(
    parent_id: int = Query(..., description="Parent indicator id to filter, should be 0 for top level indicators"),
    get_all_subtree: bool = Query(
        False, description="Getting full subtree of indicators (unsafe for high level parents"
    ),
    connection: AsyncConnection = Depends(get_connection),
) -> List[Indicators]:
    """
    Summary:
        Get indicators dictionary

    Description:
        Get a list of indicators by parent id
    """

    indicators = await get_indicators_by_parent_id_from_db(parent_id, connection, get_all_subtree)

    return [Indicators.from_dto(indicator) for indicator in indicators]


@indicators_router.get(
    "/indicator",
    response_model=Indicators,
    status_code=status.HTTP_200_OK,
)
async def get_indicator_by_id(
    indicator_id: int = Query(..., description="Getting indicator by id"),
    connection: AsyncConnection = Depends(get_connection),
) -> Indicators:
    """
    Summary:
        Get indicator

    Description:
        Get indicator by id
    """

    indicator = await get_indicator_by_id_from_db(indicator_id, connection)

    return Indicators.from_dto(indicator)


@indicators_router.post(
    "/indicator",
    response_model=Indicators,
    status_code=status.HTTP_201_CREATED,
)
async def add_indicator(indicator: IndicatorsPost, connection: AsyncConnection = Depends(get_connection)) -> Indicators:
    """
    Summary:
        Add a new indicator

    Description:
        Add a new indicator
    """

    indicator_dto = await add_indicator_to_db(indicator, connection)

    return Indicators.from_dto(indicator_dto)


@indicators_router.get(
    "/indicator_value",
    response_model=IndicatorValue,
    status_code=status.HTTP_200_OK,
)
async def get_indicator_value_by_id(
    indicator_id: int = Query(..., description="indicator id"),
    territory_id: int = Query(..., description="territory id"),
    date_type: str = Query(..., description="date type"),
    date_value: datetime = Query(..., description="time value"),
    connection: AsyncConnection = Depends(get_connection),
) -> IndicatorValue:
    """
    Summary:
        Get indicator value

    Description:
        Get indicator value by id
    """

    indicator_value = await get_indicator_value_by_id_from_db(
        indicator_id, territory_id, date_type, date_value, connection
    )

    return IndicatorValue.from_dto(indicator_value)


@indicators_router.post(
    "/indicator_value",
    response_model=IndicatorValue,
    status_code=status.HTTP_201_CREATED,
)
async def add_indicator_value(
    indicator_value: IndicatorValue, connection: AsyncConnection = Depends(get_connection)
) -> IndicatorValue:
    """
    Summary:
        Add a new indicator value

    Description:
        Add a new indicator value
    """

    indicator_value_dto = await add_indicator_value_to_db(indicator_value, connection)

    return IndicatorValue.from_dto(indicator_value_dto)


@indicators_router.get(
    "/indicator/{indicator_id}/values",
    response_model=List[IndicatorValue],
    status_code=status.HTTP_200_OK,
)
async def get_indicator_values_by_id(
    indicator_id: int = Path(..., description="indicator id"),
    territory_id: Optional[int] = Query(None, description="territory id"),
    date_type: Optional[str] = Query(None, description="date type"),
    date_value: Optional[datetime] = Query(None, description="time value"),
    connection: AsyncConnection = Depends(get_connection),
) -> List[IndicatorValue]:
    """
    Summary:
        Get indicator values

    Description:
        Get indicator values by id, territory and date could be specified in parameters.
        If parameter not specified there should be all available values for this indicator
    """

    indicator_values = await get_indicator_values_by_id_from_db(
        indicator_id, territory_id, date_type, date_value, connection
    )

    return [IndicatorValue.from_dto(value) for value in indicator_values]
