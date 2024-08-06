"""Indicators handlers are defined here."""

from datetime import datetime
from typing import List, Optional

from fastapi import Path, Query, Request
from sqlalchemy.ext.asyncio import AsyncConnection
from starlette import status

from idu_api.urban_api.logic.indicators import (
    add_indicator_to_db,
    add_indicator_value_to_db,
    add_measurement_unit_to_db,
    get_indicator_by_id_from_db,
    get_indicator_value_by_id_from_db,
    get_indicator_values_by_id_from_db,
    get_indicators_by_parent_id_from_db,
    get_measurement_units_from_db,
)
from idu_api.urban_api.schemas import (
    Indicator,
    IndicatorsPost,
    IndicatorValue,
    IndicatorValuePost,
    MeasurementUnit,
    MeasurementUnitPost
)
from idu_api.urban_api.schemas.enums import DateType, ValueType

from .routers import indicators_router


@indicators_router.get(
    "/measurement_units",
    response_model=List[MeasurementUnit],
    status_code=status.HTTP_200_OK,
)
async def get_measurement_units(request: Request) -> List[MeasurementUnit]:
    """Get existing measurement units."""
    conn: AsyncConnection = request.state.conn

    measurement_units = await get_measurement_units_from_db(conn)

    return [MeasurementUnit.from_dto(measurement_unit) for measurement_unit in measurement_units]


@indicators_router.post(
    "/measurement_units",
    response_model=MeasurementUnit,
    status_code=status.HTTP_201_CREATED,
)
async def add_measurement_unit(request: Request, measurement_unit: MeasurementUnitPost) -> MeasurementUnit:
    """Add measurement unit."""
    conn: AsyncConnection = request.state.conn

    unit = await add_measurement_unit_to_db(conn, measurement_unit)

    return MeasurementUnit.from_dto(unit)


@indicators_router.get(
    "/indicators_by_parent",
    response_model=List[Indicator],
    status_code=status.HTTP_200_OK,
)
async def get_indicators_by_parent_id(
    request: Request,
    parent_id: Optional[int] = Query(
        None, description="Parent indicator id to filter, should be skipped to get top level indicators"
    ),
    name: Optional[str] = Query(None, description="Filter by indicator name"),
    territory_id: Optional[int] = Query(None, description="Filter by territory id (not including inner territories)"),
    get_all_subtree: bool = Query(False, description="Getting full subtree of indicators"),
) -> List[Indicator]:
    """Get a list of indicators by parent id."""
    conn: AsyncConnection = request.state.conn

    indicators = await get_indicators_by_parent_id_from_db(conn, parent_id, name, territory_id, get_all_subtree)

    return [Indicator.from_dto(indicator) for indicator in indicators]


@indicators_router.get(
    "/indicator",
    response_model=Indicator,
    status_code=status.HTTP_200_OK,
)
async def get_indicator_by_id(
    request: Request,
    indicator_id: int = Query(description="Getting indicator by id"),
) -> Indicator:
    """Get indicator."""
    conn: AsyncConnection = request.state.conn

    indicator = await get_indicator_by_id_from_db(conn, indicator_id)

    return Indicator.from_dto(indicator)


@indicators_router.post(
    "/indicator",
    response_model=Indicator,
    status_code=status.HTTP_201_CREATED,
)
async def add_indicator(request: Request, indicator: IndicatorsPost) -> Indicator:
    """Add indicator."""
    conn: AsyncConnection = request.state.conn

    indicator_dto = await add_indicator_to_db(conn, indicator)

    return Indicator.from_dto(indicator_dto)


@indicators_router.get(
    "/indicator_value",
    response_model=IndicatorValue,
    status_code=status.HTTP_200_OK,
)
async def get_indicator_value_by_id(
    request: Request,
    indicator_id: int = Query(description="indicator id"),
    territory_id: int = Query(description="territory id"),
    date_type: DateType = Query(description="date type"),
    date_value: datetime = Query(description="time value"),
    value_type: ValueType = Query(description="value type"),
    information_source: str = Query(description="information source"),
) -> IndicatorValue:
    """Get indicator value for a given territory, date period, value type and source."""
    conn: AsyncConnection = request.state.conn

    indicator_value = await get_indicator_value_by_id_from_db(
        conn, indicator_id, territory_id, date_type.value, date_value, value_type.value, information_source
    )

    return IndicatorValue.from_dto(indicator_value)


@indicators_router.post(
    "/indicator_value",
    response_model=IndicatorValue,
    status_code=status.HTTP_201_CREATED,
)
async def add_indicator_value(request: Request, indicator_value: IndicatorValuePost) -> IndicatorValue:
    """Add a new indicator value for a given territory and date period."""
    conn: AsyncConnection = request.state.conn

    indicator_value_dto = await add_indicator_value_to_db(conn, indicator_value)

    return IndicatorValue.from_dto(indicator_value_dto)


@indicators_router.get(
    "/indicator/{indicator_id}/values",
    response_model=List[IndicatorValue],
    status_code=status.HTTP_200_OK,
)
async def get_indicator_values_by_id(
    request: Request,
    indicator_id: int = Path(description="indicator id"),
    territory_id: Optional[int] = Query(None, description="territory id"),
    date_type: DateType = Query(None, description="date type"),
    date_value: Optional[datetime] = Query(None, description="time value"),
    value_type: ValueType = Query(None, description="value type"),
    information_source: Optional[str] = Query(None, description="information source"),
) -> List[IndicatorValue]:
    """
    Summary:
        Get indicator values

    Description:
        Get indicator values by id, territory, date, value type and source could be specified in parameters.
        If parameters not specified there should be all available values for this indicator.
    """
    conn: AsyncConnection = request.state.conn

    date_type_field = date_type.value if date_type is not None else None
    value_type_field = value_type.value if value_type is not None else None

    indicator_values = await get_indicator_values_by_id_from_db(
        conn, indicator_id, territory_id, date_type_field, date_value, value_type_field, information_source
    )

    return [IndicatorValue.from_dto(value) for value in indicator_values]
