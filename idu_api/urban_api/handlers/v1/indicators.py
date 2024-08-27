"""Indicators handlers are defined here."""

from datetime import datetime

from fastapi import Path, Query, Request
from starlette import status

from idu_api.urban_api.logic.indicators import IndicatorsService
from idu_api.urban_api.schemas import (
    Indicator,
    IndicatorsPost,
    IndicatorValue,
    IndicatorValuePost,
    MeasurementUnit,
    MeasurementUnitPost,
)
from idu_api.urban_api.schemas.enums import DateType, ValueType

from .routers import indicators_router


@indicators_router.get(
    "/measurement_units",
    response_model=list[MeasurementUnit],
    status_code=status.HTTP_200_OK,
)
async def get_measurement_units(request: Request) -> list[MeasurementUnit]:
    """Get existing measurement units."""
    indicators_service: IndicatorsService = request.state.indicators_service

    measurement_units = await indicators_service.get_measurement_units()

    return [MeasurementUnit.from_dto(measurement_unit) for measurement_unit in measurement_units]


@indicators_router.post(
    "/measurement_units",
    response_model=MeasurementUnit,
    status_code=status.HTTP_201_CREATED,
)
async def add_measurement_unit(request: Request, measurement_unit: MeasurementUnitPost) -> MeasurementUnit:
    """Add measurement unit."""
    indicators_service: IndicatorsService = request.state.indicators_service

    unit = await indicators_service.add_measurement_unit(measurement_unit)

    return MeasurementUnit.from_dto(unit)


@indicators_router.get(
    "/indicators_by_parent",
    response_model=list[Indicator],
    status_code=status.HTTP_200_OK,
)
async def get_indicators_by_parent_id(
    request: Request,
    parent_id: int | None = Query(
        None, description="Parent indicator id to filter, should be skipped to get top level indicators"
    ),
    name: str | None = Query(None, description="Filter by indicator name"),
    territory_id: int | None = Query(None, description="Filter by territory id (not including inner territories)"),
    get_all_subtree: bool = Query(False, description="Getting full subtree of indicators"),
) -> list[Indicator]:
    """Get a list of indicators by parent id."""
    indicators_service: IndicatorsService = request.state.indicators_service

    indicators = await indicators_service.get_indicators_by_parent_id(parent_id, name, territory_id, get_all_subtree)

    return [Indicator.from_dto(indicator) for indicator in indicators]


@indicators_router.get(
    "/indicator",
    response_model=Indicator,
    status_code=status.HTTP_200_OK,
)
async def get_indicator_by_id(
    request: Request,
    indicator_id: int = Query(..., description="Getting indicator by id"),
) -> Indicator:
    """Get indicator."""
    indicators_service: IndicatorsService = request.state.indicators_service

    indicator = await indicators_service.get_indicator_by_id(indicator_id)

    return Indicator.from_dto(indicator)


@indicators_router.post(
    "/indicator",
    response_model=Indicator,
    status_code=status.HTTP_201_CREATED,
)
async def add_indicator(request: Request, indicator: IndicatorsPost) -> Indicator:
    """Add indicator."""
    indicators_service: IndicatorsService = request.state.indicators_service

    indicator_dto = await indicators_service.add_indicator(indicator)

    return Indicator.from_dto(indicator_dto)


@indicators_router.get(
    "/indicator_value",
    response_model=IndicatorValue,
    status_code=status.HTTP_200_OK,
)
async def get_indicator_value_by_id(
    request: Request,
    indicator_id: int = Query(..., description="indicator id"),
    territory_id: int = Query(..., description="territory id"),
    date_type: DateType = Query(..., description="date type"),
    date_value: datetime = Query(..., description="time value"),
    value_type: ValueType = Query(..., description="value type"),
    information_source: str = Query(..., description="information source"),
) -> IndicatorValue:
    """Get indicator value for a given territory, date period, value type and source."""
    indicators_service: IndicatorsService = request.state.indicators_service

    indicator_value = await indicators_service.get_indicator_value_by_id(
        indicator_id, territory_id, date_type.value, date_value, value_type.value, information_source
    )

    return IndicatorValue.from_dto(indicator_value)


@indicators_router.post(
    "/indicator_value",
    response_model=IndicatorValue,
    status_code=status.HTTP_201_CREATED,
)
async def add_indicator_value(request: Request, indicator_value: IndicatorValuePost) -> IndicatorValue:
    """Add a new indicator value for a given territory and date period."""
    indicators_service: IndicatorsService = request.state.indicators_service

    indicator_value_dto = await indicators_service.add_indicator_value(indicator_value)

    return IndicatorValue.from_dto(indicator_value_dto)


@indicators_router.get(
    "/indicator/{indicator_id}/values",
    response_model=list[IndicatorValue],
    status_code=status.HTTP_200_OK,
)
async def get_indicator_values_by_id(
    request: Request,
    indicator_id: int = Path(..., description="indicator id"),
    territory_id: int | None = Query(None, description="territory id"),
    date_type: DateType = Query(None, description="date type"),
    date_value: datetime | None = Query(None, description="time value"),
    value_type: ValueType = Query(None, description="value type"),
    information_source: str | None = Query(None, description="information source"),
) -> list[IndicatorValue]:
    """Get indicator values by id, territory, date, value type and source could be specified in parameters.

    If parameters not specified there should be all available values for this indicator.
    """
    indicators_service: IndicatorsService = request.state.indicators_service

    date_type_field = date_type.value if date_type is not None else None
    value_type_field = value_type.value if value_type is not None else None

    indicator_values = await indicators_service.get_indicator_values_by_id(
        indicator_id, territory_id, date_type_field, date_value, value_type_field, information_source
    )

    return [IndicatorValue.from_dto(value) for value in indicator_values]
