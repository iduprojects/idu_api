"""Indicators handlers are defined here."""

from datetime import datetime

from fastapi import HTTPException, Path, Query, Request
from starlette import status

from idu_api.urban_api.logic.indicators import IndicatorsService
from idu_api.urban_api.schemas import (
    Indicator,
    IndicatorsGroup,
    IndicatorsGroupPost,
    IndicatorsPatch,
    IndicatorsPost,
    IndicatorsPut,
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
    "/indicators_groups",
    response_model=list[IndicatorsGroup],
    status_code=status.HTTP_200_OK,
)
async def get_indicators_groups(request: Request) -> list[IndicatorsGroup]:
    """Get all indicators groups."""
    indicators_service: IndicatorsService = request.state.indicators_service

    groups = await indicators_service.get_indicators_groups()

    return [IndicatorsGroup.from_dto(group) for group in groups]


@indicators_router.post(
    "/indicators_groups",
    response_model=IndicatorsGroup,
    status_code=status.HTTP_201_CREATED,
)
async def add_indicators_group(request: Request, indicators_group: IndicatorsGroupPost) -> IndicatorsGroup:
    """Add indicators group by name and list of indicators identifiers."""
    indicators_service: IndicatorsService = request.state.indicators_service

    group = await indicators_service.add_indicators_group(indicators_group)

    return IndicatorsGroup.from_dto(group)


@indicators_router.get(
    "/indicators_groups/{indicators_group_id}",
    response_model=list[Indicator],
    status_code=status.HTTP_200_OK,
)
async def get_indicators_by_group_id(
    request: Request,
    indicators_group_id: int = Path(..., description="indicators group identifier"),
) -> list[Indicator]:
    """Get all indicators by indicators group id."""
    indicators_service: IndicatorsService = request.state.indicators_service

    indicators = await indicators_service.get_indicators_by_group_id(indicators_group_id)

    return [Indicator.from_dto(indicator) for indicator in indicators]


@indicators_router.put(
    "/indicators_groups/{indicators_group_id}",
    response_model=IndicatorsGroup,
    status_code=status.HTTP_201_CREATED,
)
async def update_indicators_group(
    request: Request,
    indicators_group: IndicatorsGroupPost,
    indicators_group_id: int = Path(..., description="indicators group identifier"),
) -> IndicatorsGroup:
    """Update indicators group fully by name and list of indicators identifiers."""
    indicators_service: IndicatorsService = request.state.indicators_service

    group = await indicators_service.update_indicators_group(indicators_group, indicators_group_id)

    return IndicatorsGroup.from_dto(group)


@indicators_router.get(
    "/indicators_by_parent",
    response_model=list[Indicator],
    status_code=status.HTTP_200_OK,
)
async def get_indicators_by_parent(
    request: Request,
    parent_id: int | None = Query(
        None, description="Parent indicator id to filter, should be skipped to get top level indicators"
    ),
    parent_name: str | None = Query(
        None, description="Parent indicator name to filter, you need to pass the full name"
    ),
    name: str | None = Query(None, description="Filter by indicator name"),
    territory_id: int | None = Query(None, description="Filter by territory id (not including inner territories)"),
    get_all_subtree: bool = Query(False, description="Getting full subtree of indicators"),
) -> list[Indicator]:
    """Get a list of indicators by parent id or name.

    You can't pass parent_id and parent_name at the same time.
    """
    indicators_service: IndicatorsService = request.state.indicators_service

    if parent_id is not None and parent_name is not None:
        raise HTTPException(400, "you can't pass parent_id and parent_name at the same time")

    indicators = await indicators_service.get_indicators_by_parent(
        parent_id, parent_name, name, territory_id, get_all_subtree
    )

    return [Indicator.from_dto(indicator) for indicator in indicators]


@indicators_router.get(
    "/indicators/{indicator_id}",
    response_model=Indicator,
    status_code=status.HTTP_200_OK,
)
async def get_indicator_by_id(
    request: Request,
    indicator_id: int = Path(..., description="Getting indicator by id"),
) -> Indicator:
    """Get indicator."""
    indicators_service: IndicatorsService = request.state.indicators_service

    indicator = await indicators_service.get_indicator_by_id(indicator_id)

    return Indicator.from_dto(indicator)


@indicators_router.post(
    "/indicators",
    response_model=Indicator,
    status_code=status.HTTP_201_CREATED,
)
async def add_indicator(request: Request, indicator: IndicatorsPost) -> Indicator:
    """Add indicator."""
    indicators_service: IndicatorsService = request.state.indicators_service

    indicator_dto = await indicators_service.add_indicator(indicator)

    return Indicator.from_dto(indicator_dto)


@indicators_router.put(
    "/indicators/{indicator_id}",
    response_model=Indicator,
    status_code=status.HTTP_201_CREATED,
)
async def put_indicator(
    request: Request, indicator: IndicatorsPut, indicator_id: int = Path(..., description="indicator identifier")
) -> Indicator:
    """Update indicator by all its attributes."""
    indicators_service: IndicatorsService = request.state.indicators_service

    indicator_dto = await indicators_service.put_indicator(indicator_id, indicator)

    return Indicator.from_dto(indicator_dto)


@indicators_router.patch(
    "/indicators/{indicator_id}",
    response_model=Indicator,
    status_code=status.HTTP_201_CREATED,
)
async def patch_indicator(
    request: Request, indicator: IndicatorsPatch, indicator_id: int = Path(..., description="indicator identifier")
) -> Indicator:
    """Update indicator by only given attributes."""
    indicators_service: IndicatorsService = request.state.indicators_service

    indicator_dto = await indicators_service.patch_indicator(indicator_id, indicator)

    return Indicator.from_dto(indicator_dto)


@indicators_router.delete(
    "/indicators/{indicator_id}",
    response_model=dict,
    status_code=status.HTTP_201_CREATED,
)
async def delete_indicator(request: Request, indicator_id: int = Path(..., description="indicator identifier")) -> dict:
    """Delete indicator by id.

    It also removes all child elements of this indicator!!!
    """
    indicators_service: IndicatorsService = request.state.indicators_service

    return await indicators_service.delete_indicator(indicator_id)


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


@indicators_router.delete(
    "/indicator_value",
    response_model=dict,
    status_code=status.HTTP_201_CREATED,
)
async def delete_indicator_value(
    request: Request,
    indicator_id: int = Query(..., description="indicator id"),
    territory_id: int = Query(..., description="territory id"),
    date_type: DateType = Query(..., description="date type"),
    date_value: datetime = Query(..., description="time value"),
    value_type: ValueType = Query(..., description="value type"),
    information_source: str = Query(..., description="information source"),
) -> dict:
    """Delete indicator value by id."""
    indicators_service: IndicatorsService = request.state.indicators_service

    return await indicators_service.delete_indicator_value(
        indicator_id, territory_id, date_type.value, date_value, value_type.value, information_source
    )


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
