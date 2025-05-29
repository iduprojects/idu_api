"""Indicators handlers are defined here."""

from datetime import date

from fastapi import Depends, HTTPException, Path, Query, Request
from otteroad import KafkaProducerClient
from starlette import status

from idu_api.urban_api.logic.indicators import IndicatorsService
from idu_api.urban_api.schemas import (
    Indicator,
    IndicatorPost,
    IndicatorPut,
    IndicatorsGroup,
    IndicatorsGroupPost,
    IndicatorsPatch,
    IndicatorValue,
    IndicatorValuePost,
    IndicatorValuePut,
    MeasurementUnit,
    MeasurementUnitPost,
    OkResponse,
)
from idu_api.urban_api.schemas.enums import DateType, ValueType

from ...utils.broker import get_kafka_producer
from .routers import indicators_router


@indicators_router.get(
    "/measurement_units",
    response_model=list[MeasurementUnit],
    status_code=status.HTTP_200_OK,
)
async def get_measurement_units(request: Request) -> list[MeasurementUnit]:
    """
    ## Get the list of measurement units.

    ### Returns:
    - **list[MeasurementUnit]**: A list of measurement units.
    """
    indicators_service: IndicatorsService = request.state.indicators_service

    measurement_units = await indicators_service.get_measurement_units()

    return [MeasurementUnit.from_dto(measurement_unit) for measurement_unit in measurement_units]


@indicators_router.post(
    "/measurement_units",
    response_model=MeasurementUnit,
    status_code=status.HTTP_201_CREATED,
)
async def add_measurement_unit(request: Request, measurement_unit: MeasurementUnitPost) -> MeasurementUnit:
    """
    ## Create a new measurement unit.

    ### Parameters:
    - **measurement_unit** (MeasurementUnitPost, Body): Data for the new measurement unit.

    ### Returns:
    - **MeasurementUnit**: The created measurement unit.

    ### Errors:
    - **409 Conflict**: If a measurement unit with the such name already exists.
    """
    indicators_service: IndicatorsService = request.state.indicators_service

    unit = await indicators_service.add_measurement_unit(measurement_unit)

    return MeasurementUnit.from_dto(unit)


@indicators_router.get(
    "/indicators_groups",
    response_model=list[IndicatorsGroup],
    status_code=status.HTTP_200_OK,
)
async def get_indicators_groups(request: Request) -> list[IndicatorsGroup]:
    """
    ## Get all indicators groups.

    ### Returns:
    - **list[IndicatorsGroup]**: A list of indicators groups.
    """
    indicators_service: IndicatorsService = request.state.indicators_service

    groups = await indicators_service.get_indicators_groups()

    return [IndicatorsGroup.from_dto(group) for group in groups]


@indicators_router.post(
    "/indicators_groups",
    response_model=IndicatorsGroup,
    status_code=status.HTTP_201_CREATED,
)
async def add_indicators_group(request: Request, indicators_group: IndicatorsGroupPost) -> IndicatorsGroup:
    """
    ## Create a new indicators group.

    ### Parameters:
    - **indicators_group** (IndicatorsGroupPost, Body): Data for the new indicators group, including name and list of indicators.

    ### Returns:
    - **IndicatorsGroup**: The created indicators group.

    ### Errors:
    - **404 Not Found**: If the given indicators do not exist.
    - **409 Conflict**: If an indicators group with the such name already exists.
    """
    indicators_service: IndicatorsService = request.state.indicators_service

    group = await indicators_service.add_indicators_group(indicators_group)

    return IndicatorsGroup.from_dto(group)


@indicators_router.put(
    "/indicators_groups",
    response_model=IndicatorsGroup,
    status_code=status.HTTP_200_OK,
)
async def update_indicators_group(
    request: Request,
    indicators_group: IndicatorsGroupPost,
) -> IndicatorsGroup:
    """
    ## Update or create an indicators group.

    **NOTE:** If an indicators group with the such name already exists, it will be updated (change all indicators).
    Otherwise, a new indicators group will be created.

    ### Parameters:
    - **indicators_group** (IndicatorsGroupPost, Body): Updated data for the indicators group, including name and list of indicators.

    ### Returns:
    - **IndicatorsGroup**: The updated indicators group.

    ### Errors:
    - **404 Not Found**: If the given indicators do not exist.
    """
    indicators_service: IndicatorsService = request.state.indicators_service

    group = await indicators_service.update_indicators_group(indicators_group)

    return IndicatorsGroup.from_dto(group)


@indicators_router.get(
    "/indicators_groups/{indicators_group_id}",
    response_model=list[Indicator],
    status_code=status.HTTP_200_OK,
)
async def get_indicators_by_group_id(
    request: Request,
    indicators_group_id: int = Path(..., description="indicators group identifier", gt=0),
) -> list[Indicator]:
    """
    ## Get all indicators within a specific indicators group.

    ### Parameters:
    - **indicators_group_id** (int, Path): Unique identifier of the indicators group.

    ### Returns:
    - **list[Indicator]**: A list of indicators within the specified group.

    ### Errors:
    - **404 Not Found**: If the indicators group does not exist.
    """
    indicators_service: IndicatorsService = request.state.indicators_service

    indicators = await indicators_service.get_indicators_by_group_id(indicators_group_id)

    return [Indicator.from_dto(indicator) for indicator in indicators]


@indicators_router.get(
    "/indicators_by_parent",
    response_model=list[Indicator],
    status_code=status.HTTP_200_OK,
)
async def get_indicators_by_parent(
    request: Request,
    parent_id: int | None = Query(
        None, description="parent indicator id to filter, should be skipped to get top level indicators", gt=0
    ),
    parent_name: str | None = Query(
        None, description="parent indicator name to filter, you need to pass the full name"
    ),
    name: str | None = Query(None, description="filter by indicator name"),
    territory_id: int | None = Query(None, description="filter by territory id (not including inner territories)"),
    service_type_id: int | None = Query(None, description="filter by service type id"),
    physical_object_type_id: int | None = Query(None, description="filter by physical object type id"),
    get_all_subtree: bool = Query(False, description="getting full subtree of indicators"),
) -> list[Indicator]:
    """
    ## Get a list of indicators by parent identifier or name.

    **WARNING:** You only can get indicators by parent identifier or parent name.

    ### Parameters:
    - **parent_id** (int | None, Query): Unique identifier of the parent indicator. If skipped, it returns the highest level indicators.
    - **parent_name** (str | None, Query): Full name of the parent indicator.
    - **name** (str | None, Query): Filters results by indicator name.
    - **territory_id** (int | None, Query): Filters results by territory identifier.
    - **service_type_id** (int | None, Query): Filters results by service type identifier.
    - **physical_object_type_id** (int | None, Query): Filters results by physical object type identifier.
    - **get_all_subtree** (bool, Query): If True, gets the entire subtree of indicators.

    ### Returns:
    - **list[Indicator]**: A list of indicators matching the filters.

    ### Errors:
    - **400 Bad Request**: If both parent_id and parent_name or service_type_id and physical_object_type_id are provided.
    - **404 Not Found**: If the indicator does not exist.
    """
    indicators_service: IndicatorsService = request.state.indicators_service

    if parent_id is not None and parent_name is not None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Please, choose either parent_id or parent_name",
        )

    indicators = await indicators_service.get_indicators_by_parent(
        parent_id, parent_name, name, territory_id, service_type_id, physical_object_type_id, get_all_subtree
    )

    return [Indicator.from_dto(indicator) for indicator in indicators]


@indicators_router.get(
    "/indicators/{indicator_id}",
    response_model=Indicator,
    status_code=status.HTTP_200_OK,
)
async def get_indicator_by_id(
    request: Request,
    indicator_id: int = Path(..., description="indicator identifier", gt=0),
) -> Indicator:
    """
    ## Get an indicator by its identifier.

    ### Parameters:
    - **indicator_id** (int, Path): Unique identifier of the indicator.

    ### Returns:
    - **Indicator**: The requested indicator.

    ### Errors:
    - **404 Not Found**: If the indicator does not exist.
    """
    indicators_service: IndicatorsService = request.state.indicators_service

    indicator = await indicators_service.get_indicator_by_id(indicator_id)

    return Indicator.from_dto(indicator)


@indicators_router.post(
    "/indicators",
    response_model=Indicator,
    status_code=status.HTTP_201_CREATED,
)
async def add_indicator(request: Request, indicator: IndicatorPost) -> Indicator:
    """
    ## Create a new indicator.

    ### Parameters:
    - **indicator** (IndicatorPost, Body): Data for the new indicator.

    ### Returns:
    - **Indicator**: The created indicator.

    ### Errors:
    - **404 Not Found**: If related entity does not exist.
    - **409 Conflict**: If an indicator with the such name already exists.
    """
    indicators_service: IndicatorsService = request.state.indicators_service

    indicator_dto = await indicators_service.add_indicator(indicator)

    return Indicator.from_dto(indicator_dto)


@indicators_router.put(
    "/indicators",
    response_model=Indicator,
    status_code=status.HTTP_200_OK,
)
async def put_indicator(request: Request, indicator: IndicatorPut) -> Indicator:
    """
    ## Update or create an indicator.

    **NOTE:** If an indicator with the such name already exists, it will be updated.
    Otherwise, a new indicator will be created.

    ### Parameters:
    - **indicator** (IndicatorPut, Body): Data for updating or creating an indicator.

    ### Returns:
    - **Indicator**: The updated or created indicator.

    ### Errors:
    - **404 Not Found**: If the indicator (or related entity) does not exist.
    """
    indicators_service: IndicatorsService = request.state.indicators_service

    indicator_dto = await indicators_service.put_indicator(indicator)

    return Indicator.from_dto(indicator_dto)


@indicators_router.patch(
    "/indicators/{indicator_id}",
    response_model=Indicator,
    status_code=status.HTTP_200_OK,
)
async def patch_indicator(
    request: Request,
    indicator: IndicatorsPatch,
    indicator_id: int = Path(..., description="indicator identifier", gt=0),
) -> Indicator:
    """
    ## Partially update an indicator.

    ### Parameters:
    - **indicator_id** (int, Path): Unique identifier of the indicator.
    - **indicator** (IndicatorsPatch, Body): Fields to update in the indicator.

    ### Returns:
    - **Indicator**: The updated indicator with modified attributes.

    ### Errors:
    - **404 Not Found**: If the indicator (or related entity) does not exist.
    """
    indicators_service: IndicatorsService = request.state.indicators_service

    indicator_dto = await indicators_service.patch_indicator(indicator_id, indicator)

    return Indicator.from_dto(indicator_dto)


@indicators_router.delete(
    "/indicators/{indicator_id}",
    response_model=OkResponse,
    status_code=status.HTTP_200_OK,
)
async def delete_indicator(
    request: Request, indicator_id: int = Path(..., description="indicator identifier", gt=0)
) -> OkResponse:
    """
    ## Delete an indicator by its identifier.

    **WARNING:** This operation also removes all child elements of the indicator.

    ### Parameters:
    - **indicator_id** (int, Path): Unique identifier of the indicator.

    ### Returns:
    - **OkResponse**: A confirmation message of the deletion.

    ### Errors:
    - **404 Not Found**: If the indicator does not exist.
    """
    indicators_service: IndicatorsService = request.state.indicators_service

    await indicators_service.delete_indicator(indicator_id)

    return OkResponse()


@indicators_router.get(
    "/indicator_value/{indicator_value_id}",
    response_model=IndicatorValue,
    status_code=status.HTTP_200_OK,
)
async def get_indicator_value_by_id(
    request: Request,
    indicator_value_id: int = Path(..., description="indicator value identifier", gt=0),
) -> IndicatorValue:
    """
    ## Get an indicator value by identifier.

    ### Parameters:
    - **indicator_value_id** (int, Path): Unique identifier of the indicator value.

    ### Returns:
    - **IndicatorValue**: The requested indicator value.

    ### Errors:
    - **404 Not Found**: If the indicator value does not exist.
    """
    indicators_service: IndicatorsService = request.state.indicators_service

    indicator_value = await indicators_service.get_indicator_value_by_id(indicator_value_id)

    return IndicatorValue.from_dto(indicator_value)


@indicators_router.post(
    "/indicator_value",
    response_model=IndicatorValue,
    status_code=status.HTTP_201_CREATED,
)
async def add_indicator_value(
    request: Request,
    indicator_value: IndicatorValuePost,
    kafka_producer: KafkaProducerClient = Depends(get_kafka_producer),
) -> IndicatorValue:
    """
    ## Create a new indicator value.

    **NOTE:** After the indicator value is created, a corresponding message will be sent to the Kafka broker.

    ### Parameters:
    - **indicator_value** (IndicatorValuePost, Body): Data for the new indicator value.

    ### Returns:
    - **IndicatorValue**: The created indicator value.

    ### Errors:
    - **404 Not Found**: If related entities do not exist.
    - **409 Conflict**: If an indicator value with the such parameters already exists.
    """
    indicators_service: IndicatorsService = request.state.indicators_service

    indicator_value_dto = await indicators_service.add_indicator_value(indicator_value, kafka_producer)

    return IndicatorValue.from_dto(indicator_value_dto)


@indicators_router.put(
    "/indicator_value",
    response_model=IndicatorValue,
    status_code=status.HTTP_200_OK,
)
async def put_indicator_value(
    request: Request,
    indicator_value: IndicatorValuePut,
    kafka_producer: KafkaProducerClient = Depends(get_kafka_producer),
) -> IndicatorValue:
    """
    ## Update or create an indicator value.

    **NOTE 1:** If an indicator value with the specified attributes already exists, it will be updated.
    Otherwise, a new indicator value will be created.

    **NOTE 2:** After the indicator value is created, a corresponding message will be sent to the Kafka broker.

    ### Parameters:
    - **indicator_value** (IndicatorValuePut, Body): Data for updating or creating an indicator.

    ### Returns:
    - **IndicatorValue**: The updated or created indicator value.

    ### Errors:
    - **404 Not Found**: If related entities do not exist.
    """
    indicators_service: IndicatorsService = request.state.indicators_service

    indicator_value_dto = await indicators_service.put_indicator_value(indicator_value, kafka_producer)

    return IndicatorValue.from_dto(indicator_value_dto)


@indicators_router.delete(
    "/indicator_value/{indicator_value_id}",
    response_model=OkResponse,
    status_code=status.HTTP_200_OK,
)
async def delete_indicator_value(
    request: Request,
    indicator_value_id: int = Path(..., description="indicator value identifier", gt=0),
) -> OkResponse:
    """
    ## Delete an indicator value by identifier.

    ### Parameters:
    - **indicator_value_id** (int, Path): Unique identifier of the indicator value.

    ### Returns:
    - **OkResponse**: A confirmation message of the deletion.

    ### Errors:
    - **404 Not Found**: If the indicator value does not exist.
    """
    indicators_service: IndicatorsService = request.state.indicators_service

    await indicators_service.delete_indicator_value(indicator_value_id)

    return OkResponse()


@indicators_router.get(
    "/indicator/{indicator_id}/values",
    response_model=list[IndicatorValue],
    status_code=status.HTTP_200_OK,
)
async def get_indicator_values_by_id(
    request: Request,
    indicator_id: int = Path(..., description="indicator identifier", gt=0),
    territory_id: int | None = Query(None, description="territory identifier", gt=0),
    date_type: DateType = Query(None, description="date type"),
    date_value: date | None = Query(None, description="time value"),
    value_type: ValueType = Query(None, description="value type"),
    information_source: str | None = Query(None, description="information source"),
) -> list[IndicatorValue]:
    """
    ## Get all indicator values by indicator identifier with optional filtering.

    ### Parameters:
    - **indicator_id** (int, Path): Unique identifier of the indicator.
    - **territory_id** (int | None, Query): Filters results by territory.
    - **date_type** (DateType, Query): Filters results bye date type.
    - **date_value** (date | None, Query): Filters results by time period.
    - **value_type** (ValueType, Query): Filters results by value type.
    - **information_source** (str | None, Query): Filters results by a case-insensitive substring match.

    ### Returns:
    - **list[IndicatorValue]**: A list of indicator values matching the filters.

    ### Errors:
    - **404 Not Found**: If the indicator does not exist.
    """
    indicators_service: IndicatorsService = request.state.indicators_service

    date_type_field = date_type.value if date_type is not None else None
    value_type_field = value_type.value if value_type is not None else None

    indicator_values = await indicators_service.get_indicator_values_by_id(
        indicator_id, territory_id, date_type_field, date_value, value_type_field, information_source
    )

    return [IndicatorValue.from_dto(value) for value in indicator_values]
