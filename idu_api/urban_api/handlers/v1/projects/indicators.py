"""Indicator values projects-related endpoints are defined here."""

from fastapi import Depends, HTTPException, Path, Query, Request, Security
from fastapi.security import HTTPBearer
from geojson_pydantic.geometries import Geometry
from otteroad import KafkaProducerClient
from starlette import status

from idu_api.urban_api.dto.users import UserDTO
from idu_api.urban_api.handlers.v1.projects.routers import projects_router
from idu_api.urban_api.logic.projects import UserProjectService
from idu_api.urban_api.schemas import (
    HexagonWithIndicators,
    OkResponse,
    ScenarioIndicatorValue,
    ScenarioIndicatorValuePatch,
    ScenarioIndicatorValuePost,
    ScenarioIndicatorValuePut,
)
from idu_api.urban_api.schemas.geometries import Feature, GeoJSONResponse
from idu_api.urban_api.utils.auth_client import get_user
from idu_api.urban_api.utils.broker import get_kafka_producer


@projects_router.get(
    "/scenarios/{scenario_id}/indicators_values",
    response_model=list[ScenarioIndicatorValue],
    status_code=status.HTTP_200_OK,
)
async def get_indicators_values_by_scenario_id(
    request: Request,
    scenario_id: int = Path(..., description="scenario identifier", gt=0),
    indicator_ids: str | None = Query(None, description="list id separated by commas"),
    indicators_group_id: int | None = Query(None, description="to filter by indicator group (identifier)", gt=0),
    territory_id: int | None = Query(None, description="to filter by territory identifier", gt=0),
    hexagon_id: int | None = Query(None, description="to filter by hexagon identifier", gt=0),
    user: UserDTO = Depends(get_user),
) -> list[ScenarioIndicatorValue]:
    """
    ## Get indicator values for a given scenario.

    ### Parameters:
    - **scenario_id** (int, Path): Unique identifier of the scenario.
    - **indicator_ids** (str | None, Query): Optional list of indicator identifiers separated by comma.
    - **indicators_group_id** (int | None, Query): Optional filter by indicator group identifier.
    - **territory_id** (int | None, Query): Optional filter by territory identifier.
    - **hexagon_id** (int | None, Query): Optional filter by hexagon identifier.

    ### Returns:
    - **list[ScenarioIndicatorValue]**: A list of scenario indicator values.

    ### Errors:
    - **400 Bad Request**: If the indicator_ids is specified in the wrong form.
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the scenario does not exist.

    ### Constraints:
    - The user must be the owner of the relevant project or the project must be publicly available.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    try:
        indicator_ids = {int(ind_id.strip()) for ind_id in indicator_ids.split(",")}
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Please, pass the indicator identifiers in the correct format separated by comma",
        ) from exc

    indicators = await user_project_service.get_scenario_indicators_values(
        scenario_id, indicator_ids, indicators_group_id, territory_id, hexagon_id, user
    )

    return [ScenarioIndicatorValue.from_dto(indicator) for indicator in indicators]


@projects_router.post(
    "/scenarios/{scenario_id}/indicators_values",
    response_model=ScenarioIndicatorValue,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Security(HTTPBearer())],
)
async def add_scenario_indicator_value(
    request: Request,
    indicator_value: ScenarioIndicatorValuePost,
    scenario_id: int = Path(..., description="scenario identifier", gt=0),
    user: UserDTO = Depends(get_user),
    kafka_producer: KafkaProducerClient = Depends(get_kafka_producer),
) -> ScenarioIndicatorValue:
    """
    ## Add a new indicator value for a given scenario.

    **NOTE:** After the indicator value is created, a corresponding message will be sent to the Kafka broker.

    ### Parameters:
    - **scenario_id** (int, Path): Unique identifier of the scenario.

    ### Body:
    - **indicator_value** (ScenarioIndicatorValuePost): The indicator value data to be added.

    ### Returns:
    - **ScenarioIndicatorValue**: The created indicator value.

    ### Errors:
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the scenario (or related entity) does not exist.
    - **409 Conflict**: If an indicator value with such attributes already exists.

    ### Constraints:
    - The user must be the owner of the relevant project.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    indicator = await user_project_service.add_scenario_indicator_value(
        indicator_value, scenario_id, user, kafka_producer
    )

    return ScenarioIndicatorValue.from_dto(indicator)


@projects_router.put(
    "/scenarios/{scenario_id}/indicators_values",
    response_model=ScenarioIndicatorValue,
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def put_scenario_indicator_value(
    request: Request,
    indicator_value: ScenarioIndicatorValuePut,
    scenario_id: int = Path(..., description="scenario identifier", gt=0),
    user: UserDTO = Depends(get_user),
    kafka_producer: KafkaProducerClient = Depends(get_kafka_producer),
) -> ScenarioIndicatorValue:
    """
    ## Update or create an indicator value for a given scenario.

    **NOTE 1:** If an indicator value with the specified attributes already exists, it will be updated.
    Otherwise, a new indicator value will be created.

    **NOTE 2:** After the indicator value is created, a corresponding message will be sent to the Kafka broker.

    ### Parameters:
    - **scenario_id** (int, Path): Unique identifier of the scenario.
    - **indicator_value** (ScenarioIndicatorValuePut, Body): The indicator value data to update or create.

    ### Returns:
    - **ScenarioIndicatorValue**: The updated or created indicator value.

    ### Errors:
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the scenario (or related entity) does not exist.

    ### Constraints:
    - The user must be the owner of the relevant project.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    indicator = await user_project_service.put_scenario_indicator_value(
        indicator_value, scenario_id, user, kafka_producer
    )

    return ScenarioIndicatorValue.from_dto(indicator)


@projects_router.patch(
    "/scenarios/{scenario_id}/indicators_values/{indicator_value_id}",
    response_model=ScenarioIndicatorValue,
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def patch_scenario_indicator_value(
    request: Request,
    indicator_value: ScenarioIndicatorValuePatch,
    scenario_id: int = Path(..., description="scenario identifier", gt=0),
    indicator_value_id: int = Path(..., description="indicator value identifier", gt=0),
    user: UserDTO = Depends(get_user),
    kafka_producer: KafkaProducerClient = Depends(get_kafka_producer),
) -> ScenarioIndicatorValue:
    """
    ## Update specific fields of an indicator value for a given scenario.

    **NOTE:** After the indicator value is updated, a corresponding message will be sent to the Kafka broker.

    ### Parameters:
    - **scenario_id** (int, Path): Unique identifier of the scenario.
    - **indicator_value_id** (int, Path): Unique identifier of the indicator value.
    - **indicator_value** (ScenarioIndicatorValuePatch, Body): The partial indicator value data to update.

    ### Returns:
    - **ScenarioIndicatorValue**: The updated indicator value.

    ### Errors:
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the scenario or indicator value (or related entity) does not exist.

    ### Constraints:
    - The user must be the owner of the relevant project.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    indicator = await user_project_service.patch_scenario_indicator_value(
        indicator_value, scenario_id, indicator_value_id, user, kafka_producer
    )

    return ScenarioIndicatorValue.from_dto(indicator)


@projects_router.delete(
    "/scenarios/{scenario_id}/indicators_values",
    response_model=OkResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def delete_indicators_values_by_scenario_id(
    request: Request,
    scenario_id: int = Path(..., description="scenario identifier", gt=0),
    user: UserDTO = Depends(get_user),
) -> OkResponse:
    """
    ## Delete all indicator values for a given scenario.

    ### Parameters:
    - **scenario_id** (int, Path): Unique identifier of the scenario.

    ### Returns:
    - **OkResponse**: A confirmation message of the deletion.

    ### Errors:
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the scenario does not exist.

    ### Constraints:
    - The user must be the owner of the relevant project.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    await user_project_service.delete_scenario_indicators_values_by_scenario_id(scenario_id, user)

    return OkResponse()


@projects_router.delete(
    "/scenarios/{scenario_id}/indicators_values/{indicator_value_id}",
    response_model=OkResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def delete_scenario_indicator_value_by_id(
    request: Request,
    scenario_id: int = Path(..., description="scenario identifier", gt=0),
    indicator_value_id: int = Path(..., description="indicator value identifier", gt=0),
    user: UserDTO = Depends(get_user),
) -> OkResponse:
    """
    ## Delete a specific indicator value for a given scenario.

    ### Parameters:
    - **scenario_id** (int, Path): Unique identifier of the scenario.
    - **indicator_value_id** (int, Path): Unique identifier of the indicator value.

    ### Returns:
    - **OkResponse**: A confirmation message of the deletion.

    ### Errors:
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the scenario or indicator value does not exist.

    ### Constraints:
    - The user must be the owner of the relevant project.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    await user_project_service.delete_scenario_indicator_value_by_id(scenario_id, indicator_value_id, user)

    return OkResponse()


@projects_router.get(
    "/scenarios/{scenario_id}/indicators_values/hexagons",
    response_model=GeoJSONResponse[Feature[Geometry, HexagonWithIndicators]],
    status_code=status.HTTP_200_OK,
)
async def get_hexagons_with_indicators_values_by_scenario_id(
    request: Request,
    scenario_id: int = Path(..., description="scenario identifier", gt=0),
    indicator_ids: str | None = Query(None, description="list of identifiers separated by comma"),
    indicators_group_id: int | None = Query(None, description="to filter by indicator group (identifier)"),
    centers_only: bool = Query(False, description="display only centers"),
    user: UserDTO = Depends(get_user),
) -> GeoJSONResponse[Feature[Geometry, HexagonWithIndicators]]:
    """
    ## Get hexagons with indicator values for a given scenario in GeoJSON format.

    ### Parameters:
    - **scenario_id** (int, Path): Unique identifier of the scenario.
    - **indicator_ids** (str | None, Query): Optional list of indicator identifiers separated by commas.
    - **indicators_group_id** (int | None, Query): Optional filter by indicator group identifier.
    - **centers_only** (bool, Query): If True, returns only center points of hexagons (default: false).

    ### Returns:
    - **GeoJSONResponse[Feature[Geometry, HexagonWithIndicators]]**: A GeoJSON response containing hexagons with indicator values.

    ### Errors:
    - **400 Bad Request**: If the indicator_ids is specified in the wrong form.
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the scenario does not exist.

    ### Constraints:
    - The user must be the owner of the relevant project or the project must be publicly available.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    try:
        indicator_ids = {int(ind_id.strip()) for ind_id in indicator_ids.split(",")}
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Please, pass the indicator identifiers in the correct format separated by comma",
        ) from exc

    hexagons = await user_project_service.get_hexagons_with_indicators_by_scenario_id(
        scenario_id, indicator_ids, indicators_group_id, user
    )

    return await GeoJSONResponse.from_list([hexagon.to_geojson_dict() for hexagon in hexagons], centers_only)


@projects_router.put(
    "/scenarios/{scenario_id}/all_indicators_values/",
    response_model=OkResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def update_all_indicators_values_by_scenario_id(
    request: Request,
    scenario_id: int = Path(..., description="scenario identifier", gt=0),
    user: UserDTO = Depends(get_user),
) -> OkResponse:
    """
    ## Update all indicator values for a given scenario.

    ### Parameters:
    - **scenario_id** (int, Path): Unique identifier of the scenario.

    ### Returns:
    - **OkResponse**: A confirmation message of the update.

    ### Errors:
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the scenario does not exist.

    ### Constraints:
    - The user must be the owner of the relevant project.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    await user_project_service.update_all_indicators_values_by_scenario_id(scenario_id, user)

    return OkResponse()


@projects_router.post(
    "/scenarios/indicators_values",
    response_model=ScenarioIndicatorValue,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Security(HTTPBearer())],
    deprecated=True,
)
async def add_scenario_indicator_deprecated(
    request: Request, indicator_value: ScenarioIndicatorValuePost, user: UserDTO = Depends(get_user)
) -> ScenarioIndicatorValue:
    """
    ## Add a new indicator value for a given scenario.

    **WARNING:** This method has been deprecated since version 0.34.0 and will be removed in version 1.0.
    Instead, use **POST /scenarios/{scenario_id}/indicators_values**.

    ### Parameters:
    - **indicator_value** (ScenarioIndicatorValuePost, Body): The indicator value data to be added.

    ### Returns:
    - **ScenarioIndicatorValue**: The created indicator value.

    ### Errors:
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the scenario (or related entity) does not exist.
    - **409 Conflict**: If an indicator value with such attributes already exists.

    ### Constraints:
    - The user must be the owner of the relevant project.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    indicator = await user_project_service.add_scenario_indicator_value(
        indicator_value, indicator_value.scenario_id, user
    )

    return ScenarioIndicatorValue.from_dto(indicator)


@projects_router.put(
    "/scenarios/indicators_values",
    response_model=ScenarioIndicatorValue,
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
    deprecated=True,
)
async def put_scenario_indicator_deprecated(
    request: Request,
    indicator_value: ScenarioIndicatorValuePut,
    user: UserDTO = Depends(get_user),
) -> ScenarioIndicatorValue:
    """
    ## Update or create an indicator value for a given scenario.

    **WARNING:** This method has been deprecated since version 0.34.0 and will be removed in version 1.0.
    Instead, use **PUT /scenarios/{scenario_id}/indicators_values**.

    **NOTE:** If an indicator value with the specified attributes already exists, it will be updated.
    Otherwise, a new indicator value will be created.

    ### Parameters:
    - **indicator_value** (ScenarioIndicatorValuePut, Body): The indicator value data to update or create.

    ### Returns:
    - **ScenarioIndicatorValue**: The updated or created indicator value.

    ### Errors:
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the scenario (or related entity) does not exist.

    ### Constraints:
    - The user must be the owner of the relevant project.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    indicator = await user_project_service.put_scenario_indicator_value(
        indicator_value, indicator_value.scenario_id, user
    )

    return ScenarioIndicatorValue.from_dto(indicator)


@projects_router.patch(
    "/scenarios/indicators_values/{indicator_value_id}",
    response_model=ScenarioIndicatorValue,
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
    deprecated=True,
)
async def patch_scenario_indicator_deprecated(
    request: Request,
    indicator_value: ScenarioIndicatorValuePatch,
    indicator_value_id: int = Path(..., description="indicator value identifier", gt=0),
    user: UserDTO = Depends(get_user),
) -> ScenarioIndicatorValue:
    """
    ## Update specific fields of an indicator value for a given scenario.

    **WARNING:** This method has been deprecated since version 0.34.0 and will be removed in version 1.0.
    Instead, use **PATCH /scenarios/{scenario_id}/indicators_values/{indicator_value_id}**.

    ### Parameters:
    - **indicator_value_id** (int, Path): Unique identifier of the indicator value.
    - **indicator_value** (ScenarioIndicatorValuePatch, Body): The partial indicator value data to update.

    ### Returns:
    - **ScenarioIndicatorValue**: The updated indicator value.

    ### Errors:
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the scenario or indicator value (or related entity) does not exist.

    ### Constraints:
    - The user must be the owner of the relevant project.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    indicator = await user_project_service.patch_scenario_indicator_value(
        indicator_value, None, indicator_value_id, user
    )

    return ScenarioIndicatorValue.from_dto(indicator)


@projects_router.delete(
    "/scenarios/indicators_values/{indicator_value_id}",
    response_model=OkResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
    deprecated=True,
)
async def delete_scenario_indicator_by_id_deprecated(
    request: Request,
    indicator_value_id: int = Path(..., description="indicator value identifier", gt=0),
    user: UserDTO = Depends(get_user),
) -> OkResponse:
    """
    ## Delete a specific indicator value for a given scenario.

    **WARNING:** This method has been deprecated since version 0.34.0 and will be removed in version 1.0.
    Instead, use **DELETE /scenarios/{scenario_id}/indicators_values/{indicator_value_id}**.

    ### Parameters:
    - **indicator_value_id** (int, Path): Unique identifier of the indicator value.

    ### Returns:
    - **OkResponse**: A confirmation message of the deletion.

    ### Errors:
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the scenario or indicator value does not exist.

    ### Constraints:
    - The user must be the owner of the relevant project.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    await user_project_service.delete_scenario_indicator_value_by_id(None, indicator_value_id, user)

    return OkResponse()
