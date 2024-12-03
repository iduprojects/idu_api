"""Indicator values projects-related endpoints are defined here."""

from fastapi import Depends, Path, Query, Request, Security
from fastapi.security import HTTPBearer
from geojson_pydantic.geometries import Geometry
from starlette import status

from idu_api.urban_api.dto.users import UserDTO
from idu_api.urban_api.handlers.v1.projects.routers import projects_router
from idu_api.urban_api.logic.projects import UserProjectService
from idu_api.urban_api.schemas import (
    HexagonWithIndicators,
    ProjectIndicatorValue,
    ProjectIndicatorValuePatch,
    ProjectIndicatorValuePost,
    ProjectIndicatorValuePut,
)
from idu_api.urban_api.schemas.geometries import Feature, GeoJSONResponse
from idu_api.urban_api.utils.auth_client import get_user


@projects_router.get(
    "/scenarios/{scenario_id}/indicators_values",
    response_model=list[ProjectIndicatorValue],
    status_code=status.HTTP_200_OK,
)
async def get_project_indicators_values_by_scenario_id(
    request: Request,
    scenario_id: int = Path(..., description="scenario identifier"),
    indicator_ids: str | None = Query(None, description="list id separated by commas"),
    indicators_group_id: int | None = Query(None, description="to filter by indicator group (identifier)"),
    territory_id: int | None = Query(None, description="to filter by territory identifier"),
    hexagon_id: int | None = Query(None, description="to filter by hexagon identifier"),
    user: UserDTO = Depends(get_user),
) -> list[ProjectIndicatorValue]:
    """Get project's indicators values for given scenario.

    It could be specified by indicator identifiers, indicators group, territory and hexagon.

    You must be the owner of the relevant project or the project must be publicly available.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    indicators = await user_project_service.get_projects_indicators_values_by_scenario_id(
        scenario_id,
        indicator_ids,
        indicators_group_id,
        territory_id,
        hexagon_id,
        user.id,
    )

    return [ProjectIndicatorValue.from_dto(indicator) for indicator in indicators]


@projects_router.get(
    "/scenarios/indicators_values/{indicator_value_id}",
    response_model=ProjectIndicatorValue,
    status_code=status.HTTP_200_OK,
)
async def get_project_indicator_value_by_id(
    request: Request,
    indicator_value_id: int = Path(..., description="indicator identifier"),
    user: UserDTO = Depends(get_user),
) -> ProjectIndicatorValue:
    """Get project's specific indicator values for given scenario.

    You must be the owner of the relevant project or the project must be publicly available.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    indicator_value = await user_project_service.get_project_indicator_value_by_id(indicator_value_id, user.id)

    return ProjectIndicatorValue.from_dto(indicator_value)


@projects_router.post(
    "/scenarios/indicators_values",
    response_model=ProjectIndicatorValue,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Security(HTTPBearer())],
)
async def add_project_indicator(
    request: Request, projects_indicator: ProjectIndicatorValuePost, user: UserDTO = Depends(get_user)
) -> ProjectIndicatorValue:
    """Add a new project's indicator value.

    You must be the owner of the relevant project.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    indicator = await user_project_service.add_projects_indicator_value(projects_indicator, user.id)

    return ProjectIndicatorValue.from_dto(indicator)


@projects_router.put(
    "/scenarios/indicators_values",
    response_model=ProjectIndicatorValue,
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def put_project_indicator(
    request: Request,
    projects_indicator: ProjectIndicatorValuePut,
    user: UserDTO = Depends(get_user),
) -> ProjectIndicatorValue:
    """Update project's indicator value if indicator value with such attributes already exists
    or create new indicator value for given scenario.

    You must be the owner of the relevant project.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    indicator = await user_project_service.put_projects_indicator_value(projects_indicator, user.id)

    return ProjectIndicatorValue.from_dto(indicator)


@projects_router.patch(
    "/scenarios/indicators_values/{indicator_value_id}",
    response_model=ProjectIndicatorValue,
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def patch_project_indicator(
    request: Request,
    projects_indicator: ProjectIndicatorValuePatch,
    indicator_value_id: int = Path(..., description="indicator value identifier"),
    user: UserDTO = Depends(get_user),
) -> ProjectIndicatorValue:
    """Update project's indicator value - only given fields.

    You must be the owner of the relevant project.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    indicator = await user_project_service.patch_projects_indicator_value(
        projects_indicator, indicator_value_id, user.id
    )

    return ProjectIndicatorValue.from_dto(indicator)


@projects_router.delete(
    "/scenarios/{scenario_id}/indicators_values",
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def delete_project_indicators_values_by_scenario_id(
    request: Request,
    scenario_id: int = Path(..., description="scenario identifier"),
    user: UserDTO = Depends(get_user),
) -> dict:
    """Delete all project's indicators values for given scenario.

    You must be the owner of the relevant project.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    return await user_project_service.delete_projects_indicators_values_by_scenario_id(scenario_id, user.id)


@projects_router.delete(
    "/scenarios/indicators_values/{indicator_value_id}",
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def delete_project_indicator_by_id(
    request: Request,
    indicator_value_id: int = Path(..., description="indicator value identifier"),
    user: UserDTO = Depends(get_user),
) -> dict:
    """Delete specific project's indicator values for given scenario.

    You must be the owner of the relevant project.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    return await user_project_service.delete_project_indicator_value_by_id(indicator_value_id, user.id)


@projects_router.get(
    "/scenarios/{scenario_id}/indicators_values/hexagons",
    response_model=GeoJSONResponse[Feature[Geometry, HexagonWithIndicators]],
    status_code=status.HTTP_200_OK,
)
async def get_hexagons_with_indicators_values_by_territory_id(
    request: Request,
    scenario_id: int = Path(..., description="scenario identifier", gt=0),
    indicator_ids: str | None = Query(None, description="list of identifiers separated by comma"),
    indicators_group_id: int | None = Query(None, description="to filter by indicator group (identifier)"),
    centers_only: bool = Query(False, description="display only centers"),
    user: UserDTO = Depends(get_user),
) -> GeoJSONResponse[Feature[Geometry, HexagonWithIndicators]]:
    """Get list of hexagons for a given territory and regional scenario
    with indicators values in properties in geojson format.

    You must be the owner of the relevant project or the project must be publicly available.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    hexagons = await user_project_service.get_hexagons_with_indicators_by_scenario_id(
        scenario_id, indicator_ids, indicators_group_id, user.id
    )

    return await GeoJSONResponse.from_list([hexagon.to_geojson_dict() for hexagon in hexagons], centers_only)
