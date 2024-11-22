"""Indicator values projects-related endpoints are defined here."""

from fastapi import Depends, Path, Query, Request, Security
from fastapi.security import HTTPBearer
from starlette import status

from idu_api.urban_api.dto.users import UserDTO
from idu_api.urban_api.handlers.v1.projects.routers import projects_router
from idu_api.urban_api.logic.projects import UserProjectService
from idu_api.urban_api.schemas import (
    ProjectsIndicatorValue,
    ProjectsIndicatorValuePatch,
    ProjectsIndicatorValuePost,
    ProjectsIndicatorValuePut,
)
from idu_api.urban_api.utils.auth_client import get_user


@projects_router.get(
    "/scenarios/{scenario_id}/indicators_values",
    response_model=list[ProjectsIndicatorValue],
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def get_project_indicators_values_by_scenario_id(
    request: Request,
    scenario_id: int = Path(..., description="scenario identifier"),
    indicator_ids: str | None = Query(None, description="list id separated by commas"),
    indicators_group_id: int | None = Query(None, description="to filter by indicator group (identifier)"),
    territory_id: int | None = Query(None, description="to filter by territory identifier"),
    hexagon_id: int | None = Query(None, description="to filter by hexagon identifier"),
    user: UserDTO = Depends(get_user),
) -> list[ProjectsIndicatorValue]:
    """Get project's indicators values for given scenario
    if relevant project is public or if you're the project owner.

    It could be specified by indicator identifiers, indicators group, territory and hexagon."""
    user_project_service: UserProjectService = request.state.user_project_service

    indicators = await user_project_service.get_projects_indicators_values_by_scenario_id(
        scenario_id,
        indicator_ids,
        indicators_group_id,
        territory_id,
        hexagon_id,
        user.id,
    )

    return [ProjectsIndicatorValue.from_dto(indicator) for indicator in indicators]


@projects_router.get(
    "/scenarios/indicator_values/{indicators_value_id}",
    response_model=ProjectsIndicatorValue,
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def get_project_indicator_value_by_id(
    request: Request,
    indicator_value_id: int = Path(..., description="indicator identifier"),
    user: UserDTO = Depends(get_user),
) -> ProjectsIndicatorValue:
    """Get project's specific indicator values for given scenario
    if relevant project is public or if you're the project owner."""
    user_project_service: UserProjectService = request.state.user_project_service

    indicator_value = await user_project_service.get_project_indicator_value_by_id(indicator_value_id, user.id)

    return ProjectsIndicatorValue.from_dto(indicator_value)


@projects_router.post(
    "/scenarios/indicators_values",
    response_model=ProjectsIndicatorValue,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Security(HTTPBearer())],
)
async def add_project_indicator(
    request: Request, projects_indicator: ProjectsIndicatorValuePost, user: UserDTO = Depends(get_user)
) -> ProjectsIndicatorValue:
    """Add a new project's indicator value."""
    user_project_service: UserProjectService = request.state.user_project_service

    indicator = await user_project_service.add_projects_indicator_value(projects_indicator, user.id)

    return ProjectsIndicatorValue.from_dto(indicator)


@projects_router.put(
    "/scenarios/indicators_values/{indicators_value_id}",
    response_model=ProjectsIndicatorValue,
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def put_project_indicator(
    request: Request,
    projects_indicator: ProjectsIndicatorValuePut,
    indicator_value_id: int = Path(..., description="indicator value identifier"),
    user: UserDTO = Depends(get_user),
) -> ProjectsIndicatorValue:
    """Put project's indicator value."""
    user_project_service: UserProjectService = request.state.user_project_service

    indicator = await user_project_service.put_projects_indicator_value(projects_indicator, indicator_value_id, user.id)

    return ProjectsIndicatorValue.from_dto(indicator)


@projects_router.patch(
    "/scenarios/indicators_values/{indicator_value_id}",
    response_model=ProjectsIndicatorValue,
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def patch_project_indicator(
    request: Request,
    projects_indicator: ProjectsIndicatorValuePatch,
    indicator_value_id: int = Path(..., description="indicator value identifier"),
    user: UserDTO = Depends(get_user),
) -> ProjectsIndicatorValue:
    """Patch project's indicator value."""
    user_project_service: UserProjectService = request.state.user_project_service

    indicator = await user_project_service.patch_projects_indicator_value(
        projects_indicator, indicator_value_id, user.id
    )

    return ProjectsIndicatorValue.from_dto(indicator)


@projects_router.delete(
    "/scenarios/{scenario_id}/indicators_values",
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def delete_projects_indicators_values_by_scenario_id(
    request: Request,
    scenario_id: int = Path(..., description="scenario identifier"),
    user: UserDTO = Depends(get_user),
) -> dict:
    """Delete all project's indicators values for given scenario if you're the project owner."""
    user_project_service: UserProjectService = request.state.user_project_service

    return await user_project_service.delete_projects_indicators_values_by_scenario_id(scenario_id, user.id)


@projects_router.delete(
    "/scenarios/indicators_values/{indicator_value_id}",
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def delete_specific_projects_indicator(
    request: Request,
    indicator_value_id: int = Path(..., description="indicator value identifier"),
    user: UserDTO = Depends(get_user),
) -> dict:
    """Delete specific project's indicator values for given scenario if you're the project owner."""
    user_project_service: UserProjectService = request.state.user_project_service

    return await user_project_service.delete_project_indicator_value_by_id(indicator_value_id, user.id)
