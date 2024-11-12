"""Indicator values projects-related endpoints are defined here."""

from fastapi import Depends, Path, Request, Security
from fastapi.security import HTTPBearer
from starlette import status

from idu_api.urban_api.dto.users import UserDTO
from idu_api.urban_api.handlers.v1.projects.routers import projects_router
from idu_api.urban_api.logic.projects import UserProjectService
from idu_api.urban_api.schemas import (
    ProjectsIndicatorValue,
    ProjectsIndicatorValuePost,
)
from idu_api.urban_api.utils.auth_client import get_user


@projects_router.get(
    "/scenarios/{scenario_id}/indicators_values",
    response_model=list[ProjectsIndicatorValue],
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def get_all_projects_indicators(
    request: Request,
    scenario_id: int = Path(..., description="scenario identifier"),
    user: UserDTO = Depends(get_user),
) -> list[ProjectsIndicatorValue]:
    """Get project's indicators values for given scenario
    if relevant project is public or if you're the project owner."""
    user_project_service: UserProjectService = request.state.user_project_service

    indicators = await user_project_service.get_all_projects_indicators_values(scenario_id, user.id)

    return [ProjectsIndicatorValue.from_dto(indicator) for indicator in indicators]


@projects_router.get(
    "/scenarios/{scenario_id}/indicator_values/{indicator_id}",
    response_model=list[ProjectsIndicatorValue],
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def get_specific_projects_indicator_values(
    request: Request,
    scenario_id: int = Path(..., description="scenario identifier"),
    indicator_id: int = Path(..., description="indicator identifier"),
    user: UserDTO = Depends(get_user),
) -> list[ProjectsIndicatorValue]:
    """Get project's specific indicator values for given scenario
    if relevant project is public or if you're the project owner."""
    user_project_service: UserProjectService = request.state.user_project_service

    indicator_values = await user_project_service.get_specific_projects_indicator_values(
        scenario_id, indicator_id, user.id
    )

    return [ProjectsIndicatorValue.from_dto(indicator) for indicator in indicator_values]


@projects_router.post(
    "/scenarios/{scenario_id}/indicators_values",
    response_model=ProjectsIndicatorValue,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Security(HTTPBearer())],
)
async def post_projects_indicator(
    request: Request, projects_indicator: ProjectsIndicatorValuePost, user: UserDTO = Depends(get_user)
) -> ProjectsIndicatorValue:
    """Add a new project's indicator value."""
    user_project_service: UserProjectService = request.state.user_project_service

    indicator = await user_project_service.add_projects_indicator_value(projects_indicator, user.id)

    return ProjectsIndicatorValue.from_dto(indicator)


@projects_router.delete(
    "/scenarios/{scenario_id}/indicators_values",
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def delete_all_projects_indicators(
    request: Request,
    scenario_id: int = Path(..., description="scenario identifier"),
    user: UserDTO = Depends(get_user),
) -> dict:
    """Delete all project's indicators values for given scenario if you're the project owner."""
    user_project_service: UserProjectService = request.state.user_project_service

    return await user_project_service.delete_all_projects_indicators_values(scenario_id, user.id)


@projects_router.delete(
    "/scenarios/{scenario_id}/indicator_values/{indicator_id}",
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def delete_specific_projects_indicator(
    request: Request,
    scenario_id: int = Path(..., description="scenario identifier"),
    indicator_id: int = Path(..., description="indicator identifier"),
    user: UserDTO = Depends(get_user),
) -> dict:
    """Delete specific project's indicator values for given scenario if you're the project owner."""
    user_project_service: UserProjectService = request.state.user_project_service

    return await user_project_service.delete_specific_projects_indicator_values(scenario_id, indicator_id, user.id)
