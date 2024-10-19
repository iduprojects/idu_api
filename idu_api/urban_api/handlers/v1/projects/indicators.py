from fastapi import Depends, Query, Request
from starlette import status

from idu_api.urban_api.dto.users import UserDTO
from idu_api.urban_api.handlers.v1.projects.routers import projects_router
from idu_api.urban_api.logic.projects import UserProjectService
from idu_api.urban_api.schemas import (
    ProjectsIndicator,
    ProjectsIndicatorPatch,
    ProjectsIndicatorPost,
    ProjectsIndicatorPut,
)
from idu_api.urban_api.utils.dependencies import user_dependency


@projects_router.get(
    "/projects_indicators/all",
    response_model=list[ProjectsIndicator],
    status_code=status.HTTP_200_OK,
)
async def get_all_projects_indicators(
    request: Request,
    scenario_id: int = Query(..., description="scenario identifier"),
    user: UserDTO = Depends(user_dependency),
) -> list[ProjectsIndicator]:
    """Get project's indicators for given scenario if relevant project is public or if you're the project owner."""
    user_project_service: UserProjectService = request.state.user_project_service

    indicators = await user_project_service.get_all_projects_indicators(scenario_id, user.id)

    return [ProjectsIndicator.from_dto(indicator) for indicator in indicators]


@projects_router.get(
    "/projects_indicators",
    response_model=ProjectsIndicator,
    status_code=status.HTTP_200_OK,
)
async def get_specific_projects_indicator(
    request: Request,
    scenario_id: int = Query(..., description="scenario identifier"),
    indicator_id: int = Query(..., description="indicator identifier"),
    user: UserDTO = Depends(user_dependency),
) -> ProjectsIndicator:
    """Get project's specific indicator for given scenario
    if relevant project is public or if you're the project owner."""
    user_project_service: UserProjectService = request.state.user_project_service

    indicator = await user_project_service.get_specific_projects_indicator(scenario_id, indicator_id, user.id)

    return ProjectsIndicator.from_dto(indicator)


@projects_router.post(
    "/projects_indicators",
    response_model=ProjectsIndicator,
    status_code=status.HTTP_201_CREATED,
)
async def post_projects_indicator(
    request: Request, projects_indicator: ProjectsIndicatorPost, user: UserDTO = Depends(user_dependency)
) -> ProjectsIndicator:
    """Add a new project's indicator."""
    user_project_service: UserProjectService = request.state.user_project_service

    indicator = await user_project_service.add_projects_indicator(projects_indicator, user.id)

    return ProjectsIndicator.from_dto(indicator)


@projects_router.put(
    "/projects_indicators",
    response_model=ProjectsIndicator,
    status_code=status.HTTP_200_OK,
)
async def put_projects_indicator(
    request: Request,
    projects_indicator: ProjectsIndicatorPut,
    user: UserDTO = Depends(user_dependency),
) -> ProjectsIndicator:
    """Update a project's indicator by setting all of its attributes."""
    user_project_service: UserProjectService = request.state.user_project_service

    indicator = await user_project_service.put_projects_indicator(projects_indicator, user.id)

    return ProjectsIndicator.from_dto(indicator)


@projects_router.patch(
    "/projects_indicators",
    response_model=ProjectsIndicator,
    status_code=status.HTTP_200_OK,
)
async def patch_projects_indicator(
    request: Request,
    projects_indicator: ProjectsIndicatorPatch,
    user: UserDTO = Depends(user_dependency),
) -> ProjectsIndicator:
    """Update a project's indicator by setting given attributes."""
    user_project_service: UserProjectService = request.state.user_project_service

    indicator = await user_project_service.patch_projects_indicator(projects_indicator, user.id)

    return ProjectsIndicator.from_dto(indicator)


@projects_router.delete(
    "/projects_indicators/all",
    status_code=status.HTTP_200_OK,
)
async def delete_all_projects_indicators(
    request: Request,
    scenario_id: int = Query(..., description="scenario identifier"),
    user: UserDTO = Depends(user_dependency),
) -> dict:
    """Delete all project's indicators for given scenario if you're the project owner."""
    user_project_service: UserProjectService = request.state.user_project_service

    return await user_project_service.delete_all_projects_indicators(scenario_id, user.id)


@projects_router.delete(
    "/projects_indicators",
    status_code=status.HTTP_200_OK,
)
async def delete_specific_projects_indicator(
    request: Request,
    scenario_id: int = Query(..., description="scenario identifier"),
    indicator_id: int = Query(..., description="indicator identifier"),
    user: UserDTO = Depends(user_dependency),
) -> dict:
    """Delete specific project's indicator for given scenario if you're the project owner."""
    user_project_service: UserProjectService = request.state.user_project_service

    return await user_project_service.delete_specific_projects_indicator(scenario_id, indicator_id, user.id)
