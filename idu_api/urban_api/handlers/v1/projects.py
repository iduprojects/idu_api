"""
Projects endpoints are defined here.
"""

from fastapi import Depends, Request
from starlette import status

from idu_api.urban_api.dto.users import UserDTO
from idu_api.urban_api.logic.projects import UserProjectService
from idu_api.urban_api.schemas import Project, ProjectPatch, ProjectPost, ProjectPut, ProjectTerritory
from idu_api.urban_api.utils.dependencies import user_dependency

from .routers import projects_router


@projects_router.get(
    "/projects",
    response_model=list[Project],
    status_code=status.HTTP_200_OK,
)
async def get_all_projects(request: Request, user: UserDTO = Depends(user_dependency)) -> list[Project]:
    """Get all public projects and projects that are owned by the user."""
    user_project_service: UserProjectService = request.state.user_project_service

    projects = await user_project_service.get_all_available_projects(user.id)

    return [Project.from_dto(project) for project in projects]


@projects_router.get(
    "/user_projects",
    response_model=list[Project],
    status_code=status.HTTP_200_OK,
)
async def get_user_projects(request: Request, user: UserDTO = Depends(user_dependency)) -> list[Project]:
    """Get all user's projects."""
    user_project_service: UserProjectService = request.state.user_project_service

    projects = await user_project_service.get_user_projects(user.id)

    return [Project.from_dto(project) for project in projects]


@projects_router.get(
    "/projects/{project_id}",
    response_model=Project,
    status_code=status.HTTP_200_OK,
)
async def get_project_by_id(request: Request, project_id: int, user: UserDTO = Depends(user_dependency)) -> Project:
    """Get a project by id."""
    user_project_service: UserProjectService = request.state.user_project_service

    project_dto = await user_project_service.get_project_by_id(project_id, user.id)

    return Project.from_dto(project_dto)


@projects_router.get(
    "/projects/{project_id}/territory_info",
    response_model=ProjectTerritory,
    status_code=status.HTTP_200_OK,
)
async def get_projects_territory_info(
    request: Request, project_id: int, user: UserDTO = Depends(user_dependency)
) -> ProjectTerritory:
    """Get territory info of a project by id."""
    user_project_service: UserProjectService = request.state.user_project_service

    project_territory_dto = await user_project_service.get_project_territory_by_id(project_id, user.id)

    return ProjectTerritory.from_dto(project_territory_dto)


@projects_router.post(
    "/projects",
    response_model=Project,
    status_code=status.HTTP_201_CREATED,
)
async def post_project(request: Request, project: ProjectPost, user: UserDTO = Depends(user_dependency)) -> Project:
    """Add a new project."""
    user_project_service: UserProjectService = request.state.user_project_service

    project_dto = await user_project_service.add_project(project, user.id)

    return Project.from_dto(project_dto)


@projects_router.put(
    "/projects/{project_id}",
    response_model=Project,
    status_code=status.HTTP_200_OK,
)
async def put_project(
    request: Request, project: ProjectPut, project_id: int, user: UserDTO = Depends(user_dependency)
) -> Project:
    """Update a project by setting all of its attributes."""
    user_project_service: UserProjectService = request.state.user_project_service

    project_dto = await user_project_service.put_project(project, project_id, user.id)

    return Project.from_dto(project_dto)


@projects_router.patch(
    "/projects/{project_id}",
    response_model=Project,
    status_code=status.HTTP_200_OK,
)
async def patch_project(
    request: Request, project: ProjectPatch, project_id: int, user: UserDTO = Depends(user_dependency)
) -> Project:
    """Update a project by setting given attributes."""
    user_project_service: UserProjectService = request.state.user_project_service

    project_dto = await user_project_service.patch_project(project, project_id, user.id)

    return Project.from_dto(project_dto)


@projects_router.delete(
    "/projects/{project_id}",
    status_code=status.HTTP_200_OK,
)
async def delete_project(request: Request, project_id: int, user: UserDTO = Depends(user_dependency)) -> dict:
    """Delete a project."""
    user_project_service: UserProjectService = request.state.user_project_service

    return await user_project_service.delete_project(project_id, user.id)
