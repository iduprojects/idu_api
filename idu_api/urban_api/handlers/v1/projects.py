"""
Projects endpoints are defined here.
"""

from fastapi import Request
from starlette import status

from idu_api.urban_api.logic.projects import UserProjectService
from idu_api.urban_api.schemas import Project, ProjectPatch, ProjectPost, ProjectPut, ProjectTerritory

from .routers import projects_router


@projects_router.get(
    "/projects",
    response_model=list[Project],
    status_code=status.HTTP_200_OK,
)
async def get_projects(request: Request) -> list[Project]:
    """Get all projects."""
    user_project_service: UserProjectService = request.state.user_project_service
    projects = await user_project_service.get_projects_from_db()

    return [Project.from_dto(project) for project in projects]


@projects_router.get(
    "/projects/{project_id}",
    response_model=Project,
    status_code=status.HTTP_200_OK,
)
async def get_project_by_id(request: Request, project_id: int) -> Project:
    """Get a project by id."""
    user_project_service: UserProjectService = request.state.user_project_service
    project = await user_project_service.get_project_by_id_from_db(project_id)

    return Project.from_dto(project)


@projects_router.get(
    "/projects/{project_id}/territory_info",
    response_model=ProjectTerritory,
    status_code=status.HTTP_200_OK,
)
async def get_projects_territory_info(request: Request, project_id: int) -> ProjectTerritory:
    """Get territory info of a project by id."""
    user_project_service: UserProjectService = request.state.user_project_service
    project_territory_dto = await user_project_service.get_project_territory_by_id_from_db(project_id)

    return ProjectTerritory.from_dto(project_territory_dto)


@projects_router.post(
    "/projects",
    response_model=Project,
    status_code=status.HTTP_201_CREATED,
)
async def post_project(request: Request, project: ProjectPost) -> Project:
    """Add a new project."""
    user_project_service: UserProjectService = request.state.user_project_service
    project_dto = await user_project_service.post_project_to_db(project)

    return Project.from_dto(project_dto)


@projects_router.put(
    "/projects/{project_id}",
    response_model=Project,
    status_code=status.HTTP_200_OK,
)
async def put_project(request: Request, project: ProjectPut, project_id: int) -> Project:
    """Update a project by setting all of its attributes."""
    user_project_service: UserProjectService = request.state.user_project_service
    project_dto = await user_project_service.put_project_to_db(project, project_id)

    return Project.from_dto(project_dto)


@projects_router.patch(
    "/projects/{project_id}",
    response_model=Project,
    status_code=status.HTTP_200_OK,
)
async def patch_project(request: Request, project: ProjectPatch, project_id: int) -> Project:
    """Update a project by setting given attributes."""
    user_project_service: UserProjectService = request.state.user_project_service
    project_dto = await user_project_service.patch_project_to_db(project, project_id)

    return Project.from_dto(project_dto)


@projects_router.delete(
    "/projects/{project_id}",
    status_code=status.HTTP_200_OK,
)
async def delete_project(request: Request, project_id: int) -> int:
    """Delete a project."""
    user_project_service: UserProjectService = request.state.user_project_service

    return await user_project_service.delete_project_from_db(project_id)
