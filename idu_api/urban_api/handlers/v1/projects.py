"""
Projects endpoints are defined here.
"""

from fastapi import Path, Query, Request
from sqlalchemy.ext.asyncio import AsyncConnection
from starlette import status
from datetime import datetime
from typing import List, Optional

from idu_api.urban_api.logic.projects import (
    get_projects_from_db,
    post_project_to_db,
    delete_project_from_db,
    put_project_to_db,
    patch_project_to_db,
    get_project_by_id_from_db,
    get_project_territory_by_id_from_db,
)
from idu_api.urban_api.schemas import Project, ProjectPost, ProjectPut, ProjectPatch, ProjectTerritory
from idu_api.urban_api.schemas.geometries import Geometry

from .routers import projects_router


@projects_router.get(
    "/projects",
    response_model=List[Project],
    status_code=status.HTTP_200_OK,
)
async def get_projects(request: Request) -> List[Project]:
    """
    Get all projects
    """
    conn: AsyncConnection = request.state.conn
    projects = await get_projects_from_db(conn)
    return [Project.from_dto(project) for project in projects]


@projects_router.get(
    "/projects/{project_id}",
    response_model=Project,
    status_code=status.HTTP_200_OK,
)
async def get_project_by_id(request: Request, project_id: int) -> Project:
    """
    Get project by id
    """
    conn: AsyncConnection = request.state.conn
    project = await get_project_by_id_from_db(conn, project_id)
    return Project.from_dto(project)


@projects_router.get(
    "/projects/{project_id}/territory_info",
    response_model=ProjectTerritory,
    status_code=status.HTTP_200_OK,
)
async def get_projects_territory_info(request: Request, project_id: int) -> ProjectTerritory:
    """
    Get territory info of a project by id
    """
    conn: AsyncConnection = request.state.conn
    project_territory_dto = await get_project_territory_by_id_from_db(conn, project_id)
    return ProjectTerritory.from_dto(project_territory_dto)


@projects_router.post(
    "/projects",
    response_model=Project,
    status_code=status.HTTP_201_CREATED,
)
async def post_project(request: Request, project: ProjectPost) -> Project:
    """
    Post a project
    """
    conn: AsyncConnection = request.state.conn
    project_dto = await post_project_to_db(conn, project)
    return Project.from_dto(project_dto)


@projects_router.put(
    "/projects/{project_id}",
    response_model=Project,
    status_code=status.HTTP_202_ACCEPTED,
)
async def put_project(request: Request, project: ProjectPut, project_id: int) -> Project:
    """
    Put a project
    """
    conn: AsyncConnection = request.state.conn
    project_dto = await put_project_to_db(conn, project, project_id)
    return Project.from_dto(project_dto)


# TODO
@projects_router.patch(
    "/projects/{project_id}",
    response_model=Project,
    status_code=status.HTTP_202_ACCEPTED,
)
async def patch_project(request: Request, project: ProjectPatch, project_id: int) -> Project:
    """
    Patch a project
    """
    conn: AsyncConnection = request.state.conn
    project_dto = await patch_project_to_db(conn, project, project_id)
    return Project.from_dto(project_dto)


@projects_router.delete(
    "/projects/{project_id}",
    status_code=status.HTTP_200_OK,
)
async def delete_project(request: Request, project_id: int) -> None:
    """
    Delete a project
    """
    conn: AsyncConnection = request.state.conn
    await delete_project_from_db(conn, project_id)
