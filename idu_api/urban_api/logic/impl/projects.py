from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.urban_api.dto import ProjectDTO, ProjectTerritoryDTO
from idu_api.urban_api.logic.impl.helpers.projects import (
    add_project_to_db,
    delete_project_from_db,
    get_all_available_projects_from_db,
    get_project_by_id_from_db,
    get_project_territory_by_id_from_db,
    get_user_projects_from_db,
    patch_project_to_db,
    put_project_to_db,
)
from idu_api.urban_api.logic.projects import UserProjectService
from idu_api.urban_api.schemas import ProjectPatch, ProjectPost, ProjectPut


class UserProjectServiceImpl(UserProjectService):
    """Service to manipulate projects entities.

    Based on async SQLAlchemy connection.
    """

    def __init__(self, conn: AsyncConnection):
        self._conn = conn

    async def get_project_by_id(self, project_id: int, user_id: str) -> ProjectDTO:
        return await get_project_by_id_from_db(self._conn, project_id, user_id)

    async def add_project(self, project: ProjectPost, user_id: str) -> ProjectDTO:
        return await add_project_to_db(self._conn, project, user_id)

    async def get_all_available_projects(self, user_id: int) -> list[ProjectDTO]:
        return await get_all_available_projects_from_db(self._conn, user_id)

    async def get_user_projects(self, user_id: str) -> list[ProjectDTO]:
        return await get_user_projects_from_db(self._conn, user_id)

    async def get_project_territory_by_id(self, project_id: int, user_id: str) -> ProjectTerritoryDTO:
        return await get_project_territory_by_id_from_db(self._conn, project_id, user_id)

    async def delete_project(self, project_id: int, user_id: str) -> dict:
        return await delete_project_from_db(self._conn, project_id, user_id)

    async def put_project(self, project: ProjectPut, project_id: int, user_id: str) -> ProjectDTO:
        return await put_project_to_db(self._conn, project, project_id, user_id)

    async def patch_project(self, project: ProjectPatch, project_id: int, user_id: str) -> ProjectDTO:
        return await patch_project_to_db(self._conn, project, project_id, user_id)
