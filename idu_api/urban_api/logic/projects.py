import abc
from typing import Protocol

from idu_api.urban_api.dto import ProjectDTO, ProjectTerritoryDTO
from idu_api.urban_api.schemas import ProjectPatch, ProjectPost, ProjectPut


class UserProjectService(Protocol):
    """Service to manipulate projects objects."""

    @abc.abstractmethod
    async def get_project_by_id_from_db(self, project_id: int) -> ProjectDTO:
        """Get project object by id."""

    @abc.abstractmethod
    async def post_project_to_db(self, project: ProjectPost) -> ProjectDTO:
        """Create project object."""

    @abc.abstractmethod
    async def get_projects_from_db(self) -> list[ProjectDTO]:
        """Get all projects."""

    @abc.abstractmethod
    async def get_project_territory_by_id_from_db(self, project_id: int) -> ProjectTerritoryDTO:
        """Get project object by id."""

    @abc.abstractmethod
    async def delete_project_from_db(self, project_id: int) -> int:
        """Delete project object."""

    @abc.abstractmethod
    async def put_project_to_db(self, project: ProjectPut, project_id: int) -> ProjectDTO:
        """Put project object."""

    @abc.abstractmethod
    async def patch_project_to_db(self, project: ProjectPatch, project_id: int) -> ProjectDTO:
        """Patch project object."""
