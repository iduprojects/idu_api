import abc
from typing import Protocol

from idu_api.urban_api.dto import ProjectDTO, ProjectTerritoryDTO, ScenarioDTO, TargetProfileDTO
from idu_api.urban_api.schemas import (
    ProjectPatch,
    ProjectPost,
    ProjectPut,
    ScenariosPatch,
    ScenariosPost,
    ScenariosPut,
    TargetProfilesPost,
)


class UserProjectService(Protocol):
    """Service to manipulate projects objects."""

    @abc.abstractmethod
    async def get_project_by_id(self, project_id: int, user_id: str) -> ProjectDTO:
        """Get project object by id."""

    @abc.abstractmethod
    async def add_project(self, project: ProjectPost, user_id: str) -> ProjectDTO:
        """Create project object and base scenario."""

    @abc.abstractmethod
    async def get_all_available_projects(self, user_id: str) -> list[ProjectDTO]:
        """Get all public and user's projects."""

    @abc.abstractmethod
    async def get_user_projects(self, user_id: str) -> list[ProjectDTO]:
        """Get all user's projects."""

    @abc.abstractmethod
    async def get_project_territory_by_id(self, project_id: int, user_id) -> ProjectTerritoryDTO:
        """Get project object by id."""

    @abc.abstractmethod
    async def delete_project(self, project_id: int, user_id) -> dict:
        """Delete project object."""

    @abc.abstractmethod
    async def put_project(self, project: ProjectPut, project_id: int, user_id: str) -> ProjectDTO:
        """Put project object."""

    @abc.abstractmethod
    async def patch_project(self, project: ProjectPatch, project_id: int, user_id: str) -> ProjectDTO:
        """Patch project object."""

    @abc.abstractmethod
    async def get_scenarios_by_project_id(self, project_id: int, user_id) -> list[ScenarioDTO]:
        """Get list of scenario objects by project id."""

    @abc.abstractmethod
    async def get_scenario_by_id(self, scenario_id: int, user_id) -> ScenarioDTO:
        """Get scenario object by id."""

    @abc.abstractmethod
    async def add_scenario(self, scenario: ScenariosPost, user_id: str) -> ScenarioDTO:
        """Create scenario object."""

    @abc.abstractmethod
    async def put_scenario(self, scenario: ScenariosPut, scenario_id: int, user_id) -> ScenarioDTO:
        """Put project object."""

    @abc.abstractmethod
    async def patch_scenario(self, scenario: ScenariosPatch, scenario_id: int, user_id: str) -> ScenarioDTO:
        """Patch project object."""

    @abc.abstractmethod
    async def delete_scenario(self, scenario_id: int, user_id: str) -> dict:
        """Delete scenario object."""

    @abc.abstractmethod
    async def get_target_profiles(self) -> list[TargetProfileDTO]:
        """Get all target profile objects."""

    @abc.abstractmethod
    async def add_target_profile(self, target_profile: TargetProfilesPost) -> TargetProfileDTO:
        """Create a new target profile object."""
