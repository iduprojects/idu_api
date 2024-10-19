import abc
from typing import Protocol

from idu_api.urban_api.dto import (
    ProjectDTO,
    ProjectsIndicatorDTO,
    ProjectTerritoryDTO,
    ScenarioDTO,
    ScenarioUrbanObjectDTO,
)
from idu_api.urban_api.schemas import (
    PhysicalObjectsDataPost,
    PhysicalObjectWithGeometryPost,
    ProjectPatch,
    ProjectPost,
    ProjectPut,
    ProjectsIndicatorPatch,
    ProjectsIndicatorPost,
    ProjectsIndicatorPut,
    ScenariosPatch,
    ScenariosPost,
    ScenariosPut,
    ServicesDataPost,
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
    async def add_physical_object_to_scenario(
        self, scenario_id: int, physical_object: PhysicalObjectWithGeometryPost, user_id: str
    ) -> ScenarioUrbanObjectDTO:
        """Add physical object to scenario."""

    @abc.abstractmethod
    async def add_existing_physical_object_to_scenario(
        self, scenario_id: int, object_geometry_id: int, physical_object: PhysicalObjectsDataPost, user_id: str
    ) -> ScenarioUrbanObjectDTO:
        """Add existing physical object to scenario."""

    @abc.abstractmethod
    async def add_service_to_scenario(
        self, scenario_id: int, service: ServicesDataPost, user_id: str
    ) -> ScenarioUrbanObjectDTO:
        """Add service object to scenario."""

    @abc.abstractmethod
    async def add_existing_service_to_scenario(
        self, scenario_id: int, service_id: int, physical_object_id: int, object_geometry_id: int, user_id: str
    ) -> ScenarioUrbanObjectDTO:
        """Add existing service object to scenario."""

    @abc.abstractmethod
    async def get_all_projects_indicators(self, scenario_id: int, user_id: str) -> list[ProjectsIndicatorDTO]:
        """Get project's indicators for given scenario if relevant project is public or if you're the project owner."""

    @abc.abstractmethod
    async def get_specific_projects_indicator(
        self, scenario_id: int, indicator_id: int, user_id: str
    ) -> ProjectsIndicatorDTO:
        """Get project's specific indicator for given scenario if relevant project is public or if you're the project owner."""

    @abc.abstractmethod
    async def add_projects_indicator(
        self, projects_indicator: ProjectsIndicatorPost, user_id: str
    ) -> ProjectsIndicatorDTO:
        """Add a new project's indicator."""

    @abc.abstractmethod
    async def put_projects_indicator(
        self, projects_indicator: ProjectsIndicatorPut, user_id: str
    ) -> ProjectsIndicatorDTO:
        """Update a project's indicator by setting all of its attributes."""

    @abc.abstractmethod
    async def patch_projects_indicator(
        self, projects_indicator: ProjectsIndicatorPatch, user_id: str
    ) -> ProjectsIndicatorDTO:
        """Update a project's indicator by setting given attributes."""

    @abc.abstractmethod
    async def delete_all_projects_indicators(self, scenario_id: int, user_id: str) -> dict:
        """Delete all project's indicators for given scenario if you're the project owner."""

    @abc.abstractmethod
    async def delete_specific_projects_indicator(self, scenario_id: int, indicator_id: int, user_id: str) -> dict:
        """Delete specific project's indicator for given scenario if you're the project owner."""
