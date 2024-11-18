import abc
import io
from typing import Protocol

from idu_api.urban_api.dto import (
    PhysicalObjectDataDTO,
    ProjectDTO,
    ProjectsIndicatorValueDTO,
    ProjectTerritoryDTO,
    ScenarioDTO,
    ScenarioGeometryDTO,
    ScenarioGeometryWithAllObjectsDTO,
    ScenarioPhysicalObjectDTO,
    ScenarioServiceDTO,
    ServiceDTO,
)
from idu_api.urban_api.dto.object_geometries import GeometryWithAllObjectsDTO, ObjectGeometryDTO
from idu_api.urban_api.schemas import (
    ProjectPatch,
    ProjectPost,
    ProjectPut,
    ProjectsIndicatorValuePatch,
    ProjectsIndicatorValuePost,
    ProjectsIndicatorValuePut,
    ScenariosPatch,
    ScenariosPost,
    ScenariosPut,
)
from idu_api.urban_api.utils.minio_client import AsyncMinioClient


class UserProjectService(Protocol):
    """Service to manipulate projects objects."""

    @abc.abstractmethod
    async def get_project_by_id(self, project_id: int, user_id: str) -> ProjectDTO:
        """Get project object by id."""

    @abc.abstractmethod
    async def get_project_territory_by_id(self, project_id: int, user_id: str) -> ProjectTerritoryDTO:
        """Get project territory object by id."""

    @abc.abstractmethod
    async def get_all_available_projects(self, user_id: str | None, is_regional: bool) -> list[ProjectDTO]:
        """Get all public and user's projects."""

    @abc.abstractmethod
    async def get_all_preview_projects_images(
        self, minio_client: AsyncMinioClient, user_id: str | None, is_regional: bool
    ) -> io.BytesIO:
        """Get preview images (zip) for all public and user's projects."""

    @abc.abstractmethod
    async def get_all_preview_projects_images_url(
        self, minio_client: AsyncMinioClient, user_id: str | None, is_regional: bool
    ) -> list[dict[str, int | str]]:
        """Get preview images url for all public and user's projects."""

    @abc.abstractmethod
    async def get_user_projects(self, user_id: str, is_regional: bool) -> list[ProjectDTO]:
        """Get all user's projects."""

    @abc.abstractmethod
    async def get_user_preview_projects_images(
        self, minio_client: AsyncMinioClient, user_id: str, is_regional: bool
    ) -> io.BytesIO:
        """Get preview images (zip) for all user's projects with parallel MinIO requests."""

    @abc.abstractmethod
    async def get_user_preview_projects_images_url(
        self, minio_client: AsyncMinioClient, user_id: str, is_regional: bool
    ) -> list[dict[str, int | str]]:
        """Get preview images url for all user's projects."""

    @abc.abstractmethod
    async def add_project(self, project: ProjectPost, user_id: str) -> ProjectDTO:
        """Create project object."""

    @abc.abstractmethod
    async def put_project(self, project: ProjectPut, project_id: int, user_id: str) -> ProjectDTO:
        """Update project object by all its attributes."""

    @abc.abstractmethod
    async def patch_project(self, project: ProjectPatch, project_id: int, user_id: str) -> ProjectDTO:
        """Update project object by only given attributes."""

    @abc.abstractmethod
    async def delete_project(self, project_id: int, minio_client: AsyncMinioClient, user_id: str) -> dict:
        """Delete project object."""

    @abc.abstractmethod
    async def upload_project_image(
        self, minio_client: AsyncMinioClient, project_id: int, user_id: str, file: bytes
    ) -> dict:
        """Create project image preview and upload it (full and preview) to minio bucket."""

    @abc.abstractmethod
    async def get_full_project_image(self, minio_client: AsyncMinioClient, project_id: int, user_id: str) -> io.BytesIO:
        """Get full image for given project."""

    @abc.abstractmethod
    async def get_preview_project_image(
        self, minio_client: AsyncMinioClient, project_id: int, user_id: str
    ) -> io.BytesIO:
        """Get preview image for given project."""

    @abc.abstractmethod
    async def get_full_project_image_url(self, minio_client: AsyncMinioClient, project_id: int, user_id: str) -> str:
        """Get full image url for given project."""

    @abc.abstractmethod
    async def get_scenarios_by_project_id(self, project_id: int, user_id: str) -> list[ScenarioDTO]:
        """Get list of scenario objects by project id."""

    @abc.abstractmethod
    async def get_scenario_by_id(self, scenario_id: int, user_id: str) -> ScenarioDTO:
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
    async def get_physical_objects_by_scenario_id(
        self,
        scenario_id: int,
        user_id: str,
        physical_object_type_id: int | None,
        physical_object_function_id: int | None,
    ) -> list[ScenarioPhysicalObjectDTO]:
        """Get list of physical objects by scenario identifier."""

    @abc.abstractmethod
    async def get_context_physical_objects_by_scenario_id(
        self,
        scenario_id: int,
        user_id: str,
        physical_object_type_id: int | None,
        physical_object_function_id: int | None,
    ) -> list[PhysicalObjectDataDTO]:
        """Get list of physical objects for 'context' of the project territory."""

    @abc.abstractmethod
    async def get_services_by_scenario_id(
        self,
        scenario_id: int,
        user_id: str,
        service_type_id: int | None,
        urban_function_id: int | None,
    ) -> list[ScenarioServiceDTO]:
        """Get list of services by scenario identifier."""

    @abc.abstractmethod
    async def get_context_services_by_scenario_id(
        self,
        scenario_id: int,
        user_id: str,
        service_type_id: int | None,
        urban_function_id: int | None,
    ) -> list[ServiceDTO]:
        """Get list of services for 'context' of the project territory."""

    @abc.abstractmethod
    async def get_geometries_by_scenario_id(
        self,
        scenario_id: int,
        user_id: str,
        physical_object_id: int | None,
        service_id: int | None,
    ) -> list[ScenarioGeometryDTO]:
        """Get all geometries for given scenario."""

    @abc.abstractmethod
    async def get_geometries_with_all_objects_by_scenario_id(
        self,
        scenario_id: int,
        user_id: str,
        physical_object_type_id: int | None,
        service_type_id: int | None,
        physical_object_function_id: int | None,
        urban_function_id: int | None,
    ) -> list[ScenarioGeometryWithAllObjectsDTO]:
        """Get geometries with lists of physical objects and services by scenario identifier."""

    @abc.abstractmethod
    async def get_context_geometries_by_scenario_id(
        self,
        scenario_id: int,
        user_id: str,
        physical_object_id: int | None,
        service_id: int | None,
    ) -> list[ObjectGeometryDTO]:
        """Get list of geometries for 'context' of the project territory."""

    @abc.abstractmethod
    async def get_context_geometries_with_all_objects_by_scenario_id(
        self,
        scenario_id: int,
        user_id: str,
        physical_object_type_id: int | None,
        service_type_id: int | None,
        physical_object_function_id: int | None,
        urban_function_id: int | None,
    ) -> list[GeometryWithAllObjectsDTO]:
        """Get geometries with lists of physical objects and services for 'context' of the project territory."""

    @abc.abstractmethod
    async def get_projects_indicators_values_by_scenario_id(
        self,
        scenario_id: int,
        indicator_ids: str | None,
        indicator_group_id: int | None,
        territory_id: int | None,
        hexagon_id: int | None,
        user_id: str,
    ) -> list[ProjectsIndicatorValueDTO]:
        """Get project's indicators values for given scenario
        if relevant project is public or if you're the project owner."""

    @abc.abstractmethod
    async def get_project_indicator_value_by_id(
        self, indicator_value_id: int, user_id: str
    ) -> ProjectsIndicatorValueDTO:
        """Get project's specific indicator values for given scenario
        if relevant project is public or if you're the project owner."""

    @abc.abstractmethod
    async def add_projects_indicator_value(
        self, projects_indicator: ProjectsIndicatorValuePost, user_id: str
    ) -> ProjectsIndicatorValueDTO:
        """Add a new project's indicator value."""

    @abc.abstractmethod
    async def put_projects_indicator_value(
        self, projects_indicator: ProjectsIndicatorValuePut, indicator_value_id: int, user_id: str
    ) -> ProjectsIndicatorValueDTO:
        """Put project's indicator value."""

    @abc.abstractmethod
    async def patch_projects_indicator_value(
        self, projects_indicator: ProjectsIndicatorValuePatch, indicator_value_id: int, user_id: str
    ) -> ProjectsIndicatorValueDTO:
        """Patch project's indicator value."""

    @abc.abstractmethod
    async def delete_projects_indicators_values_by_scenario_id(self, scenario_id: int, user_id: str) -> dict:
        """Delete all project's indicators values for given scenario if you're the project owner."""

    @abc.abstractmethod
    async def delete_project_indicator_value_by_id(self, indicator_value_id: int, user_id: str) -> dict:
        """Delete specific project's indicator values by indicator value identifier if you're the project owner."""
