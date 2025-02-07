import abc
import io
from datetime import date
from typing import Any, Literal, Protocol

from idu_api.urban_api.dto import (
    FunctionalZoneDTO,
    FunctionalZoneSourceDTO,
    HexagonWithIndicatorsDTO,
    PageDTO,
    PhysicalObjectDTO,
    ProjectDTO,
    ProjectTerritoryDTO,
    ProjectWithTerritoryDTO,
    ScenarioDTO,
    ScenarioFunctionalZoneDTO,
    ScenarioGeometryDTO,
    ScenarioGeometryWithAllObjectsDTO,
    ScenarioIndicatorValueDTO,
    ScenarioPhysicalObjectDTO,
    ScenarioServiceDTO,
    ScenarioUrbanObjectDTO,
    ServiceDTO,
)
from idu_api.urban_api.dto.object_geometries import GeometryWithAllObjectsDTO, ObjectGeometryDTO
from idu_api.urban_api.schemas import (
    ObjectGeometryPatch,
    ObjectGeometryPut,
    PhysicalObjectPatch,
    PhysicalObjectPut,
    PhysicalObjectWithGeometryPost,
    ProjectPatch,
    ProjectPost,
    ProjectPut,
    ScenarioFunctionalZonePatch,
    ScenarioFunctionalZonePost,
    ScenarioFunctionalZonePut,
    ScenarioIndicatorValuePatch,
    ScenarioIndicatorValuePost,
    ScenarioIndicatorValuePut,
    ScenarioPatch,
    ScenarioPost,
    ScenarioPut,
    ScenarioServicePost,
    ServicePatch,
    ServicePut,
)
from idu_api.urban_api.utils.minio_client import AsyncMinioClient


class UserProjectService(Protocol):  # pylint: disable=too-many-public-methods
    """Service to manipulate projects objects."""

    @abc.abstractmethod
    async def get_project_by_id(self, project_id: int, user_id: str | None) -> ProjectDTO:
        """Get project object by id."""

    @abc.abstractmethod
    async def get_project_territory_by_id(self, project_id: int, user_id: str | None) -> ProjectTerritoryDTO:
        """Get project territory object by id."""

    @abc.abstractmethod
    async def get_projects(
        self,
        user_id: str | None,
        only_own: bool,
        is_regional: bool,
        project_type: Literal["common", "city"] | None,
        territory_id: int | None,
        name: str | None,
        created_at: date | None,
        order_by: Literal["created_at", "updated_at"] | None,
        ordering: Literal["asc", "desc"] | None,
        paginate: bool = False,
    ) -> PageDTO[ProjectDTO]:
        """Get all public and user's projects."""

    async def get_all_projects(self) -> list[ProjectDTO]:
        """Get all projects minimal info."""

    @abc.abstractmethod
    async def get_projects_territories(
        self,
        user_id: str | None,
        only_own: bool,
        project_type: Literal["common", "city"] | None,
        territory_id: int | None,
    ) -> list[ProjectWithTerritoryDTO]:
        """Get all public and user's projects territories."""

    @abc.abstractmethod
    async def get_preview_projects_images(
        self,
        minio_client: AsyncMinioClient,
        user_id: str | None,
        only_own: bool,
        is_regional: bool,
        project_type: Literal["common", "city"] | None,
        territory_id: int | None,
        name: str | None,
        created_at: date | None,
        order_by: Literal["created_at", "updated_at"] | None,
        ordering: Literal["asc", "desc"] | None,
        page: int,
        page_size: int,
    ) -> io.BytesIO:
        """Get preview images (zip) for all public and user's projects."""

    @abc.abstractmethod
    async def get_preview_projects_images_url(
        self,
        minio_client: AsyncMinioClient,
        user_id: str | None,
        only_own: bool,
        is_regional: bool,
        project_type: Literal["common", "city"] | None,
        territory_id: int | None,
        name: str | None,
        created_at: date | None,
        order_by: Literal["created_at", "updated_at"] | None,
        ordering: Literal["asc", "desc"] | None,
        page: int,
        page_size: int,
    ) -> list[dict[str, int | str]]:
        """Get preview images url for all public and user's projects."""

    @abc.abstractmethod
    async def get_user_projects(self, user_id: str, is_regional: bool, territory_id: int | None) -> PageDTO[ProjectDTO]:
        """Get all user's projects."""

    @abc.abstractmethod
    async def get_user_preview_projects_images(
        self,
        minio_client: AsyncMinioClient,
        user_id: str,
        is_regional: bool,
        territory_id: int | None,
        page: int,
        page_size: int,
    ) -> io.BytesIO:
        """Get preview images (zip) for all user's projects with parallel MinIO requests."""

    @abc.abstractmethod
    async def get_user_preview_projects_images_url(
        self,
        minio_client: AsyncMinioClient,
        user_id: str,
        is_regional: bool,
        territory_id: int | None,
        page: int,
        page_size: int,
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
    async def get_full_project_image(
        self, minio_client: AsyncMinioClient, project_id: int, user_id: str | None
    ) -> io.BytesIO:
        """Get full image for given project."""

    @abc.abstractmethod
    async def get_preview_project_image(
        self, minio_client: AsyncMinioClient, project_id: int, user_id: str | None
    ) -> io.BytesIO:
        """Get preview image for given project."""

    @abc.abstractmethod
    async def get_full_project_image_url(
        self, minio_client: AsyncMinioClient, project_id: int, user_id: str | None
    ) -> str:
        """Get full image url for given project."""

    @abc.abstractmethod
    async def get_scenarios_by_project_id(self, project_id: int, user_id: str | None) -> list[ScenarioDTO]:
        """Get list of scenario objects by project id."""

    @abc.abstractmethod
    async def get_scenario_by_id(self, scenario_id: int, user_id: str | None) -> ScenarioDTO:
        """Get scenario object by id."""

    @abc.abstractmethod
    async def add_scenario(self, scenario: ScenarioPost, user_id: str) -> ScenarioDTO:
        """Create scenario object from base scenario."""

    @abc.abstractmethod
    async def copy_scenario(self, scenario: ScenarioPost, scenario_id: int, user_id: str) -> ScenarioDTO:
        """Create a new scenario from another scenario (copy) by its identifier."""

    @abc.abstractmethod
    async def put_scenario(self, scenario: ScenarioPut, scenario_id: int, user_id) -> ScenarioDTO:
        """Put project object."""

    @abc.abstractmethod
    async def patch_scenario(self, scenario: ScenarioPatch, scenario_id: int, user_id: str) -> ScenarioDTO:
        """Patch project object."""

    @abc.abstractmethod
    async def delete_scenario(self, scenario_id: int, user_id: str) -> dict:
        """Delete scenario object."""

    @abc.abstractmethod
    async def get_physical_objects_by_scenario_id(
        self,
        scenario_id: int,
        user_id: str | None,
        physical_object_type_id: int | None,
        physical_object_function_id: int | None,
    ) -> list[ScenarioPhysicalObjectDTO]:
        """Get list of physical objects by scenario identifier."""

    @abc.abstractmethod
    async def get_context_physical_objects(
        self,
        project_id: int,
        user_id: str | None,
        physical_object_type_id: int | None,
        physical_object_function_id: int | None,
    ) -> list[PhysicalObjectDTO]:
        """Get list of physical objects for 'context' of the project territory."""

    @abc.abstractmethod
    async def add_physical_object_with_geometry(
        self,
        physical_object: PhysicalObjectWithGeometryPost,
        scenario_id: int,
        user_id: str,
    ) -> ScenarioUrbanObjectDTO:
        """Create scenario physical object with geometry."""

    @abc.abstractmethod
    async def update_physical_objects_by_function_id(
        self,
        physical_object: list[PhysicalObjectWithGeometryPost],
        scenario_id: int,
        user_id: str,
        physical_object_function_id: int,
    ) -> list[ScenarioUrbanObjectDTO]:
        """Delete all physical objects by physical object function identifier
        and upload new objects with the same function for given scenario."""

    @abc.abstractmethod
    async def put_physical_object(
        self,
        physical_object: PhysicalObjectPut,
        scenario_id: int,
        physical_object_id: int,
        is_scenario_object: bool,
        user_id: str,
    ) -> ScenarioPhysicalObjectDTO:
        """Update scenario physical object by all its attributes."""

    @abc.abstractmethod
    async def patch_physical_object(
        self,
        physical_object: PhysicalObjectPatch,
        scenario_id: int,
        physical_object_id: int,
        is_scenario_object: bool,
        user_id: str,
    ) -> ScenarioPhysicalObjectDTO:
        """Update scenario physical object by only given attributes."""

    @abc.abstractmethod
    async def delete_physical_object(
        self,
        scenario_id: int,
        physical_object_id: int,
        is_scenario_object: bool,
        user_id: str,
    ) -> dict:
        """Delete scenario physical object."""

    @abc.abstractmethod
    async def get_services_by_scenario_id(
        self,
        scenario_id: int,
        user_id: str | None,
        service_type_id: int | None,
        urban_function_id: int | None,
    ) -> list[ScenarioServiceDTO]:
        """Get list of services by scenario identifier."""

    @abc.abstractmethod
    async def get_context_services(
        self,
        project_id: int,
        user_id: str | None,
        service_type_id: int | None,
        urban_function_id: int | None,
    ) -> list[ServiceDTO]:
        """Get list of services for 'context' of the project territory."""

    @abc.abstractmethod
    async def add_service(self, service: ScenarioServicePost, scenario_id: int, user_id: str) -> ScenarioUrbanObjectDTO:
        """Create scenario service object."""

    @abc.abstractmethod
    async def put_service(
        self,
        service: ServicePut,
        scenario_id: int,
        service_id: int,
        is_scenario_object: bool,
        user_id: str,
    ) -> ScenarioServiceDTO:
        """Update scenario service by all its attributes."""

    @abc.abstractmethod
    async def patch_service(
        self,
        service: ServicePatch,
        scenario_id: int,
        service_id: int,
        is_scenario_object: bool,
        user_id: str,
    ) -> ScenarioServiceDTO:
        """Update scenario service by only given attributes."""

    @abc.abstractmethod
    async def delete_service(
        self,
        scenario_id: int,
        service_id: int,
        is_scenario_object: bool,
        user_id: str,
    ) -> dict:
        """Delete scenario service."""

    @abc.abstractmethod
    async def get_geometries_by_scenario_id(
        self,
        scenario_id: int,
        user_id: str | None,
        physical_object_id: int | None,
        service_id: int | None,
    ) -> list[ScenarioGeometryDTO]:
        """Get all geometries for given scenario."""

    @abc.abstractmethod
    async def get_geometries_with_all_objects_by_scenario_id(
        self,
        scenario_id: int,
        user_id: str | None,
        physical_object_type_id: int | None,
        service_type_id: int | None,
        physical_object_function_id: int | None,
        urban_function_id: int | None,
    ) -> list[ScenarioGeometryWithAllObjectsDTO]:
        """Get geometries with lists of physical objects and services by scenario identifier."""

    @abc.abstractmethod
    async def get_context_geometries(
        self,
        project_id: int,
        user_id: str | None,
        physical_object_id: int | None,
        service_id: int | None,
    ) -> list[ObjectGeometryDTO]:
        """Get list of geometries for 'context' of the project territory."""

    @abc.abstractmethod
    async def get_context_geometries_with_all_objects(
        self,
        project_id: int,
        user_id: str | None,
        physical_object_type_id: int | None,
        service_type_id: int | None,
        physical_object_function_id: int | None,
        urban_function_id: int | None,
    ) -> list[GeometryWithAllObjectsDTO]:
        """Get geometries with lists of physical objects and services for 'context' of the project territory."""

    @abc.abstractmethod
    async def put_object_geometry(
        self,
        object_geometry: ObjectGeometryPut,
        scenario_id: int,
        object_geometry_id: int,
        is_scenario_object: bool,
        user_id: str,
    ) -> ScenarioGeometryDTO:
        """Update scenario object geometry by all its attributes."""

    @abc.abstractmethod
    async def patch_object_geometry(
        self,
        object_geometry: ObjectGeometryPatch,
        scenario_id: int,
        object_geometry_id: int,
        is_scenario_object: bool,
        user_id: str,
    ) -> ScenarioGeometryDTO:
        """Update scenario object geometry by only given attributes."""

    @abc.abstractmethod
    async def delete_object_geometry(
        self,
        scenario_id: int,
        object_geometry_id: int,
        is_scenario_object: bool,
        user_id: str,
    ) -> dict:
        """Delete scenario object geometry."""

    @abc.abstractmethod
    async def get_scenario_indicators_values(
        self,
        scenario_id: int,
        indicator_ids: str | None,
        indicator_group_id: int | None,
        territory_id: int | None,
        hexagon_id: int | None,
        user_id: str | None,
    ) -> list[ScenarioIndicatorValueDTO]:
        """Get project's indicators values for given scenario
        if relevant project is public or if you're the project owner."""

    @abc.abstractmethod
    async def add_scenario_indicator_value(
        self, indicator_value: ScenarioIndicatorValuePost, scenario_id: int, user_id: str
    ) -> ScenarioIndicatorValueDTO:
        """Add a new project's indicator value."""

    @abc.abstractmethod
    async def put_scenario_indicator_value(
        self, indicator_value: ScenarioIndicatorValuePut, scenario_id: int, user_id: str
    ) -> ScenarioIndicatorValueDTO:
        """Put project's indicator value."""

    @abc.abstractmethod
    async def patch_scenario_indicator_value(
        self,
        indicator_value: ScenarioIndicatorValuePatch,
        scenario_id: int | None,
        indicator_value_id: int,
        user_id: str,
    ) -> ScenarioIndicatorValueDTO:
        """Patch project's indicator value."""

    @abc.abstractmethod
    async def delete_scenario_indicators_values_by_scenario_id(self, scenario_id: int, user_id: str) -> dict:
        """Delete all project's indicators values for given scenario if you're the project owner."""

    @abc.abstractmethod
    async def delete_scenario_indicator_value_by_id(
        self, scenario_id: int | None, indicator_value_id: int, user_id: str
    ) -> dict:
        """Delete specific project's indicator values by indicator value identifier if you're the project owner."""

    @abc.abstractmethod
    async def get_hexagons_with_indicators_by_scenario_id(
        self,
        scenario_id: int,
        indicator_ids: str | None,
        indicators_group_id: int | None,
        user_id: str | None,
    ) -> list[HexagonWithIndicatorsDTO]:
        """Get project's indicators values for given regional scenario with hexagons."""

    @abc.abstractmethod
    async def update_all_indicators_values_by_scenario_id(self, scenario_id: int, user_id: str) -> dict[str, Any]:
        """Update all indicators values for given scenario."""

    @abc.abstractmethod
    async def get_functional_zones_sources_by_scenario_id(
        self, scenario_id: int, user_id: str | None
    ) -> list[FunctionalZoneSourceDTO]:
        """Get list of pairs year + source for functional zones for given scenario."""

    @abc.abstractmethod
    async def get_functional_zones_by_scenario_id(
        self,
        scenario_id: int,
        year: int,
        source: str,
        functional_zone_type_id: int | None,
        user_id: str | None,
    ) -> list[ScenarioFunctionalZoneDTO]:
        """Get list of functional zone objects by scenario identifier."""

    @abc.abstractmethod
    async def get_context_functional_zones_sources(
        self, project_id: int, user_id: str | None
    ) -> list[FunctionalZoneSourceDTO]:
        """Get list of pairs year + source for functional zones for 'context' of the project territory."""

    @abc.abstractmethod
    async def get_context_functional_zones(
        self,
        project_id: int,
        year: int,
        source: str,
        functional_zone_type_id: int | None,
        user_id: str | None,
    ) -> list[FunctionalZoneDTO]:
        """Get list of functional zone objects for 'context' of the project territory."""

    @abc.abstractmethod
    async def add_scenario_functional_zones(
        self, profiles: list[ScenarioFunctionalZonePost], scenario_id: int, user_id: str
    ) -> list[ScenarioFunctionalZoneDTO]:
        """Add list of scenario functional zone objects."""

    @abc.abstractmethod
    async def put_scenario_functional_zone(
        self,
        profile: ScenarioFunctionalZonePut,
        scenario_id: int,
        functional_zone_id: int,
        user_id: str,
    ) -> ScenarioFunctionalZoneDTO:
        """Update scenario functional zone object by all its attributes."""

    @abc.abstractmethod
    async def patch_scenario_functional_zone(
        self,
        profile: ScenarioFunctionalZonePatch,
        scenario_id: int,
        functional_zone_id: int,
        user_id: str,
    ) -> ScenarioFunctionalZoneDTO:
        """Update scenario functional zone object by only given attributes."""

    @abc.abstractmethod
    async def delete_functional_zones_by_scenario_id(self, scenario_id: int, user_id: str) -> dict:
        """Delete functional zones by scenario identifier."""
