"""Projects handlers logic is defined here."""

import io

from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.urban_api.dto import (
    ObjectGeometryDTO,
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
from idu_api.urban_api.dto.object_geometries import GeometryWithAllObjectsDTO
from idu_api.urban_api.logic.impl.helpers.projects_geometries import (
    get_context_geometries_by_scenario_id_from_db,
    get_context_geometries_with_all_objects_by_scenario_id_from_db,
    get_geometries_by_scenario_id_from_db,
    get_geometries_with_all_objects_by_scenario_id_from_db,
)
from idu_api.urban_api.logic.impl.helpers.projects_indicators import (
    add_projects_indicator_value_to_db,
    delete_all_projects_indicators_values_from_db,
    delete_specific_projects_indicator_values_from_db,
    get_all_projects_indicators_values_from_db,
    get_specific_projects_indicator_values_from_db,
    patch_projects_indicator_value_to_db,
    put_projects_indicator_value_to_db,
)
from idu_api.urban_api.logic.impl.helpers.projects_objects import (
    add_project_to_db,
    delete_project_from_db,
    get_all_available_projects_from_db,
    get_all_preview_projects_images_from_minio,
    get_all_preview_projects_images_url_from_minio,
    get_full_project_image_from_minio,
    get_full_project_image_url_from_minio,
    get_preview_project_image_from_minio,
    get_project_by_id_from_db,
    get_project_territory_by_id_from_db,
    get_user_preview_projects_images_from_minio,
    get_user_preview_projects_images_url_from_minio,
    get_user_projects_from_db,
    patch_project_to_db,
    put_project_to_db,
    upload_project_image_to_minio,
)
from idu_api.urban_api.logic.impl.helpers.projects_physical_objects import (
    get_context_physical_objects_by_scenario_id_from_db,
    get_physical_objects_by_scenario_id,
)
from idu_api.urban_api.logic.impl.helpers.projects_scenarios import (
    add_new_scenario_to_db,
    delete_scenario_from_db,
    get_scenario_by_id_from_db,
    get_scenarios_by_project_id_from_db,
    patch_scenario_to_db,
    put_scenario_to_db,
)
from idu_api.urban_api.logic.impl.helpers.projects_services import (
    get_context_services_by_scenario_id_from_db,
    get_services_by_scenario_id,
)
from idu_api.urban_api.logic.projects import UserProjectService
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


class UserProjectServiceImpl(UserProjectService):
    """Service to manipulate projects entities.

    Based on async SQLAlchemy connection.
    """

    def __init__(self, conn: AsyncConnection):
        self._conn = conn

    async def get_project_by_id(self, project_id: int, user_id: str) -> ProjectDTO:
        return await get_project_by_id_from_db(self._conn, project_id, user_id)

    async def get_project_territory_by_id(self, project_id: int, user_id: str) -> ProjectTerritoryDTO:
        return await get_project_territory_by_id_from_db(self._conn, project_id, user_id)

    async def get_all_available_projects(self, user_id: str | None, is_regional: bool) -> list[ProjectDTO]:
        return await get_all_available_projects_from_db(self._conn, user_id, is_regional)

    async def get_all_preview_projects_images(
        self, minio_client: AsyncMinioClient, user_id: str | None, is_regional: bool
    ) -> io.BytesIO:
        return await get_all_preview_projects_images_from_minio(self._conn, minio_client, user_id, is_regional)

    async def get_all_preview_projects_images_url(
        self, minio_client: AsyncMinioClient, user_id: str | None, is_regional: bool
    ) -> list[dict[str, int | str]]:
        return await get_all_preview_projects_images_url_from_minio(self._conn, minio_client, user_id, is_regional)

    async def get_user_projects(self, user_id: str, is_regional: bool) -> list[ProjectDTO]:
        return await get_user_projects_from_db(self._conn, user_id, is_regional)

    async def get_user_preview_projects_images(
        self, minio_client: AsyncMinioClient, user_id: str, is_regional: bool
    ) -> io.BytesIO:
        return await get_user_preview_projects_images_from_minio(self._conn, minio_client, user_id, is_regional)

    async def get_user_preview_projects_images_url(
        self, minio_client: AsyncMinioClient, user_id: str | None, is_regional: bool
    ) -> list[dict[str, int | str]]:
        return await get_user_preview_projects_images_url_from_minio(self._conn, minio_client, user_id, is_regional)

    async def add_project(self, project: ProjectPost, user_id: str) -> ProjectDTO:
        return await add_project_to_db(self._conn, project, user_id)

    async def put_project(self, project: ProjectPut, project_id: int, user_id: str) -> ProjectDTO:
        return await put_project_to_db(self._conn, project, project_id, user_id)

    async def patch_project(self, project: ProjectPatch, project_id: int, user_id: str) -> ProjectDTO:
        return await patch_project_to_db(self._conn, project, project_id, user_id)

    async def delete_project(self, project_id: int, minio_client: AsyncMinioClient, user_id: str) -> dict:
        return await delete_project_from_db(self._conn, project_id, minio_client, user_id)

    async def upload_project_image(
        self, minio_client: AsyncMinioClient, project_id: int, user_id: str, file: bytes
    ) -> dict:
        return await upload_project_image_to_minio(self._conn, minio_client, project_id, user_id, file)

    async def get_full_project_image(self, minio_client: AsyncMinioClient, project_id: int, user_id: str) -> io.BytesIO:
        return await get_full_project_image_from_minio(self._conn, minio_client, project_id, user_id)

    async def get_preview_project_image(
        self, minio_client: AsyncMinioClient, project_id: int, user_id: str
    ) -> io.BytesIO:
        return await get_preview_project_image_from_minio(self._conn, minio_client, project_id, user_id)

    async def get_full_project_image_url(self, minio_client: AsyncMinioClient, project_id: int, user_id: str) -> str:
        return await get_full_project_image_url_from_minio(self._conn, minio_client, project_id, user_id)

    async def get_scenarios_by_project_id(self, project_id: int, user_id) -> list[ScenarioDTO]:
        return await get_scenarios_by_project_id_from_db(self._conn, project_id, user_id)

    async def get_scenario_by_id(self, scenario_id: int, user_id) -> ScenarioDTO:
        return await get_scenario_by_id_from_db(self._conn, scenario_id, user_id)

    async def add_scenario(self, scenario: ScenariosPost, user_id: str) -> ScenarioDTO:
        return await add_new_scenario_to_db(self._conn, scenario, user_id)

    async def put_scenario(self, scenario: ScenariosPut, scenario_id: int, user_id) -> ScenarioDTO:
        return await put_scenario_to_db(self._conn, scenario, scenario_id, user_id)

    async def patch_scenario(self, scenario: ScenariosPatch, scenario_id: int, user_id: str) -> ScenarioDTO:
        return await patch_scenario_to_db(self._conn, scenario, scenario_id, user_id)

    async def delete_scenario(self, scenario_id: int, user_id: str) -> dict:
        return await delete_scenario_from_db(self._conn, scenario_id, user_id)

    async def get_physical_objects_by_scenario_id(
        self,
        scenario_id: int,
        user_id: str,
        physical_object_type_id: int | None,
        physical_object_function_id: int | None,
    ) -> list[ScenarioPhysicalObjectDTO]:
        return await get_physical_objects_by_scenario_id(
            self._conn,
            scenario_id,
            user_id,
            physical_object_type_id,
            physical_object_function_id,
        )

    async def get_context_physical_objects_by_scenario_id(
        self,
        scenario_id: int,
        user_id: str,
        physical_object_type_id: int | None,
        physical_object_function_id: int | None,
    ) -> list[PhysicalObjectDataDTO]:
        return await get_context_physical_objects_by_scenario_id_from_db(
            self._conn,
            scenario_id,
            user_id,
            physical_object_type_id,
            physical_object_function_id,
        )

    async def get_services_by_scenario_id(
        self,
        scenario_id: int,
        user_id: str,
        service_type_id: int | None,
        urban_function_id: int | None,
    ) -> list[ScenarioServiceDTO]:
        return await get_services_by_scenario_id(
            self._conn,
            scenario_id,
            user_id,
            service_type_id,
            urban_function_id,
        )

    async def get_context_services_by_scenario_id(
        self,
        scenario_id: int,
        user_id: str,
        service_type_id: int | None,
        urban_function_id: int | None,
    ) -> list[ServiceDTO]:
        return await get_context_services_by_scenario_id_from_db(
            self._conn,
            scenario_id,
            user_id,
            service_type_id,
            urban_function_id,
        )

    async def get_geometries_by_scenario_id(
        self,
        scenario_id: int,
        user_id: str,
        physical_object_id: int | None,
        service_id: int | None,
    ) -> list[ScenarioGeometryDTO]:
        return await get_geometries_by_scenario_id_from_db(
            self._conn,
            scenario_id,
            user_id,
            physical_object_id,
            service_id,
        )

    async def get_geometries_with_all_objects_by_scenario_id(
        self,
        scenario_id: int,
        user_id: str,
        physical_object_type_id: int | None,
        service_type_id: int | None,
        physical_object_function_id: int | None,
        urban_function_id: int | None,
    ) -> list[ScenarioGeometryWithAllObjectsDTO]:
        return await get_geometries_with_all_objects_by_scenario_id_from_db(
            self._conn,
            scenario_id,
            user_id,
            physical_object_type_id,
            service_type_id,
            physical_object_function_id,
            urban_function_id,
        )

    async def get_context_geometries_by_scenario_id(
        self,
        scenario_id: int,
        user_id: str,
        physical_object_id: int | None,
        service_id: int | None,
    ) -> list[ObjectGeometryDTO]:
        return await get_context_geometries_by_scenario_id_from_db(
            self._conn,
            scenario_id,
            user_id,
            physical_object_id,
            service_id,
        )

    async def get_context_geometries_with_all_objects_by_scenario_id(
        self,
        scenario_id: int,
        user_id: str,
        physical_object_type_id: int | None,
        service_type_id: int | None,
        physical_object_function_id: int | None,
        urban_function_id: int | None,
    ) -> list[GeometryWithAllObjectsDTO]:
        return await get_context_geometries_with_all_objects_by_scenario_id_from_db(
            self._conn,
            scenario_id,
            user_id,
            physical_object_type_id,
            service_type_id,
            physical_object_function_id,
            urban_function_id,
        )

    async def get_all_projects_indicators_values(
        self, scenario_id: int, user_id: str
    ) -> list[ProjectsIndicatorValueDTO]:
        return await get_all_projects_indicators_values_from_db(self._conn, scenario_id, user_id)

    async def get_specific_projects_indicator_values(
        self, scenario_id: int, indicator_id: int, user_id: str
    ) -> list[ProjectsIndicatorValueDTO]:
        return await get_specific_projects_indicator_values_from_db(self._conn, scenario_id, indicator_id, user_id)

    async def add_projects_indicator_value(
        self, projects_indicator: ProjectsIndicatorValuePost, user_id: str
    ) -> ProjectsIndicatorValueDTO:
        return await add_projects_indicator_value_to_db(self._conn, projects_indicator, user_id)

    async def put_projects_indicator_value(
        self, projects_indicator: ProjectsIndicatorValuePut, user_id: str
    ) -> ProjectsIndicatorValueDTO:
        return await put_projects_indicator_value_to_db(self._conn, projects_indicator, user_id)

    async def patch_projects_indicator_value(
        self, projects_indicator: ProjectsIndicatorValuePatch, user_id: str
    ) -> ProjectsIndicatorValueDTO:
        return await patch_projects_indicator_value_to_db(self._conn, projects_indicator, user_id)

    async def delete_all_projects_indicators_values(self, scenario_id: int, user_id: str) -> dict:
        return await delete_all_projects_indicators_values_from_db(self._conn, scenario_id, user_id)

    async def delete_specific_projects_indicator_values(
        self, scenario_id: int, indicator_id: int, user_id: str
    ) -> dict:
        return await delete_specific_projects_indicator_values_from_db(self._conn, scenario_id, indicator_id, user_id)
