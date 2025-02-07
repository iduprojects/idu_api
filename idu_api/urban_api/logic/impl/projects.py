"""Projects handlers logic is defined here."""

import io
from datetime import date
from typing import Any, Literal

import structlog
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.urban_api.dto import (
    FunctionalZoneDTO,
    FunctionalZoneSourceDTO,
    HexagonWithIndicatorsDTO,
    ObjectGeometryDTO,
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
from idu_api.urban_api.dto.object_geometries import GeometryWithAllObjectsDTO
from idu_api.urban_api.logic.impl.helpers.projects_functional_zones import (
    add_scenario_functional_zones_to_db,
    delete_functional_zones_by_scenario_id_from_db,
    get_context_functional_zones_from_db,
    get_context_functional_zones_sources_from_db,
    get_functional_zones_by_scenario_id_from_db,
    get_functional_zones_sources_by_scenario_id_from_db,
    patch_scenario_functional_zone_to_db,
    put_scenario_functional_zone_to_db,
)
from idu_api.urban_api.logic.impl.helpers.projects_geometries import (
    delete_object_geometry_from_db,
    get_context_geometries_from_db,
    get_context_geometries_with_all_objects_from_db,
    get_geometries_by_scenario_id_from_db,
    get_geometries_with_all_objects_by_scenario_id_from_db,
    patch_object_geometry_to_db,
    put_object_geometry_to_db,
)
from idu_api.urban_api.logic.impl.helpers.projects_indicators import (
    add_scenario_indicator_value_to_db,
    delete_scenario_indicator_value_by_id_from_db,
    delete_scenario_indicators_values_by_scenario_id_from_db,
    get_hexagons_with_indicators_by_scenario_id_from_db,
    get_scenario_indicators_values_by_scenario_id_from_db,
    patch_scenario_indicator_value_to_db,
    put_scenario_indicator_value_to_db,
    update_all_indicators_values_by_scenario_id_to_db,
)
from idu_api.urban_api.logic.impl.helpers.projects_objects import (
    add_project_to_db,
    delete_project_from_db,
    get_all_projects_from_db,
    get_full_project_image_from_minio,
    get_full_project_image_url_from_minio,
    get_preview_project_image_from_minio,
    get_preview_projects_images_from_minio,
    get_preview_projects_images_url_from_minio,
    get_project_by_id_from_db,
    get_project_territory_by_id_from_db,
    get_projects_from_db,
    get_projects_territories_from_db,
    get_user_preview_projects_images_from_minio,
    get_user_preview_projects_images_url_from_minio,
    get_user_projects_from_db,
    patch_project_to_db,
    put_project_to_db,
    upload_project_image_to_minio,
)
from idu_api.urban_api.logic.impl.helpers.projects_physical_objects import (
    add_physical_object_with_geometry_to_db,
    delete_physical_object_from_db,
    get_context_physical_objects_from_db,
    get_physical_objects_by_scenario_id_from_db,
    patch_physical_object_to_db,
    put_physical_object_to_db,
    update_physical_objects_by_function_id_to_db,
)
from idu_api.urban_api.logic.impl.helpers.projects_scenarios import (
    add_new_scenario_to_db,
    copy_scenario_to_db,
    delete_scenario_from_db,
    get_scenario_by_id_from_db,
    get_scenarios_by_project_id_from_db,
    patch_scenario_to_db,
    put_scenario_to_db,
)
from idu_api.urban_api.logic.impl.helpers.projects_services import (
    add_service_to_db,
    delete_service_from_db,
    get_context_services_from_db,
    get_services_by_scenario_id_from_db,
    patch_service_to_db,
    put_service_to_db,
)
from idu_api.urban_api.logic.projects import UserProjectService
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


class UserProjectServiceImpl(UserProjectService):  # pylint: disable=too-many-public-methods
    """Service to manipulate projects entities.

    Based on async SQLAlchemy connection.
    """

    def __init__(self, conn: AsyncConnection, logger: structlog.stdlib.BoundLogger):
        self._conn = conn
        self._logger = logger

    async def get_project_by_id(self, project_id: int, user_id: str | None) -> ProjectDTO:
        return await get_project_by_id_from_db(self._conn, project_id, user_id)

    async def get_project_territory_by_id(self, project_id: int, user_id: str | None) -> ProjectTerritoryDTO:
        return await get_project_territory_by_id_from_db(self._conn, project_id, user_id)

    async def get_all_projects(self) -> list[ProjectDTO]:
        return await get_all_projects_from_db(self._conn)

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
        return await get_projects_from_db(
            self._conn,
            user_id,
            only_own,
            is_regional,
            project_type,
            territory_id,
            name,
            created_at,
            order_by,
            ordering,
            paginate,
        )

    async def get_projects_territories(
        self,
        user_id: str | None,
        only_own: bool,
        project_type: Literal["common", "city"] | None,
        territory_id: int | None,
    ) -> list[ProjectWithTerritoryDTO]:
        return await get_projects_territories_from_db(self._conn, user_id, only_own, project_type, territory_id)

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
        return await get_preview_projects_images_from_minio(
            self._conn,
            minio_client,
            user_id,
            only_own,
            is_regional,
            project_type,
            territory_id,
            name,
            created_at,
            order_by,
            ordering,
            page,
            page_size,
            self._logger,
        )

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
        return await get_preview_projects_images_url_from_minio(
            self._conn,
            minio_client,
            user_id,
            only_own,
            is_regional,
            project_type,
            territory_id,
            name,
            created_at,
            order_by,
            ordering,
            page,
            page_size,
            self._logger,
        )

    async def get_user_projects(self, user_id: str, is_regional: bool, territory_id: int | None) -> PageDTO[ProjectDTO]:
        return await get_user_projects_from_db(self._conn, user_id, is_regional, territory_id)

    async def get_user_preview_projects_images(
        self,
        minio_client: AsyncMinioClient,
        user_id: str,
        is_regional: bool,
        territory_id: int | None,
        page: int,
        page_size: int,
    ) -> io.BytesIO:
        return await get_user_preview_projects_images_from_minio(
            self._conn, minio_client, user_id, is_regional, territory_id, page, page_size, self._logger
        )

    async def get_user_preview_projects_images_url(
        self,
        minio_client: AsyncMinioClient,
        user_id: str,
        is_regional: bool,
        territory_id: int | None,
        page: int,
        page_size: int,
    ) -> list[dict[str, int | str]]:
        return await get_user_preview_projects_images_url_from_minio(
            self._conn, minio_client, user_id, is_regional, territory_id, page, page_size, self._logger
        )

    async def add_project(self, project: ProjectPost, user_id: str) -> ProjectDTO:
        return await add_project_to_db(self._conn, project, user_id, logger=self._logger)

    async def put_project(self, project: ProjectPut, project_id: int, user_id: str) -> ProjectDTO:
        return await put_project_to_db(self._conn, project, project_id, user_id)

    async def patch_project(self, project: ProjectPatch, project_id: int, user_id: str) -> ProjectDTO:
        return await patch_project_to_db(self._conn, project, project_id, user_id)

    async def delete_project(self, project_id: int, minio_client: AsyncMinioClient, user_id: str) -> dict:
        return await delete_project_from_db(self._conn, project_id, minio_client, user_id, self._logger)

    async def upload_project_image(
        self, minio_client: AsyncMinioClient, project_id: int, user_id: str, file: bytes
    ) -> dict:
        return await upload_project_image_to_minio(self._conn, minio_client, project_id, user_id, file, self._logger)

    async def get_full_project_image(
        self, minio_client: AsyncMinioClient, project_id: int, user_id: str | None
    ) -> io.BytesIO:
        return await get_full_project_image_from_minio(self._conn, minio_client, project_id, user_id, self._logger)

    async def get_preview_project_image(
        self, minio_client: AsyncMinioClient, project_id: int, user_id: str | None
    ) -> io.BytesIO:
        return await get_preview_project_image_from_minio(self._conn, minio_client, project_id, user_id, self._logger)

    async def get_full_project_image_url(
        self, minio_client: AsyncMinioClient, project_id: int, user_id: str | None
    ) -> str:
        return await get_full_project_image_url_from_minio(self._conn, minio_client, project_id, user_id, self._logger)

    async def get_scenarios_by_project_id(self, project_id: int, user_id: str | None) -> list[ScenarioDTO]:
        return await get_scenarios_by_project_id_from_db(self._conn, project_id, user_id)

    async def get_scenario_by_id(self, scenario_id: int, user_id: str | None) -> ScenarioDTO:
        return await get_scenario_by_id_from_db(self._conn, scenario_id, user_id)

    async def add_scenario(self, scenario: ScenarioPost, user_id: str) -> ScenarioDTO:
        return await add_new_scenario_to_db(self._conn, scenario, user_id)

    async def copy_scenario(self, scenario: ScenarioPost, scenario_id: int, user_id: str) -> ScenarioDTO:
        return await copy_scenario_to_db(self._conn, scenario, scenario_id, user_id)

    async def put_scenario(self, scenario: ScenarioPut, scenario_id: int, user_id) -> ScenarioDTO:
        return await put_scenario_to_db(self._conn, scenario, scenario_id, user_id)

    async def patch_scenario(self, scenario: ScenarioPatch, scenario_id: int, user_id: str) -> ScenarioDTO:
        return await patch_scenario_to_db(self._conn, scenario, scenario_id, user_id)

    async def delete_scenario(self, scenario_id: int, user_id: str) -> dict:
        return await delete_scenario_from_db(self._conn, scenario_id, user_id)

    async def get_physical_objects_by_scenario_id(
        self,
        scenario_id: int,
        user_id: str | None,
        physical_object_type_id: int | None,
        physical_object_function_id: int | None,
    ) -> list[ScenarioPhysicalObjectDTO]:
        return await get_physical_objects_by_scenario_id_from_db(
            self._conn,
            scenario_id,
            user_id,
            physical_object_type_id,
            physical_object_function_id,
        )

    async def get_context_physical_objects(
        self,
        project_id: int,
        user_id: str | None,
        physical_object_type_id: int | None,
        physical_object_function_id: int | None,
    ) -> list[PhysicalObjectDTO]:
        return await get_context_physical_objects_from_db(
            self._conn,
            project_id,
            user_id,
            physical_object_type_id,
            physical_object_function_id,
        )

    async def add_physical_object_with_geometry(
        self,
        physical_object: PhysicalObjectWithGeometryPost,
        scenario_id: int,
        user_id: str,
    ) -> ScenarioUrbanObjectDTO:
        return await add_physical_object_with_geometry_to_db(self._conn, physical_object, scenario_id, user_id)

    async def update_physical_objects_by_function_id(
        self,
        physical_object: list[PhysicalObjectWithGeometryPost],
        scenario_id: int,
        user_id: str,
        physical_object_function_id: int,
    ) -> list[ScenarioUrbanObjectDTO]:
        return await update_physical_objects_by_function_id_to_db(
            self._conn, physical_object, scenario_id, user_id, physical_object_function_id
        )

    async def put_physical_object(
        self,
        physical_object: PhysicalObjectPut,
        scenario_id: int,
        physical_object_id: int,
        is_scenario_object: bool,
        user_id: str,
    ) -> ScenarioPhysicalObjectDTO:
        return await put_physical_object_to_db(
            self._conn, physical_object, scenario_id, physical_object_id, is_scenario_object, user_id
        )

    async def patch_physical_object(
        self,
        physical_object: PhysicalObjectPatch,
        scenario_id: int,
        physical_object_id: int,
        is_scenario_object: bool,
        user_id: str,
    ) -> ScenarioPhysicalObjectDTO:
        return await patch_physical_object_to_db(
            self._conn, physical_object, scenario_id, physical_object_id, is_scenario_object, user_id
        )

    async def delete_physical_object(
        self,
        scenario_id: int,
        physical_object_id: int,
        is_scenario_object: bool,
        user_id: str,
    ) -> dict:
        return await delete_physical_object_from_db(
            self._conn, scenario_id, physical_object_id, is_scenario_object, user_id
        )

    async def get_services_by_scenario_id(
        self,
        scenario_id: int,
        user_id: str | None,
        service_type_id: int | None,
        urban_function_id: int | None,
    ) -> list[ScenarioServiceDTO]:
        return await get_services_by_scenario_id_from_db(
            self._conn,
            scenario_id,
            user_id,
            service_type_id,
            urban_function_id,
        )

    async def get_context_services(
        self,
        project_id: int,
        user_id: str | None,
        service_type_id: int | None,
        urban_function_id: int | None,
    ) -> list[ServiceDTO]:
        return await get_context_services_from_db(self._conn, project_id, user_id, service_type_id, urban_function_id)

    async def add_service(self, service: ScenarioServicePost, scenario_id: int, user_id: str) -> ScenarioUrbanObjectDTO:
        return await add_service_to_db(self._conn, service, scenario_id, user_id)

    async def put_service(
        self,
        service: ServicePut,
        scenario_id: int,
        service_id: int,
        is_scenario_object: bool,
        user_id: str,
    ) -> ScenarioServiceDTO:
        return await put_service_to_db(self._conn, service, scenario_id, service_id, is_scenario_object, user_id)

    async def patch_service(
        self,
        service: ServicePatch,
        scenario_id: int,
        service_id: int,
        is_scenario_object: bool,
        user_id: str,
    ) -> ScenarioServiceDTO:
        return await patch_service_to_db(self._conn, service, scenario_id, service_id, is_scenario_object, user_id)

    async def delete_service(
        self,
        scenario_id: int,
        service_id: int,
        is_scenario_object: bool,
        user_id: str,
    ) -> dict:
        return await delete_service_from_db(self._conn, scenario_id, service_id, is_scenario_object, user_id)

    async def get_geometries_by_scenario_id(
        self,
        scenario_id: int,
        user_id: str | None,
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
        user_id: str | None,
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

    async def get_context_geometries(
        self,
        project_id: int,
        user_id: str | None,
        physical_object_id: int | None,
        service_id: int | None,
    ) -> list[ObjectGeometryDTO]:
        return await get_context_geometries_from_db(
            self._conn,
            project_id,
            user_id,
            physical_object_id,
            service_id,
        )

    async def get_context_geometries_with_all_objects(
        self,
        project_id: int,
        user_id: str | None,
        physical_object_type_id: int | None,
        service_type_id: int | None,
        physical_object_function_id: int | None,
        urban_function_id: int | None,
    ) -> list[GeometryWithAllObjectsDTO]:
        return await get_context_geometries_with_all_objects_from_db(
            self._conn,
            project_id,
            user_id,
            physical_object_type_id,
            service_type_id,
            physical_object_function_id,
            urban_function_id,
        )

    async def put_object_geometry(
        self,
        object_geometry: ObjectGeometryPut,
        scenario_id: int,
        object_geometry_id: int,
        is_scenario_object: bool,
        user_id: str,
    ) -> ScenarioGeometryDTO:
        return await put_object_geometry_to_db(
            self._conn, object_geometry, scenario_id, object_geometry_id, is_scenario_object, user_id
        )

    async def patch_object_geometry(
        self,
        object_geometry: ObjectGeometryPatch,
        scenario_id: int,
        object_geometry_id: int,
        is_scenario_object: bool,
        user_id: str,
    ) -> ScenarioGeometryDTO:
        return await patch_object_geometry_to_db(
            self._conn, object_geometry, scenario_id, object_geometry_id, is_scenario_object, user_id
        )

    async def delete_object_geometry(
        self,
        scenario_id: int,
        object_geometry_id: int,
        is_scenario_object: bool,
        user_id: str,
    ) -> dict:
        return await delete_object_geometry_from_db(
            self._conn, scenario_id, object_geometry_id, is_scenario_object, user_id
        )

    async def get_scenario_indicators_values(
        self,
        scenario_id: int,
        indicator_ids: str | None,
        indicator_group_id: int | None,
        territory_id: int | None,
        hexagon_id: int | None,
        user_id: str | None,
    ) -> list[ScenarioIndicatorValueDTO]:
        return await get_scenario_indicators_values_by_scenario_id_from_db(
            self._conn, scenario_id, indicator_ids, indicator_group_id, territory_id, hexagon_id, user_id
        )

    async def add_scenario_indicator_value(
        self, indicator_value: ScenarioIndicatorValuePost, scenario_id, user_id: str
    ) -> ScenarioIndicatorValueDTO:
        return await add_scenario_indicator_value_to_db(self._conn, indicator_value, scenario_id, user_id)

    async def put_scenario_indicator_value(
        self, indicator_value: ScenarioIndicatorValuePut, scenario_id: int, user_id: str
    ) -> ScenarioIndicatorValueDTO:
        return await put_scenario_indicator_value_to_db(self._conn, indicator_value, scenario_id, user_id)

    async def patch_scenario_indicator_value(
        self,
        indicator_value: ScenarioIndicatorValuePatch,
        scenario_id: int | None,
        indicator_value_id: int,
        user_id: str,
    ) -> ScenarioIndicatorValueDTO:
        return await patch_scenario_indicator_value_to_db(
            self._conn, indicator_value, scenario_id, indicator_value_id, user_id
        )

    async def delete_scenario_indicators_values_by_scenario_id(self, scenario_id: int, user_id: str) -> dict:
        return await delete_scenario_indicators_values_by_scenario_id_from_db(self._conn, scenario_id, user_id)

    async def delete_scenario_indicator_value_by_id(
        self, scenario_id: int | None, indicator_value_id: int, user_id: str
    ) -> dict:
        return await delete_scenario_indicator_value_by_id_from_db(self._conn, scenario_id, indicator_value_id, user_id)

    async def get_hexagons_with_indicators_by_scenario_id(
        self,
        scenario_id: int,
        indicator_ids: str | None,
        indicators_group_id: int | None,
        user_id: str | None,
    ) -> list[HexagonWithIndicatorsDTO]:
        return await get_hexagons_with_indicators_by_scenario_id_from_db(
            self._conn, scenario_id, indicator_ids, indicators_group_id, user_id
        )

    async def update_all_indicators_values_by_scenario_id(self, scenario_id: int, user_id: str) -> dict[str, Any]:
        return await update_all_indicators_values_by_scenario_id_to_db(
            self._conn, scenario_id, user_id, logger=self._logger
        )

    async def get_functional_zones_sources_by_scenario_id(
        self, scenario_id: int, user_id: str | None
    ) -> list[FunctionalZoneSourceDTO]:
        return await get_functional_zones_sources_by_scenario_id_from_db(self._conn, scenario_id, user_id)

    async def get_functional_zones_by_scenario_id(
        self,
        scenario_id: int,
        year: int,
        source: str,
        functional_zone_type_id: int | None,
        user_id: str | None,
    ) -> list[ScenarioFunctionalZoneDTO]:
        return await get_functional_zones_by_scenario_id_from_db(
            self._conn, scenario_id, year, source, functional_zone_type_id, user_id
        )

    async def get_context_functional_zones_sources(
        self, project_id: int, user_id: str | None
    ) -> list[FunctionalZoneSourceDTO]:
        return await get_context_functional_zones_sources_from_db(self._conn, project_id, user_id)

    async def get_context_functional_zones(
        self,
        project_id: int,
        year: int,
        source: str,
        functional_zone_type_id: int | None,
        user_id: str | None,
    ) -> list[FunctionalZoneDTO]:
        return await get_context_functional_zones_from_db(
            self._conn, project_id, year, source, functional_zone_type_id, user_id
        )

    async def add_scenario_functional_zones(
        self, profiles: list[ScenarioFunctionalZonePost], scenario_id: int, user_id: str
    ) -> list[ScenarioFunctionalZoneDTO]:
        return await add_scenario_functional_zones_to_db(self._conn, profiles, scenario_id, user_id)

    async def put_scenario_functional_zone(
        self,
        profile: ScenarioFunctionalZonePut,
        scenario_id: int,
        functional_zone_id: int,
        user_id: str,
    ) -> ScenarioFunctionalZoneDTO:
        return await put_scenario_functional_zone_to_db(self._conn, profile, scenario_id, functional_zone_id, user_id)

    async def patch_scenario_functional_zone(
        self,
        profile: ScenarioFunctionalZonePatch,
        scenario_id: int,
        functional_zone_id: int,
        user_id: str,
    ) -> ScenarioFunctionalZoneDTO:
        return await patch_scenario_functional_zone_to_db(self._conn, profile, scenario_id, functional_zone_id, user_id)

    async def delete_functional_zones_by_scenario_id(self, scenario_id: int, user_id: str) -> dict:
        return await delete_functional_zones_by_scenario_id_from_db(self._conn, scenario_id, user_id)
