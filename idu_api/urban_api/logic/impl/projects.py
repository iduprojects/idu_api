"""Projects handlers logic is defined here."""

import io
from datetime import date
from typing import Any, Literal

import structlog

from idu_api.common.db.connection.manager import PostgresConnectionManager
from idu_api.urban_api.dto import (
    FunctionalZoneDTO,
    FunctionalZoneSourceDTO,
    HexagonWithIndicatorsDTO,
    ObjectGeometryDTO,
    PageDTO,
    PhysicalObjectDTO,
    PhysicalObjectWithGeometryDTO,
    ProjectDTO,
    ProjectTerritoryDTO,
    ProjectWithTerritoryDTO,
    ScenarioDTO,
    ScenarioFunctionalZoneDTO,
    ScenarioGeometryDTO,
    ScenarioGeometryWithAllObjectsDTO,
    ScenarioIndicatorValueDTO,
    ScenarioPhysicalObjectDTO,
    ScenarioPhysicalObjectWithGeometryDTO,
    ScenarioServiceDTO,
    ScenarioServiceWithGeometryDTO,
    ScenarioUrbanObjectDTO,
    ServiceDTO,
    ServiceWithGeometryDTO,
    UserDTO,
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
    get_preview_projects_images_from_minio,
    get_preview_projects_images_url_from_minio,
    get_project_by_id_from_db,
    get_project_image_from_minio,
    get_project_image_url_from_minio,
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
    add_building_to_db,
    add_physical_object_with_geometry_to_db,
    delete_building_from_db,
    delete_physical_object_from_db,
    get_context_physical_objects_from_db,
    get_context_physical_objects_with_geometry_from_db,
    get_physical_objects_by_scenario_id_from_db,
    get_physical_objects_with_geometry_by_scenario_id_from_db,
    patch_building_to_db,
    patch_physical_object_to_db,
    put_building_to_db,
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
    get_context_services_with_geometry_from_db,
    get_services_by_scenario_id_from_db,
    get_services_with_geometry_by_scenario_id_from_db,
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
    ScenarioBuildingPatch,
    ScenarioBuildingPost,
    ScenarioBuildingPut,
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

    Based on async `PostgresConnectionManager`.
    """

    def __init__(self, connection_manager: PostgresConnectionManager, logger: structlog.stdlib.BoundLogger):
        self._connection_manager = connection_manager
        self._logger = logger

    async def get_project_by_id(self, project_id: int, user: UserDTO | None) -> ProjectDTO:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_project_by_id_from_db(conn, project_id, user)

    async def get_project_territory_by_id(self, project_id: int, user: UserDTO | None) -> ProjectTerritoryDTO:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_project_territory_by_id_from_db(conn, project_id, user)

    async def get_all_projects(self) -> list[ProjectDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_all_projects_from_db(conn)

    async def get_projects(
        self,
        user: UserDTO | None,
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
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_projects_from_db(
                conn,
                user,
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
        user: UserDTO | None,
        only_own: bool,
        project_type: Literal["common", "city"] | None,
        territory_id: int | None,
    ) -> list[ProjectWithTerritoryDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_projects_territories_from_db(conn, user, only_own, project_type, territory_id)

    async def get_preview_projects_images(
        self,
        minio_client: AsyncMinioClient,
        user: UserDTO | None,
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
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_preview_projects_images_from_minio(
                conn,
                minio_client,
                user,
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
        user: UserDTO | None,
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
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_preview_projects_images_url_from_minio(
                conn,
                minio_client,
                user,
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

    async def get_user_projects(
        self, user: UserDTO, is_regional: bool, territory_id: int | None
    ) -> PageDTO[ProjectDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_user_projects_from_db(conn, user, is_regional, territory_id)

    async def get_user_preview_projects_images(
        self,
        minio_client: AsyncMinioClient,
        user: UserDTO,
        is_regional: bool,
        territory_id: int | None,
        page: int,
        page_size: int,
    ) -> io.BytesIO:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_user_preview_projects_images_from_minio(
                conn, minio_client, user, is_regional, territory_id, page, page_size, self._logger
            )

    async def get_user_preview_projects_images_url(
        self,
        minio_client: AsyncMinioClient,
        user: UserDTO,
        is_regional: bool,
        territory_id: int | None,
        page: int,
        page_size: int,
    ) -> list[dict[str, int | str]]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_user_preview_projects_images_url_from_minio(
                conn, minio_client, user, is_regional, territory_id, page, page_size, self._logger
            )

    async def add_project(self, project: ProjectPost, user: UserDTO) -> ProjectDTO:
        async with self._connection_manager.get_connection() as conn:
            return await add_project_to_db(conn, project, user, logger=self._logger)

    async def put_project(self, project: ProjectPut, project_id: int, user: UserDTO) -> ProjectDTO:
        async with self._connection_manager.get_connection() as conn:
            return await put_project_to_db(conn, project, project_id, user)

    async def patch_project(self, project: ProjectPatch, project_id: int, user: UserDTO) -> ProjectDTO:
        async with self._connection_manager.get_connection() as conn:
            return await patch_project_to_db(conn, project, project_id, user)

    async def delete_project(self, project_id: int, minio_client: AsyncMinioClient, user: UserDTO) -> dict:
        async with self._connection_manager.get_connection() as conn:
            return await delete_project_from_db(conn, project_id, minio_client, user, self._logger)

    async def upload_project_image(
        self, minio_client: AsyncMinioClient, project_id: int, user: UserDTO, file: bytes
    ) -> dict:
        async with self._connection_manager.get_connection() as conn:
            return await upload_project_image_to_minio(conn, minio_client, project_id, user, file, self._logger)

    async def get_project_image(
        self,
        minio_client: AsyncMinioClient,
        project_id: int,
        user: UserDTO | None,
        image_type: Literal["origin", "preview"],
    ) -> io.BytesIO:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_project_image_from_minio(conn, minio_client, project_id, user, image_type, self._logger)

    async def get_project_image_url(
        self,
        minio_client: AsyncMinioClient,
        project_id: int,
        user: UserDTO | None,
        image_type: Literal["origin", "preview"],
    ) -> str:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_project_image_url_from_minio(
                conn, minio_client, project_id, user, image_type, self._logger
            )

    async def get_scenarios_by_project_id(self, project_id: int, user: UserDTO | None) -> list[ScenarioDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_scenarios_by_project_id_from_db(conn, project_id, user)

    async def get_scenario_by_id(self, scenario_id: int, user: UserDTO | None) -> ScenarioDTO:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_scenario_by_id_from_db(conn, scenario_id, user)

    async def add_scenario(self, scenario: ScenarioPost, user: UserDTO) -> ScenarioDTO:
        async with self._connection_manager.get_connection() as conn:
            return await add_new_scenario_to_db(conn, scenario, user)

    async def copy_scenario(self, scenario: ScenarioPost, scenario_id: int, user: UserDTO) -> ScenarioDTO:
        async with self._connection_manager.get_connection() as conn:
            return await copy_scenario_to_db(conn, scenario, scenario_id, user)

    async def put_scenario(self, scenario: ScenarioPut, scenario_id: int, user) -> ScenarioDTO:
        async with self._connection_manager.get_connection() as conn:
            return await put_scenario_to_db(conn, scenario, scenario_id, user)

    async def patch_scenario(self, scenario: ScenarioPatch, scenario_id: int, user: UserDTO) -> ScenarioDTO:
        async with self._connection_manager.get_connection() as conn:
            return await patch_scenario_to_db(conn, scenario, scenario_id, user)

    async def delete_scenario(self, scenario_id: int, user: UserDTO) -> dict:
        async with self._connection_manager.get_connection() as conn:
            return await delete_scenario_from_db(conn, scenario_id, user)

    async def get_physical_objects_by_scenario_id(
        self,
        scenario_id: int,
        user: UserDTO | None,
        physical_object_type_id: int | None,
        physical_object_function_id: int | None,
    ) -> list[ScenarioPhysicalObjectDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_physical_objects_by_scenario_id_from_db(
                conn,
                scenario_id,
                user,
                physical_object_type_id,
                physical_object_function_id,
            )

    async def get_physical_objects_with_geometry_by_scenario_id(
        self,
        scenario_id: int,
        user: UserDTO | None,
        physical_object_type_id: int | None,
        physical_object_function_id: int | None,
    ) -> list[ScenarioPhysicalObjectWithGeometryDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_physical_objects_with_geometry_by_scenario_id_from_db(
                conn,
                scenario_id,
                user,
                physical_object_type_id,
                physical_object_function_id,
            )

    async def get_context_physical_objects(
        self,
        project_id: int,
        user: UserDTO | None,
        physical_object_type_id: int | None,
        physical_object_function_id: int | None,
    ) -> list[PhysicalObjectDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_context_physical_objects_from_db(
                conn,
                project_id,
                user,
                physical_object_type_id,
                physical_object_function_id,
            )

    async def get_context_physical_objects_with_geometry(
        self,
        project_id: int,
        user: UserDTO | None,
        physical_object_type_id: int | None,
        physical_object_function_id: int | None,
    ) -> list[PhysicalObjectWithGeometryDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_context_physical_objects_with_geometry_from_db(
                conn,
                project_id,
                user,
                physical_object_type_id,
                physical_object_function_id,
            )

    async def add_physical_object_with_geometry(
        self,
        physical_object: PhysicalObjectWithGeometryPost,
        scenario_id: int,
        user: UserDTO,
    ) -> ScenarioUrbanObjectDTO:
        async with self._connection_manager.get_connection() as conn:
            return await add_physical_object_with_geometry_to_db(conn, physical_object, scenario_id, user)

    async def update_physical_objects_by_function_id(
        self,
        physical_object: list[PhysicalObjectWithGeometryPost],
        scenario_id: int,
        user: UserDTO,
        physical_object_function_id: int,
    ) -> list[ScenarioUrbanObjectDTO]:
        async with self._connection_manager.get_connection() as conn:
            return await update_physical_objects_by_function_id_to_db(
                conn, physical_object, scenario_id, user, physical_object_function_id
            )

    async def put_physical_object(
        self,
        physical_object: PhysicalObjectPut,
        scenario_id: int,
        physical_object_id: int,
        is_scenario_object: bool,
        user: UserDTO,
    ) -> ScenarioPhysicalObjectDTO:
        async with self._connection_manager.get_connection() as conn:
            return await put_physical_object_to_db(
                conn, physical_object, scenario_id, physical_object_id, is_scenario_object, user
            )

    async def patch_physical_object(
        self,
        physical_object: PhysicalObjectPatch,
        scenario_id: int,
        physical_object_id: int,
        is_scenario_object: bool,
        user: UserDTO,
    ) -> ScenarioPhysicalObjectDTO:
        async with self._connection_manager.get_connection() as conn:
            return await patch_physical_object_to_db(
                conn, physical_object, scenario_id, physical_object_id, is_scenario_object, user
            )

    async def delete_physical_object(
        self,
        scenario_id: int,
        physical_object_id: int,
        is_scenario_object: bool,
        user: UserDTO,
    ) -> dict:
        async with self._connection_manager.get_connection() as conn:
            return await delete_physical_object_from_db(conn, scenario_id, physical_object_id, is_scenario_object, user)

    async def add_building(
        self,
        building: ScenarioBuildingPost,
        scenario_id: int,
        user: UserDTO,
    ) -> ScenarioPhysicalObjectDTO:
        async with self._connection_manager.get_connection() as conn:
            return await add_building_to_db(conn, building, scenario_id, user)

    async def put_building(
        self,
        building: ScenarioBuildingPut,
        scenario_id: int,
        user: UserDTO,
    ) -> ScenarioPhysicalObjectDTO:
        async with self._connection_manager.get_connection() as conn:
            return await put_building_to_db(conn, building, scenario_id, user)

    async def patch_building(
        self,
        building: ScenarioBuildingPatch,
        scenario_id: int,
        building_id: int,
        is_scenario_object: bool,
        user: UserDTO,
    ) -> ScenarioPhysicalObjectDTO:
        async with self._connection_manager.get_connection() as conn:
            return await patch_building_to_db(conn, building, scenario_id, building_id, is_scenario_object, user)

    async def delete_building(
        self,
        scenario_id: int,
        building_id: int,
        is_scenario_object: bool,
        user: UserDTO,
    ) -> dict[str, str]:
        async with self._connection_manager.get_connection() as conn:
            return await delete_building_from_db(conn, scenario_id, building_id, is_scenario_object, user)

    async def get_services_by_scenario_id(
        self,
        scenario_id: int,
        user: UserDTO | None,
        service_type_id: int | None,
        urban_function_id: int | None,
    ) -> list[ScenarioServiceDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_services_by_scenario_id_from_db(
                conn,
                scenario_id,
                user,
                service_type_id,
                urban_function_id,
            )

    async def get_services_with_geometry_by_scenario_id(
        self,
        scenario_id: int,
        user: UserDTO | None,
        service_type_id: int | None,
        urban_function_id: int | None,
    ) -> list[ScenarioServiceWithGeometryDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_services_with_geometry_by_scenario_id_from_db(
                conn,
                scenario_id,
                user,
                service_type_id,
                urban_function_id,
            )

    async def get_context_services(
        self,
        project_id: int,
        user: UserDTO | None,
        service_type_id: int | None,
        urban_function_id: int | None,
    ) -> list[ServiceDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_context_services_from_db(conn, project_id, user, service_type_id, urban_function_id)

    async def get_context_services_with_geometry(
        self,
        project_id: int,
        user: UserDTO | None,
        service_type_id: int | None,
        urban_function_id: int | None,
    ) -> list[ServiceWithGeometryDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_context_services_with_geometry_from_db(
                conn, project_id, user, service_type_id, urban_function_id
            )

    async def add_service(
        self, service: ScenarioServicePost, scenario_id: int, user: UserDTO
    ) -> ScenarioUrbanObjectDTO:
        async with self._connection_manager.get_connection() as conn:
            return await add_service_to_db(conn, service, scenario_id, user)

    async def put_service(
        self,
        service: ServicePut,
        scenario_id: int,
        service_id: int,
        is_scenario_object: bool,
        user: UserDTO,
    ) -> ScenarioServiceDTO:
        async with self._connection_manager.get_connection() as conn:
            return await put_service_to_db(conn, service, scenario_id, service_id, is_scenario_object, user)

    async def patch_service(
        self,
        service: ServicePatch,
        scenario_id: int,
        service_id: int,
        is_scenario_object: bool,
        user: UserDTO,
    ) -> ScenarioServiceDTO:
        async with self._connection_manager.get_connection() as conn:
            return await patch_service_to_db(conn, service, scenario_id, service_id, is_scenario_object, user)

    async def delete_service(
        self,
        scenario_id: int,
        service_id: int,
        is_scenario_object: bool,
        user: UserDTO,
    ) -> dict:
        async with self._connection_manager.get_connection() as conn:
            return await delete_service_from_db(conn, scenario_id, service_id, is_scenario_object, user)

    async def get_geometries_by_scenario_id(
        self,
        scenario_id: int,
        user: UserDTO | None,
        physical_object_id: int | None,
        service_id: int | None,
    ) -> list[ScenarioGeometryDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_geometries_by_scenario_id_from_db(
                conn,
                scenario_id,
                user,
                physical_object_id,
                service_id,
            )

    async def get_geometries_with_all_objects_by_scenario_id(
        self,
        scenario_id: int,
        user: UserDTO | None,
        physical_object_type_id: int | None,
        service_type_id: int | None,
        physical_object_function_id: int | None,
        urban_function_id: int | None,
    ) -> list[ScenarioGeometryWithAllObjectsDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_geometries_with_all_objects_by_scenario_id_from_db(
                conn,
                scenario_id,
                user,
                physical_object_type_id,
                service_type_id,
                physical_object_function_id,
                urban_function_id,
            )

    async def get_context_geometries(
        self,
        project_id: int,
        user: UserDTO | None,
        physical_object_id: int | None,
        service_id: int | None,
    ) -> list[ObjectGeometryDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_context_geometries_from_db(
                conn,
                project_id,
                user,
                physical_object_id,
                service_id,
            )

    async def get_context_geometries_with_all_objects(
        self,
        project_id: int,
        user: UserDTO | None,
        physical_object_type_id: int | None,
        service_type_id: int | None,
        physical_object_function_id: int | None,
        urban_function_id: int | None,
    ) -> list[GeometryWithAllObjectsDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_context_geometries_with_all_objects_from_db(
                conn,
                project_id,
                user,
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
        user: UserDTO,
    ) -> ScenarioGeometryDTO:
        async with self._connection_manager.get_connection() as conn:
            return await put_object_geometry_to_db(
                conn, object_geometry, scenario_id, object_geometry_id, is_scenario_object, user
            )

    async def patch_object_geometry(
        self,
        object_geometry: ObjectGeometryPatch,
        scenario_id: int,
        object_geometry_id: int,
        is_scenario_object: bool,
        user: UserDTO,
    ) -> ScenarioGeometryDTO:
        async with self._connection_manager.get_connection() as conn:
            return await patch_object_geometry_to_db(
                conn, object_geometry, scenario_id, object_geometry_id, is_scenario_object, user
            )

    async def delete_object_geometry(
        self,
        scenario_id: int,
        object_geometry_id: int,
        is_scenario_object: bool,
        user: UserDTO,
    ) -> dict:
        async with self._connection_manager.get_connection() as conn:
            return await delete_object_geometry_from_db(conn, scenario_id, object_geometry_id, is_scenario_object, user)

    async def get_scenario_indicators_values(
        self,
        scenario_id: int,
        indicator_ids: str | None,
        indicator_group_id: int | None,
        territory_id: int | None,
        hexagon_id: int | None,
        user: UserDTO | None,
    ) -> list[ScenarioIndicatorValueDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_scenario_indicators_values_by_scenario_id_from_db(
                conn, scenario_id, indicator_ids, indicator_group_id, territory_id, hexagon_id, user
            )

    async def add_scenario_indicator_value(
        self, indicator_value: ScenarioIndicatorValuePost, scenario_id, user: UserDTO
    ) -> ScenarioIndicatorValueDTO:
        async with self._connection_manager.get_connection() as conn:
            return await add_scenario_indicator_value_to_db(conn, indicator_value, scenario_id, user)

    async def put_scenario_indicator_value(
        self, indicator_value: ScenarioIndicatorValuePut, scenario_id: int, user: UserDTO
    ) -> ScenarioIndicatorValueDTO:
        async with self._connection_manager.get_connection() as conn:
            return await put_scenario_indicator_value_to_db(conn, indicator_value, scenario_id, user)

    async def patch_scenario_indicator_value(
        self,
        indicator_value: ScenarioIndicatorValuePatch,
        scenario_id: int | None,
        indicator_value_id: int,
        user: UserDTO,
    ) -> ScenarioIndicatorValueDTO:
        async with self._connection_manager.get_connection() as conn:
            return await patch_scenario_indicator_value_to_db(
                conn, indicator_value, scenario_id, indicator_value_id, user
            )

    async def delete_scenario_indicators_values_by_scenario_id(self, scenario_id: int, user: UserDTO) -> dict:
        async with self._connection_manager.get_connection() as conn:
            return await delete_scenario_indicators_values_by_scenario_id_from_db(conn, scenario_id, user)

    async def delete_scenario_indicator_value_by_id(
        self, scenario_id: int | None, indicator_value_id: int, user: UserDTO
    ) -> dict:
        async with self._connection_manager.get_connection() as conn:
            return await delete_scenario_indicator_value_by_id_from_db(conn, scenario_id, indicator_value_id, user)

    async def get_hexagons_with_indicators_by_scenario_id(
        self,
        scenario_id: int,
        indicator_ids: str | None,
        indicators_group_id: int | None,
        user: UserDTO | None,
    ) -> list[HexagonWithIndicatorsDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_hexagons_with_indicators_by_scenario_id_from_db(
                conn, scenario_id, indicator_ids, indicators_group_id, user
            )

    async def update_all_indicators_values_by_scenario_id(self, scenario_id: int, user: UserDTO) -> dict[str, Any]:
        async with self._connection_manager.get_connection() as conn:
            return await update_all_indicators_values_by_scenario_id_to_db(conn, scenario_id, user, logger=self._logger)

    async def get_functional_zones_sources_by_scenario_id(
        self, scenario_id: int, user: UserDTO | None
    ) -> list[FunctionalZoneSourceDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_functional_zones_sources_by_scenario_id_from_db(conn, scenario_id, user)

    async def get_functional_zones_by_scenario_id(
        self,
        scenario_id: int,
        year: int,
        source: str,
        functional_zone_type_id: int | None,
        user: UserDTO | None,
    ) -> list[ScenarioFunctionalZoneDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_functional_zones_by_scenario_id_from_db(
                conn, scenario_id, year, source, functional_zone_type_id, user
            )

    async def get_context_functional_zones_sources(
        self, project_id: int, user: UserDTO | None
    ) -> list[FunctionalZoneSourceDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_context_functional_zones_sources_from_db(conn, project_id, user)

    async def get_context_functional_zones(
        self,
        project_id: int,
        year: int,
        source: str,
        functional_zone_type_id: int | None,
        user: UserDTO | None,
    ) -> list[FunctionalZoneDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_context_functional_zones_from_db(
                conn, project_id, year, source, functional_zone_type_id, user
            )

    async def add_scenario_functional_zones(
        self, profiles: list[ScenarioFunctionalZonePost], scenario_id: int, user: UserDTO
    ) -> list[ScenarioFunctionalZoneDTO]:
        async with self._connection_manager.get_connection() as conn:
            return await add_scenario_functional_zones_to_db(conn, profiles, scenario_id, user)

    async def put_scenario_functional_zone(
        self,
        profile: ScenarioFunctionalZonePut,
        scenario_id: int,
        functional_zone_id: int,
        user: UserDTO,
    ) -> ScenarioFunctionalZoneDTO:
        async with self._connection_manager.get_connection() as conn:
            return await put_scenario_functional_zone_to_db(conn, profile, scenario_id, functional_zone_id, user)

    async def patch_scenario_functional_zone(
        self,
        profile: ScenarioFunctionalZonePatch,
        scenario_id: int,
        functional_zone_id: int,
        user: UserDTO,
    ) -> ScenarioFunctionalZoneDTO:
        async with self._connection_manager.get_connection() as conn:
            return await patch_scenario_functional_zone_to_db(conn, profile, scenario_id, functional_zone_id, user)

    async def delete_functional_zones_by_scenario_id(self, scenario_id: int, user: UserDTO) -> dict:
        async with self._connection_manager.get_connection() as conn:
            return await delete_functional_zones_by_scenario_id_from_db(conn, scenario_id, user)
