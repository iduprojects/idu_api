from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.urban_api.dto import ProjectDTO, ProjectTerritoryDTO, ScenarioDTO, ScenarioUrbanObjectDTO
from idu_api.urban_api.logic.impl.helpers.projects_objects import (
    add_project_to_db,
    delete_project_from_db,
    get_all_available_projects_from_db,
    get_project_by_id_from_db,
    get_project_territory_by_id_from_db,
    get_user_projects_from_db,
    patch_project_to_db,
    put_project_to_db,
)
from idu_api.urban_api.logic.impl.helpers.projects_scenarios import (
    add_existing_physical_object_to_scenario_in_db,
    add_existing_service_to_scenario_in_db,
    add_physical_object_to_scenario_in_db,
    add_scenario_to_db,
    add_service_to_scenario_in_db,
    delete_scenario_from_db,
    get_scenario_by_id_from_db,
    get_scenarios_by_project_id_from_db,
    patch_scenario_to_db,
    put_scenario_to_db,
)
from idu_api.urban_api.logic.projects import UserProjectService
from idu_api.urban_api.schemas import (
    PhysicalObjectsDataPost,
    PhysicalObjectWithGeometryPost,
    ProjectPatch,
    ProjectPost,
    ProjectPut,
    ScenariosPatch,
    ScenariosPost,
    ScenariosPut,
    ServicesDataPost,
)


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

    async def get_scenarios_by_project_id(self, project_id: int, user_id) -> list[ScenarioDTO]:
        return await get_scenarios_by_project_id_from_db(self._conn, project_id, user_id)

    async def get_scenario_by_id(self, scenario_id: int, user_id) -> ScenarioDTO:
        return await get_scenario_by_id_from_db(self._conn, scenario_id, user_id)

    async def add_scenario(self, scenario: ScenariosPost, user_id: str) -> ScenarioDTO:
        return await add_scenario_to_db(self._conn, scenario, user_id)

    async def put_scenario(self, scenario: ScenariosPut, scenario_id: int, user_id) -> ScenarioDTO:
        return await put_scenario_to_db(self._conn, scenario, scenario_id, user_id)

    async def patch_scenario(self, scenario: ScenariosPatch, scenario_id: int, user_id: str) -> ScenarioDTO:
        return await patch_scenario_to_db(self._conn, scenario, scenario_id, user_id)

    async def delete_scenario(self, scenario_id: int, user_id: str) -> dict:
        return await delete_scenario_from_db(self._conn, scenario_id, user_id)

    async def add_physical_object_to_scenario(
        self, scenario_id: int, physical_object: PhysicalObjectWithGeometryPost, user_id: str
    ) -> ScenarioUrbanObjectDTO:
        return await add_physical_object_to_scenario_in_db(self._conn, scenario_id, physical_object, user_id)

    async def add_existing_physical_object_to_scenario(
        self, scenario_id: int, object_geometry_id: int, physical_object: PhysicalObjectsDataPost, user_id: str
    ) -> ScenarioUrbanObjectDTO:
        return await add_existing_physical_object_to_scenario_in_db(
            self._conn, scenario_id, object_geometry_id, physical_object, user_id
        )

    async def add_service_to_scenario(
        self, scenario_id: int, service: ServicesDataPost, user_id: str
    ) -> ScenarioUrbanObjectDTO:
        return await add_service_to_scenario_in_db(self._conn, scenario_id, service, user_id)

    async def add_existing_service_to_scenario(
        self, scenario_id: int, service_id: int, physical_object_id: int, object_geometry_id: int, user_id: str
    ) -> ScenarioUrbanObjectDTO:
        return await add_existing_service_to_scenario_in_db(
            self._conn, scenario_id, service_id, physical_object_id, object_geometry_id, user_id
        )
