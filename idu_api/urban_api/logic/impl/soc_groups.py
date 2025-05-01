"""Social groups and values logic of getting entities from the database is defined here."""

from typing import Literal

from idu_api.common.db.connection import PostgresConnectionManager
from idu_api.urban_api.dto import (
    ServiceTypeDTO,
    SocGroupDTO,
    SocGroupIndicatorValueDTO,
    SocGroupWithServiceTypesDTO,
    SocValueDTO,
    SocValueWithSocGroupsDTO,
)
from idu_api.urban_api.logic.impl.helpers.soc_groups import (
    add_service_type_to_social_group_from_db,
    add_social_group_indicator_value_to_db,
    add_social_group_to_db,
    add_social_value_to_db,
    add_value_to_social_group_from_db,
    delete_social_group_from_db,
    delete_social_group_indicator_value_from_db,
    delete_social_value_from_db,
    get_service_types_by_social_value_id_from_db,
    get_social_group_by_id_from_db,
    get_social_group_indicator_values_from_db,
    get_social_groups_from_db,
    get_social_value_by_id_from_db,
    get_social_values_from_db,
    put_social_group_indicator_value_to_db,
)
from idu_api.urban_api.logic.soc_groups import SocGroupsService
from idu_api.urban_api.schemas import (
    SocGroupIndicatorValuePost,
    SocGroupIndicatorValuePut,
    SocGroupPost,
    SocGroupServiceTypePost,
    SocValuePost,
)


class SocGroupsServiceImpl(SocGroupsService):
    """Service to manipulate social groups and its values objects."""

    def __init__(self, connection_manager: PostgresConnectionManager):
        self._connection_manager = connection_manager

    async def get_social_groups(self) -> list[SocGroupDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_social_groups_from_db(conn)

    async def get_social_group_by_id(self, soc_group_id: int) -> SocGroupWithServiceTypesDTO:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_social_group_by_id_from_db(conn, soc_group_id)

    async def add_social_group(self, soc_group: SocGroupPost) -> SocGroupWithServiceTypesDTO:
        async with self._connection_manager.get_connection() as conn:
            return await add_social_group_to_db(conn, soc_group)

    async def add_service_type_to_social_group(
        self, soc_group_id: int, service_type: SocGroupServiceTypePost
    ) -> SocGroupWithServiceTypesDTO:
        async with self._connection_manager.get_connection() as conn:
            return await add_service_type_to_social_group_from_db(conn, soc_group_id, service_type)

    async def delete_social_group(self, soc_group_id: int) -> dict[str, str]:
        async with self._connection_manager.get_connection() as conn:
            return await delete_social_group_from_db(conn, soc_group_id)

    async def get_social_values(self) -> list[SocValueDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_social_values_from_db(conn)

    async def get_social_value_by_id(self, soc_value_id: int) -> SocValueWithSocGroupsDTO:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_social_value_by_id_from_db(conn, soc_value_id)

    async def add_social_value(self, soc_value: SocValuePost) -> SocValueWithSocGroupsDTO:
        async with self._connection_manager.get_connection() as conn:
            return await add_social_value_to_db(conn, soc_value)

    async def add_value_to_social_group(
        self, soc_group_id: int, service_type_id: int, soc_value_id: int
    ) -> SocValueWithSocGroupsDTO:
        async with self._connection_manager.get_connection() as conn:
            return await add_value_to_social_group_from_db(conn, soc_group_id, service_type_id, soc_value_id)

    async def delete_social_value(self, soc_value_id: int) -> dict[str, str]:
        async with self._connection_manager.get_connection() as conn:
            return await delete_social_value_from_db(conn, soc_value_id)

    async def get_social_group_indicator_values(
        self,
        soc_group_id: int,
        soc_value_id: int | None,
        territory_id: int | None,
        year: int | None,
        last_only: bool,
    ) -> list[SocGroupIndicatorValueDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_social_group_indicator_values_from_db(
                conn, soc_group_id, soc_value_id, territory_id, year, last_only
            )

    async def add_social_group_indicator_value(
        self, soc_group_id: int, soc_group_indicator: SocGroupIndicatorValuePost
    ) -> SocGroupIndicatorValueDTO:
        async with self._connection_manager.get_connection() as conn:
            return await add_social_group_indicator_value_to_db(conn, soc_group_id, soc_group_indicator)

    async def put_social_group_indicator_value(
        self, soc_group_id: int, soc_group_indicator: SocGroupIndicatorValuePut
    ) -> SocGroupIndicatorValueDTO:
        async with self._connection_manager.get_connection() as conn:
            return await put_social_group_indicator_value_to_db(conn, soc_group_id, soc_group_indicator)

    async def delete_social_group_indicator_value_from_db(
        self,
        soc_group_id: int,
        soc_value_id: int | None,
        territory_id: int | None,
        year: int | None,
    ) -> dict[str, str]:
        async with self._connection_manager.get_connection() as conn:
            return await delete_social_group_indicator_value_from_db(
                conn, soc_group_id, soc_value_id, territory_id, year
            )

    async def get_service_types_by_social_value_id(
        self, social_value_id: int, ordering: Literal["asc", "desc"] | None
    ) -> list[ServiceTypeDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_service_types_by_social_value_id_from_db(conn, social_value_id, ordering)
