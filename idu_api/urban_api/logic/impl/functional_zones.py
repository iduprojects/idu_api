"""Functional zones handlers logic of getting entities from the database is defined here."""

from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.urban_api.dto import (
    FunctionalZoneDTO,
    FunctionalZoneTypeDTO,
    ProfilesReclamationDataDTO,
    ProfilesReclamationDataMatrixDTO,
)
from idu_api.urban_api.logic.functional_zones import FunctionalZonesService
from idu_api.urban_api.logic.impl.helpers.functional_zones import (
    add_functional_zone_to_db,
    add_functional_zone_type_to_db,
    add_profiles_reclamation_data_to_db,
    delete_functional_zone_from_db,
    delete_profiles_reclamation_data_from_db,
    get_all_sources_from_db,
    get_functional_zone_types_from_db,
    get_profiles_reclamation_data_matrix_from_db,
    patch_functional_zone_to_db,
    put_functional_zone_to_db,
    put_profiles_reclamation_data_to_db,
)
from idu_api.urban_api.schemas import (
    FunctionalZonePatch,
    FunctionalZonePost,
    FunctionalZonePut,
    FunctionalZoneTypePost,
    ProfilesReclamationDataPost,
    ProfilesReclamationDataPut,
)


class FunctionalZonesServiceImpl(FunctionalZonesService):
    """Service to manipulate functional zone objects.

    Based on async SQLAlchemy connection.
    """

    def __init__(self, conn: AsyncConnection):
        self._conn = conn

    async def get_functional_zone_types(self) -> list[FunctionalZoneTypeDTO]:
        return await get_functional_zone_types_from_db(self._conn)

    async def add_functional_zone_type(self, functional_zone_type: FunctionalZoneTypePost) -> FunctionalZoneTypeDTO:
        return await add_functional_zone_type_to_db(self._conn, functional_zone_type)

    async def get_all_sources(self, territory_id: int | None) -> list[int]:
        return await get_all_sources_from_db(self._conn, territory_id)

    async def get_profiles_reclamation_data_matrix(
        self, labels: list[int], territory_id: int | None
    ) -> ProfilesReclamationDataMatrixDTO:
        return await get_profiles_reclamation_data_matrix_from_db(self._conn, labels, territory_id)

    async def add_profiles_reclamation_data(
        self, profiles_reclamation: ProfilesReclamationDataPost
    ) -> ProfilesReclamationDataDTO:
        return await add_profiles_reclamation_data_to_db(self._conn, profiles_reclamation)

    async def put_profiles_reclamation_data(
        self, profiles_reclamation: ProfilesReclamationDataPut
    ) -> ProfilesReclamationDataDTO:
        return await put_profiles_reclamation_data_to_db(self._conn, profiles_reclamation)

    async def delete_profiles_reclamation_data(
        self, source_id: int, target_id: int, territory_id: int | None
    ) -> dict[str, str]:
        return await delete_profiles_reclamation_data_from_db(self._conn, source_id, target_id, territory_id)

    async def add_functional_zone(self, functional_zone: FunctionalZonePost) -> FunctionalZoneDTO:
        return await add_functional_zone_to_db(self._conn, functional_zone)

    async def put_functional_zone(
        self, functional_zone_id: int, functional_zone: FunctionalZonePut
    ) -> FunctionalZoneDTO:
        return await put_functional_zone_to_db(self._conn, functional_zone_id, functional_zone)

    async def patch_functional_zone(
        self, functional_zone_id: int, functional_zone: FunctionalZonePatch
    ) -> FunctionalZoneDTO:
        return await patch_functional_zone_to_db(self._conn, functional_zone_id, functional_zone)

    async def delete_functional_zone(self, functional_zone_id: int) -> dict:
        return await delete_functional_zone_from_db(self._conn, functional_zone_id)
