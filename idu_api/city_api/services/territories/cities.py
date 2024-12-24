from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.city_api.dto.territory import CATerritoryDTO, CATerritoryWithoutGeometryDTO
from idu_api.city_api.dto.territory_hierarchy import TerritoryHierarchyDTO
from idu_api.city_api.services.territories.territories import (
    get_ca_territory_by_id,
    get_territories_by_parent_id_and_level,
    get_territory_hierarchy,
    get_territory_hierarchy_by_parent_id,
)


class CitiesService:
    def __init__(self, conn: AsyncConnection):
        self.conn = conn

    async def get_city_by_id(
        self, city_id: int | str, no_geometry: bool = False
    ) -> CATerritoryDTO | CATerritoryWithoutGeometryDTO:
        return await get_ca_territory_by_id(self.conn, city_id, no_geometry)

    async def get_hierarchy_by_city_id(self, city_id: int | str) -> list[TerritoryHierarchyDTO]:
        return await get_territory_hierarchy_by_parent_id(self.conn, city_id)

    async def get_territories_by_city_id_level_and_type(
        self, city_id: int | str, level: int, type: int, no_geometry: bool = False  # pylint: disable=redefined-builtin
    ) -> list[CATerritoryDTO | CATerritoryWithoutGeometryDTO]:
        _ = await get_ca_territory_by_id(self.conn, city_id, no_geometry)

        return await get_territories_by_parent_id_and_level(
            self.conn, city_id, level=level, type=type, no_geometry=no_geometry
        )

    async def get_city_hierarchy(self, city_id: int | str, no_geometry: bool = False) -> dict:
        return await get_territory_hierarchy(self.conn, city_id, no_geometry)
