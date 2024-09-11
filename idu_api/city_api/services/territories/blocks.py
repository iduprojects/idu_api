from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.city_api.dto.blocks import BlocksDTO, BlocksWithoutGeometryDTO
from idu_api.city_api.dto.territory import CATerritoryDTO, CATerritoryWithoutGeometryDTO
from idu_api.city_api.services.territories.territories import get_territory_hierarchy_by_parent_id, \
    get_territories_by_parent_id_and_level


class BlocksService:
    def __init__(self, conn: AsyncConnection):
        self.conn = conn

    async def get_blocks_by_city_id(
            self, city_id: int, no_geometry: bool = False
    ) -> list[CATerritoryDTO | CATerritoryWithoutGeometryDTO]:
        hierarchy = await get_territory_hierarchy_by_parent_id(self.conn, city_id)
        if not (len(hierarchy) != 0 and hierarchy[-1].territory_type_name == "Квартал"):
            return []

        result = await get_territories_by_parent_id_and_level(
            self.conn, city_id, level=hierarchy[-1].level, no_geometry=no_geometry
        )

        return result
