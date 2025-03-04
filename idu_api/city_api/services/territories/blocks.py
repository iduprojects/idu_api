from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.city_api.dto.territory import CATerritoryDTO, CATerritoryWithoutGeometryDTO
from idu_api.city_api.services.territories.territories import (
    get_territories_by_parent_id_and_level,
    get_territory_hierarchy_by_parent_id,
)


class BlocksService:  # pylint: disable=too-few-public-methods
    def __init__(self, conn: AsyncConnection):
        self.conn = conn

    async def get_blocks_by_territory_id(
        self, city_id: int | str, no_geometry: bool = False
    ) -> list[CATerritoryDTO | CATerritoryWithoutGeometryDTO]:
        hierarchy = await get_territory_hierarchy_by_parent_id(self.conn, city_id)
        if not (len(hierarchy) != 0 and hierarchy[-1].territory_type_name == "Квартал"):
            return []

        result = await get_territories_by_parent_id_and_level(
            self.conn, city_id, level=hierarchy[-1].level, no_geometry=no_geometry
        )

        return result
