from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.city_api.dto.physical_objects import PhysicalObjectsDTO
from idu_api.city_api.services.objects.urban_objects import get_physical_objects_by_territory_ids
from idu_api.city_api.services.territories.territories import get_ca_territory_by_id, get_territory_ids_by_parent_id


class PhysicalObjectsService:  # pylint: disable=too-few-public-methods
    def __init__(self, conn: AsyncConnection):
        self.conn = conn

    async def get_physical_objects_by_territory_id(
        self, city_id: int | str, territory_id: int | str
    ) -> list[PhysicalObjectsDTO]:
        _ = await get_ca_territory_by_id(self.conn, city_id)

        children: list[int] = await get_territory_ids_by_parent_id(self.conn, territory_id)
        result: list[PhysicalObjectsDTO] = await get_physical_objects_by_territory_ids(self.conn, children)
        return result
