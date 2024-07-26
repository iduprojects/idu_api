from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.city_api.dto.services import CityServiceDTO
from idu_api.city_api.services.objects.urban_objects import get_services_by_territory_ids
from idu_api.city_api.services.territories.territories import get_territory_ids_by_parent_id


class ServicesService:
    def __init__(self, conn: AsyncConnection):
        self.conn = conn

    async def get_services_by_territory_id(self, territory_id: int) -> list[CityServiceDTO]:
        children: list[int] = await get_territory_ids_by_parent_id(self.conn, territory_id)
        result: list[CityServiceDTO] = await get_services_by_territory_ids(self.conn, children)
        return result
