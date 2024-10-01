from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.urban_api.dto import FunctionalZoneTypeDTO
from idu_api.urban_api.logic.functional_zones import FunctionalZonesService
from idu_api.urban_api.logic.impl.helpers.functional_zones import (
    add_functional_zone_type_to_db,
    get_functional_zone_types_from_db,
)
from idu_api.urban_api.schemas import FunctionalZoneTypePost


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
