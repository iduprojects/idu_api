"""Urban objects handlers logic of getting entities from the database is defined here."""

from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.urban_api.dto import UrbanObjectDTO
from idu_api.urban_api.logic.impl.helpers.urban_objects import (
    delete_urban_object_by_id_from_db,
    get_urban_object_by_id_from_db,
    get_urban_object_by_object_geometry_id_from_db,
    get_urban_object_by_physical_object_id_from_db,
    get_urban_object_by_service_id_from_db,
    get_urban_objects_by_territory_id_from_db,
)
from idu_api.urban_api.logic.urban_objects import UrbanObjectsService


class UrbanObjectsServiceImpl(UrbanObjectsService):
    """Service to manipulate urban objects.

    Based on async SQLAlchemy connection.
    """

    def __init__(self, conn: AsyncConnection):
        self._conn = conn

    async def get_urban_object_by_id(self, urban_object_id: int) -> UrbanObjectDTO:
        return await get_urban_object_by_id_from_db(self._conn, urban_object_id)

    async def get_urban_object_by_physical_object_id(self, physical_object_id: int) -> list[UrbanObjectDTO]:
        return await get_urban_object_by_physical_object_id_from_db(self._conn, physical_object_id)

    async def get_urban_object_by_object_geometry_id(self, object_geometry_id: int) -> list[UrbanObjectDTO]:
        return await get_urban_object_by_object_geometry_id_from_db(self._conn, object_geometry_id)

    async def get_urban_object_by_service_id(self, service_id: int) -> list[UrbanObjectDTO]:
        return await get_urban_object_by_service_id_from_db(self._conn, service_id)

    async def delete_urban_object_by_id(self, urban_object_id: int) -> dict:
        return await delete_urban_object_by_id_from_db(self._conn, urban_object_id)

    async def get_urban_objects_by_territory_id(
        self, territory_id: int, service_type_id: int | None, physical_object_type_id: int | None
    ) -> list[UrbanObjectDTO]:
        return await get_urban_objects_by_territory_id_from_db(
            self._conn, territory_id, service_type_id, physical_object_type_id
        )
