"""Buffers handlers logic of getting entities from the database is defined here."""

from shapely.geometry import MultiPolygon, Polygon

from idu_api.common.db.connection import PostgresConnectionManager
from idu_api.urban_api.dto import (
    BufferDTO,
    BufferTypeDTO,
    DefaultBufferValueDTO,
)
from idu_api.urban_api.logic.buffers import BufferService
from idu_api.urban_api.logic.impl.helpers.buffers import (
    add_buffer_type_to_db,
    add_default_buffer_value_to_db,
    delete_buffer_from_db,
    get_all_default_buffer_values_from_db,
    get_buffer_types_from_db,
    put_buffer_to_db,
    put_default_buffer_value_to_db,
)
from idu_api.urban_api.schemas import (
    BufferPut,
    BufferTypePost,
    DefaultBufferValuePost,
    DefaultBufferValuePut,
)

Geom = Polygon | MultiPolygon


class BufferServiceImpl(BufferService):
    """Service to manipulate buffer objects.

    Based on async `PostgresConnectionManager`.
    """

    def __init__(self, connection_manager: PostgresConnectionManager):
        self._connection_manager = connection_manager

    async def get_buffer_types(self) -> list[BufferTypeDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_buffer_types_from_db(conn)

    async def add_buffer_type(self, buffer_type: BufferTypePost) -> BufferTypeDTO:
        async with self._connection_manager.get_connection() as conn:
            return await add_buffer_type_to_db(conn, buffer_type)

    async def get_all_default_buffer_values(self) -> list[DefaultBufferValueDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_all_default_buffer_values_from_db(conn)

    async def add_default_buffer_value(self, buffer_value: DefaultBufferValuePost) -> DefaultBufferValueDTO:
        async with self._connection_manager.get_connection() as conn:
            return await add_default_buffer_value_to_db(conn, buffer_value)

    async def put_default_buffer_value(self, buffer_value: DefaultBufferValuePut) -> DefaultBufferValueDTO:
        async with self._connection_manager.get_connection() as conn:
            return await put_default_buffer_value_to_db(conn, buffer_value)

    async def put_buffer(self, buffer: BufferPut) -> BufferDTO:
        async with self._connection_manager.get_connection() as conn:
            return await put_buffer_to_db(conn, buffer)

    async def delete_buffer(self, buffer_type_id: int, urban_object_id: int) -> dict:
        async with self._connection_manager.get_connection() as conn:
            return await delete_buffer_from_db(conn, buffer_type_id, urban_object_id)
