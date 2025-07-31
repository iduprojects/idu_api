"""Buffers handlers logic of getting entities from the database is defined here."""

import abc
from typing import Protocol

from shapely.geometry import LineString, MultiPolygon, Point, Polygon

from idu_api.urban_api.dto import (
    BufferDTO,
    BufferTypeDTO,
    DefaultBufferValueDTO,
)
from idu_api.urban_api.schemas import (
    BufferPut,
    BufferTypePost,
    DefaultBufferValuePost,
    DefaultBufferValuePut,
)

Geom = Point | Polygon | MultiPolygon | LineString


class BufferService(Protocol):
    """Service to manipulate buffer objects."""

    @abc.abstractmethod
    async def get_buffer_types(self) -> list[BufferTypeDTO]:
        """Get all buffer type objects."""

    @abc.abstractmethod
    async def add_buffer_type(self, buffer_type: BufferTypePost) -> BufferTypeDTO:
        """Create buffer type object."""

    @abc.abstractmethod
    async def get_all_default_buffer_values(self) -> list[DefaultBufferValueDTO]:
        """Get a list of all buffer types with default value for each physical object/service type."""

    @abc.abstractmethod
    async def add_default_buffer_value(self, buffer_value: DefaultBufferValuePost) -> DefaultBufferValueDTO:
        """Add a default buffer value."""

    @abc.abstractmethod
    async def put_default_buffer_value(self, buffer_value: DefaultBufferValuePut) -> DefaultBufferValueDTO:
        """Add or update a default buffer value."""

    @abc.abstractmethod
    async def put_buffer(self, buffer: BufferPut) -> BufferDTO:
        """Update buffer by all its attributes."""

    @abc.abstractmethod
    async def delete_buffer(self, buffer_type_id: int, urban_object_id: int) -> dict:
        """Delete buffer by identifier."""
