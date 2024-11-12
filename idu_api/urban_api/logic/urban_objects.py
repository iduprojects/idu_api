"""Urban objects handlers logic of getting entities from the database is defined here."""

import abc
from typing import Protocol

from idu_api.urban_api.dto import UrbanObjectDTO


class UrbanObjectsService(Protocol):
    """Service to manipulate urban objects."""

    @abc.abstractmethod
    async def get_urban_object_by_id(self, urban_object_id: int) -> UrbanObjectDTO:
        """Get urban object by urban object id."""

    @abc.abstractmethod
    async def get_urban_object_by_physical_object_id(self, physical_object_id: int) -> list[UrbanObjectDTO]:
        """Get list of urban objects by physical object id."""

    @abc.abstractmethod
    async def get_urban_object_by_object_geometry_id(self, object_geometry_id: int) -> list[UrbanObjectDTO]:
        """Get list of urban objects by object geometry id."""

    @abc.abstractmethod
    async def get_urban_object_by_service_id(self, service_id: int) -> list[UrbanObjectDTO]:
        """Get list of urban objects by service id."""

    @abc.abstractmethod
    async def delete_urban_object_by_id(self, urban_object_id: int) -> dict:
        """Delete urban object by urban object id."""

    @abc.abstractmethod
    async def get_urban_objects_by_territory_id(
        self, territory_id: int, service_type_id: int | None, physical_object_type_id: int | None
    ) -> list[UrbanObjectDTO]:
        """Get a list of urban objects by territory_id with service_type and physical_object_type filters."""
