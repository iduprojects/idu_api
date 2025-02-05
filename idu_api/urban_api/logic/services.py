"""Service handlers logic of getting entities from the database is defined here."""

import abc
from typing import Protocol

from idu_api.urban_api.dto import ServiceDTO, UrbanObjectDTO
from idu_api.urban_api.schemas import ServicePatch, ServicePost, ServicePut


class ServicesDataService(Protocol):
    """Service to manipulate service objects."""

    @abc.abstractmethod
    async def get_service_by_id(self, service_id: int) -> ServiceDTO:
        """Get service object by id with territories."""

    @abc.abstractmethod
    async def add_service(self, service: ServicePost) -> ServiceDTO:
        """Create service object."""

    @abc.abstractmethod
    async def put_service(self, service: ServicePut, service_id: int) -> ServiceDTO:
        """Put service object."""

    @abc.abstractmethod
    async def patch_service(self, service: ServicePatch, service_id: int) -> ServiceDTO:
        """Patch service object."""

    @abc.abstractmethod
    async def delete_service(self, service_id: int) -> dict:
        """Delete service object."""

    @abc.abstractmethod
    async def add_service_to_object(
        self, service_id: int, physical_object_id: int, object_geometry_id: int
    ) -> UrbanObjectDTO:
        """Add existing service to physical object."""
