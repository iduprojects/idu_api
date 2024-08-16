"""Service types/urban functions handlers logic of getting entities from the database is defined here."""

import abc
from typing import Protocol

from idu_api.urban_api.dto import ServiceTypesDTO, UrbanFunctionDTO
from idu_api.urban_api.schemas import ServiceTypesPost, UrbanFunctionPost


class ServiceTypesService(Protocol):
    """Service to manipulate service types objects."""

    @abc.abstractmethod
    async def get_service_types(self, urban_function_id: int | None) -> list[ServiceTypesDTO]:
        """Get all service type objects."""

    @abc.abstractmethod
    async def add_service_type(self, service_type: ServiceTypesPost) -> ServiceTypesDTO:
        """Create service type object."""

    @abc.abstractmethod
    async def get_urban_functions_by_parent_id(
        self,
        parent_id: int | None,
        name: str | None,
        get_all_subtree: bool,
    ) -> list[UrbanFunctionDTO]:
        """Get an urban function or list of urban functions by parent."""

    @abc.abstractmethod
    async def add_urban_function(self, urban_function: UrbanFunctionPost) -> UrbanFunctionDTO:
        """Create urban function object."""
