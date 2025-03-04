"""Service types/urban functions handlers logic of getting entities from the database is defined here."""

import abc
from typing import Protocol

from idu_api.urban_api.dto import ServiceTypeDTO, ServiceTypesHierarchyDTO, UrbanFunctionDTO
from idu_api.urban_api.schemas import (
    ServiceTypePatch,
    ServiceTypePost,
    ServiceTypePut,
    UrbanFunctionPatch,
    UrbanFunctionPost,
    UrbanFunctionPut,
)


class ServiceTypesService(Protocol):
    """Service to manipulate service types objects."""

    @abc.abstractmethod
    async def get_service_types(self, urban_function_id: int | None) -> list[ServiceTypeDTO]:
        """Get all service type objects."""

    @abc.abstractmethod
    async def add_service_type(self, service_type: ServiceTypePost) -> ServiceTypeDTO:
        """Create service type object."""

    @abc.abstractmethod
    async def put_service_type(self, service_type: ServiceTypePut) -> ServiceTypeDTO:
        """Update service type object by getting all its attributes."""

    @abc.abstractmethod
    async def patch_service_type(self, service_type_id: int, service_type: ServiceTypePatch) -> ServiceTypeDTO:
        """Update service type object by getting only given attributes."""

    @abc.abstractmethod
    async def delete_service_type(self, service_type_id: int) -> dict:
        """Delete service type object by id."""

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

    @abc.abstractmethod
    async def put_urban_function(self, urban_function: UrbanFunctionPut) -> UrbanFunctionDTO:
        """Update urban function object by getting all its attributes."""

    @abc.abstractmethod
    async def patch_urban_function(
        self, urban_function_id: int, urban_function: UrbanFunctionPatch
    ) -> UrbanFunctionDTO:
        """Update urban function object by getting only given attributes."""

    @abc.abstractmethod
    async def delete_urban_function(self, urban_function_id: int) -> dict:
        """Delete urban function object by id."""

    @abc.abstractmethod
    async def get_service_types_hierarchy(self, ids: set[int] | None) -> list[ServiceTypesHierarchyDTO]:
        """Get service types hierarchy (from top-level urban function to service type)
        based on a list of required service type ids.

        If the list of identifiers was not passed, it returns the full hierarchy.
        """
