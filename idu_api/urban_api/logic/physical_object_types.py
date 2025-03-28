"""Physical object types handlers logic of getting entities from the database is defined here."""

import abc
from typing import Protocol

from idu_api.urban_api.dto import (
    PhysicalObjectFunctionDTO,
    PhysicalObjectTypeDTO,
    PhysicalObjectTypesHierarchyDTO,
    ServiceTypeDTO,
)
from idu_api.urban_api.schemas import (
    PhysicalObjectFunctionPatch,
    PhysicalObjectFunctionPost,
    PhysicalObjectFunctionPut,
    PhysicalObjectTypePatch,
    PhysicalObjectTypePost,
)


class PhysicalObjectTypesService(Protocol):
    """physical object to manipulate physical object types."""

    @abc.abstractmethod
    async def get_physical_object_types(
        self,
        physical_object_function_id: int | None,
        name: str | None,
    ) -> list[PhysicalObjectTypeDTO]:
        """Get all physical object type objects."""

    @abc.abstractmethod
    async def add_physical_object_type(self, physical_object_type: PhysicalObjectTypePost) -> PhysicalObjectTypeDTO:
        """Create physical object type object."""

    @abc.abstractmethod
    async def patch_physical_object_type(
        self, physical_object_type_id: int, physical_object_type: PhysicalObjectTypePatch
    ) -> PhysicalObjectTypeDTO:
        """Update physical object type object by getting only given attributes."""

    @abc.abstractmethod
    async def delete_physical_object_type(self, physical_object_type_id: int) -> dict:
        """Delete physical object type object by id."""

    @abc.abstractmethod
    async def get_physical_object_functions_by_parent_id(
        self,
        parent_id: int | None,
        name: str | None,
        get_all_subtree: bool,
    ) -> list[PhysicalObjectFunctionDTO]:
        """Get a physical object function or list of physical object functions by parent."""

    @abc.abstractmethod
    async def add_physical_object_function(
        self, physical_object_function: PhysicalObjectFunctionPost
    ) -> PhysicalObjectFunctionDTO:
        """Create physical object function object."""

    @abc.abstractmethod
    async def put_physical_object_function(
        self, physical_object_function: PhysicalObjectFunctionPut
    ) -> PhysicalObjectFunctionDTO:
        """Update physical object function object by getting all its attributes."""

    @abc.abstractmethod
    async def patch_physical_object_function(
        self, physical_object_function_id: int, physical_object_function: PhysicalObjectFunctionPatch
    ) -> PhysicalObjectFunctionDTO:
        """Update physical object function object by getting only given attributes."""

    @abc.abstractmethod
    async def delete_physical_object_function(self, physical_object_function_id: int) -> dict:
        """Delete physical object function object by id."""

    @abc.abstractmethod
    async def get_physical_object_types_hierarchy(self, ids: set[int] | None) -> list[PhysicalObjectTypesHierarchyDTO]:
        """Get physical object types hierarchy (from top-level physical object function to physical object type)
        based on a list of required physical object type ids.

        If the list of identifiers was not passed, it returns the full hierarchy.
        """

    @abc.abstractmethod
    async def get_service_types_by_physical_object_type(
        self, physical_object_type_id: int | None
    ) -> list[ServiceTypeDTO]:
        """Get available service types for given physical object type."""
