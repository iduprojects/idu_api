"""Object geometries handlers logic of getting entities from the database is defined here."""

import abc
from typing import Protocol

from idu_api.urban_api.dto import ObjectGeometryDTO, PhysicalObjectDataDTO, UrbanObjectDTO
from idu_api.urban_api.schemas import ObjectGeometriesPatch, ObjectGeometriesPost, ObjectGeometriesPut


class ObjectGeometriesService(Protocol):
    """Service to manipulate object geometries."""

    @abc.abstractmethod
    async def get_object_geometry_by_ids(self, object_geometry_ids: list[int]) -> list[ObjectGeometryDTO]:
        """Get list of object geometries by list of identifiers."""

    @abc.abstractmethod
    async def put_object_geometry(
        self, object_geometry: ObjectGeometriesPut, object_geometry_id: int
    ) -> ObjectGeometryDTO:
        """Put object geometry."""

    @abc.abstractmethod
    async def patch_object_geometry(
        self, object_geometry: ObjectGeometriesPatch, object_geometry_id: int
    ) -> ObjectGeometryDTO:
        """Patch object geometry."""

    @abc.abstractmethod
    async def delete_object_geometry(self, object_geometry_id: int) -> dict:
        """Delete object geometry."""

    @abc.abstractmethod
    async def add_object_geometry_to_physical_object(
        self, physical_object_id: int, object_geometry: ObjectGeometriesPost
    ) -> UrbanObjectDTO:
        """Create object geometry connected with physical object."""

    @abc.abstractmethod
    async def get_physical_objects_by_object_geometry_id(self, object_geometry_id: int) -> list[PhysicalObjectDataDTO]:
        """Get physical object or list of physical objects by object geometry id."""
