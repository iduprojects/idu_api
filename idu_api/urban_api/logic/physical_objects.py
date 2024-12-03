"""Physical objects handlers logic of getting entities from the database is defined here."""

import abc
from typing import Protocol

from shapely.geometry import LineString, MultiPolygon, Point, Polygon

from idu_api.urban_api.dto import (
    LivingBuildingDTO,
    ObjectGeometryDTO,
    PhysicalObjectDataDTO,
    PhysicalObjectWithGeometryDTO,
    PhysicalObjectWithTerritoryDTO,
    ServiceDTO,
    ServiceWithGeometryDTO,
    UrbanObjectDTO,
)
from idu_api.urban_api.schemas import (
    LivingBuildingsDataPatch,
    LivingBuildingsDataPost,
    LivingBuildingsDataPut,
    PhysicalObjectsDataPatch,
    PhysicalObjectsDataPost,
    PhysicalObjectsDataPut,
    PhysicalObjectWithGeometryPost,
)

Geom = Point | Polygon | MultiPolygon | LineString


class PhysicalObjectsService(Protocol):
    """Service to manipulate physical objects."""

    @abc.abstractmethod
    async def get_physical_objects_by_ids(self, ids: list[int]) -> list[PhysicalObjectWithGeometryDTO]:
        """Get physical objects by list of ids."""

    @abc.abstractmethod
    async def get_physical_objects_around(
        self, geometry: Geom, physical_object_type_id: int | None, buffer_meters: int
    ) -> list[PhysicalObjectWithGeometryDTO]:
        """Get physical objects which are in buffer area of the given geometry."""

    @abc.abstractmethod
    async def add_physical_object_with_geometry(
        self, physical_object: PhysicalObjectWithGeometryPost
    ) -> UrbanObjectDTO:
        """Create physical object with geometry."""

    @abc.abstractmethod
    async def put_physical_object(
        self, physical_object: PhysicalObjectsDataPut, physical_object_id: int
    ) -> PhysicalObjectDataDTO:
        """Put physical object."""

    @abc.abstractmethod
    async def patch_physical_object(
        self, physical_object: PhysicalObjectsDataPatch, physical_object_id: int
    ) -> PhysicalObjectDataDTO:
        """Patch physical object."""

    @abc.abstractmethod
    async def delete_physical_object(self, physical_object_id: int) -> dict:
        """Delete physical object."""

    @abc.abstractmethod
    async def add_living_building(self, living_building: LivingBuildingsDataPost) -> LivingBuildingDTO:
        """Create living building object."""

    @abc.abstractmethod
    async def put_living_building(
        self, living_building: LivingBuildingsDataPut, living_building_id: int
    ) -> LivingBuildingDTO:
        """Put living building object."""

    @abc.abstractmethod
    async def patch_living_building(
        self, living_building: LivingBuildingsDataPatch, living_building_id: int
    ) -> LivingBuildingDTO:
        """Patch living building object."""

    @abc.abstractmethod
    async def delete_living_building(self, living_building_id: int) -> dict:
        """Delete living building object."""

    @abc.abstractmethod
    async def get_living_buildings_by_physical_object_id(self, physical_object_id: int) -> list[LivingBuildingDTO]:
        """Get living building or list of living buildings by physical object id."""

    @abc.abstractmethod
    async def get_services_by_physical_object_id(
        self,
        physical_object_id: int,
        service_type_id: int | None,
        territory_type_id: int | None,
    ) -> list[ServiceDTO]:
        """Get service or list of services by physical object id.

        Could be specified by service type id and territory type id.
        """

    @abc.abstractmethod
    async def get_services_with_geometry_by_physical_object_id(
        self,
        physical_object_id: int,
        service_type_id: int | None,
        territory_type_id: int | None,
    ) -> list[ServiceWithGeometryDTO]:
        """Get service or list of services with geometry by physical object id.

        Could be specified by service type id and territory type id.
        """

    @abc.abstractmethod
    async def get_physical_object_geometries(self, physical_object_id: int) -> list[ObjectGeometryDTO]:
        """Get geometry or list of geometries by physical object id."""

    @abc.abstractmethod
    async def add_physical_object_to_object_geometry(
        self, object_geometry_id: int, physical_object: PhysicalObjectsDataPost
    ) -> UrbanObjectDTO:
        """Create object geometry connected with physical object."""

    @abc.abstractmethod
    async def get_physical_object_with_territories_by_id(
        self, physical_object_id: int
    ) -> PhysicalObjectWithTerritoryDTO:
        """Get service object by id."""
