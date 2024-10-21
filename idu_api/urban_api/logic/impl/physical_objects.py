"""Physical objects handlers logic is defined here."""

from shapely.geometry import LineString, MultiPolygon, Point, Polygon
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.urban_api.dto import (
    LivingBuildingsDTO,
    ObjectGeometryDTO,
    PhysicalObjectDataDTO,
    PhysicalObjectWithGeometryDTO,
    PhysicalObjectWithTerritoryDTO,
    ServiceDTO,
    ServiceWithGeometryDTO,
    UrbanObjectDTO,
)
from idu_api.urban_api.logic.impl.helpers.physical_objects import (
    add_living_building_to_db,
    add_physical_object_to_object_geometry_in_db,
    add_physical_object_with_geometry_to_db,
    delete_living_building_in_db,
    delete_physical_object_in_db,
    get_living_buildings_by_physical_object_id_from_db,
    get_physical_object_geometries_from_db,
    get_physical_object_with_territories_by_id_from_db,
    get_physical_objects_around_from_db,
    get_physical_objects_by_ids_from_db,
    get_services_by_physical_object_id_from_db,
    get_services_with_geometry_by_physical_object_id_from_db,
    patch_living_building_to_db,
    patch_physical_object_to_db,
    put_living_building_to_db,
    put_physical_object_to_db,
)
from idu_api.urban_api.logic.physical_objects import PhysicalObjectsService
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


class PhysicalObjectsServiceImpl(PhysicalObjectsService):
    """Service to manipulate physical objects entities.

    Based on async SQLAlchemy connection.
    """

    def __init__(self, conn: AsyncConnection):
        self._conn = conn

    async def get_physical_objects_by_ids(self, ids: list[int]) -> list[PhysicalObjectWithGeometryDTO]:
        return await get_physical_objects_by_ids_from_db(self._conn, ids)

    async def get_physical_objects_around(
        self, geometry: Geom, physical_object_type_id: int | None, buffer_meters: int
    ) -> list[PhysicalObjectWithGeometryDTO]:
        return await get_physical_objects_around_from_db(self._conn, geometry, physical_object_type_id, buffer_meters)

    async def add_physical_object_with_geometry(
        self, physical_object: PhysicalObjectWithGeometryPost
    ) -> UrbanObjectDTO:
        return await add_physical_object_with_geometry_to_db(self._conn, physical_object)

    async def put_physical_object(
        self, physical_object: PhysicalObjectsDataPut, physical_object_id: int
    ) -> PhysicalObjectDataDTO:
        return await put_physical_object_to_db(self._conn, physical_object, physical_object_id)

    async def patch_physical_object(
        self, physical_object: PhysicalObjectsDataPatch, physical_object_id: int
    ) -> PhysicalObjectDataDTO:
        return await patch_physical_object_to_db(self._conn, physical_object, physical_object_id)

    async def delete_physical_object(self, physical_object_id: int) -> dict:
        return await delete_physical_object_in_db(self._conn, physical_object_id)

    async def add_living_building(self, living_building: LivingBuildingsDataPost) -> LivingBuildingsDTO:
        return await add_living_building_to_db(self._conn, living_building)

    async def put_living_building(
        self, living_building: LivingBuildingsDataPut, living_building_id: int
    ) -> LivingBuildingsDTO:
        return await put_living_building_to_db(self._conn, living_building, living_building_id)

    async def patch_living_building(
        self, living_building: LivingBuildingsDataPatch, living_building_id: int
    ) -> LivingBuildingsDTO:
        return await patch_living_building_to_db(self._conn, living_building, living_building_id)

    async def delete_living_building(self, living_building_id: int) -> dict:
        return await delete_living_building_in_db(self._conn, living_building_id)

    async def get_living_buildings_by_physical_object_id(self, physical_object_id: int) -> list[LivingBuildingsDTO]:
        return await get_living_buildings_by_physical_object_id_from_db(self._conn, physical_object_id)

    async def get_services_by_physical_object_id(
        self,
        physical_object_id: int,
        service_type_id: int | None,
        territory_type_id: int | None,
    ) -> list[ServiceDTO]:
        return await get_services_by_physical_object_id_from_db(
            self._conn, physical_object_id, service_type_id, territory_type_id
        )

    async def get_services_with_geometry_by_physical_object_id(
        self,
        physical_object_id: int,
        service_type_id: int | None,
        territory_type_id: int | None,
    ) -> list[ServiceWithGeometryDTO]:
        return await get_services_with_geometry_by_physical_object_id_from_db(
            self._conn, physical_object_id, service_type_id, territory_type_id
        )

    async def get_physical_object_geometries(self, physical_object_id: int) -> list[ObjectGeometryDTO]:
        return await get_physical_object_geometries_from_db(self._conn, physical_object_id)

    async def add_physical_object_to_object_geometry(
        self, object_geometry_id: int, physical_object: PhysicalObjectsDataPost
    ) -> UrbanObjectDTO:
        return await add_physical_object_to_object_geometry_in_db(self._conn, object_geometry_id, physical_object)

    async def get_physical_object_with_territories_by_id(
        self, physical_object_id: int
    ) -> PhysicalObjectWithTerritoryDTO:
        return await get_physical_object_with_territories_by_id_from_db(self._conn, physical_object_id)
