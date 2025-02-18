"""Physical objects handlers logic is defined here."""

from shapely.geometry import LineString, MultiPolygon, Point, Polygon

from idu_api.common.db.connection.manager import PostgresConnectionManager
from idu_api.urban_api.dto import (
    BuildingDTO,
    ObjectGeometryDTO,
    PhysicalObjectDTO,
    PhysicalObjectWithGeometryDTO,
    ServiceDTO,
    ServiceWithGeometryDTO,
    UrbanObjectDTO,
)
from idu_api.urban_api.logic.impl.helpers.physical_objects import (
    add_building_to_db,
    add_physical_object_to_object_geometry_to_db,
    add_physical_object_with_geometry_to_db,
    delete_building_from_db,
    delete_physical_object_from_db,
    get_buildings_by_physical_object_id_from_db,
    get_physical_object_by_id_from_db,
    get_physical_object_geometries_from_db,
    get_physical_objects_around_from_db,
    get_physical_objects_with_geometry_by_ids_from_db,
    get_services_by_physical_object_id_from_db,
    get_services_with_geometry_by_physical_object_id_from_db,
    patch_building_to_db,
    patch_physical_object_to_db,
    put_building_to_db,
    put_physical_object_to_db,
)
from idu_api.urban_api.logic.physical_objects import PhysicalObjectsService
from idu_api.urban_api.schemas import (
    BuildingPatch,
    BuildingPost,
    BuildingPut,
    PhysicalObjectPatch,
    PhysicalObjectPost,
    PhysicalObjectPut,
    PhysicalObjectWithGeometryPost,
)

Geom = Point | Polygon | MultiPolygon | LineString


class PhysicalObjectsServiceImpl(PhysicalObjectsService):
    """Service to manipulate physical objects entities.

    Based on async `PostgresConnectionManager`.
    """

    def __init__(self, connection_manager: PostgresConnectionManager):
        self._connection_manager = connection_manager

    async def get_physical_objects_with_geometry_by_ids(self, ids: list[int]) -> list[PhysicalObjectWithGeometryDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_physical_objects_with_geometry_by_ids_from_db(conn, ids)

    async def get_physical_object_by_id(self, physical_object_id: int) -> PhysicalObjectDTO:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_physical_object_by_id_from_db(conn, physical_object_id)

    async def get_physical_objects_around(
        self, geometry: Geom, physical_object_type_id: int | None, buffer_meters: int
    ) -> list[PhysicalObjectWithGeometryDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_physical_objects_around_from_db(conn, geometry, physical_object_type_id, buffer_meters)

    async def add_physical_object_with_geometry(
        self, physical_object: PhysicalObjectWithGeometryPost
    ) -> UrbanObjectDTO:
        async with self._connection_manager.get_connection() as conn:
            return await add_physical_object_with_geometry_to_db(conn, physical_object)

    async def put_physical_object(
        self, physical_object: PhysicalObjectPut, physical_object_id: int
    ) -> PhysicalObjectDTO:
        async with self._connection_manager.get_connection() as conn:
            return await put_physical_object_to_db(conn, physical_object, physical_object_id)

    async def patch_physical_object(
        self, physical_object: PhysicalObjectPatch, physical_object_id: int
    ) -> PhysicalObjectDTO:
        async with self._connection_manager.get_connection() as conn:
            return await patch_physical_object_to_db(conn, physical_object, physical_object_id)

    async def delete_physical_object(self, physical_object_id: int) -> dict:
        async with self._connection_manager.get_connection() as conn:
            return await delete_physical_object_from_db(conn, physical_object_id)

    async def add_building(self, building: BuildingPost) -> PhysicalObjectDTO:
        async with self._connection_manager.get_connection() as conn:
            return await add_building_to_db(conn, building)

    async def put_building(self, building: BuildingPut) -> PhysicalObjectDTO:
        async with self._connection_manager.get_connection() as conn:
            return await put_building_to_db(conn, building)

    async def patch_building(self, building: BuildingPatch, building_id: int) -> PhysicalObjectDTO:
        async with self._connection_manager.get_connection() as conn:
            return await patch_building_to_db(conn, building, building_id)

    async def delete_building(self, building_id: int) -> dict:
        async with self._connection_manager.get_connection() as conn:
            return await delete_building_from_db(conn, building_id)

    async def get_buildings_by_physical_object_id(self, physical_object_id: int) -> list[BuildingDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_buildings_by_physical_object_id_from_db(conn, physical_object_id)

    async def get_services_by_physical_object_id(
        self,
        physical_object_id: int,
        service_type_id: int | None,
        territory_type_id: int | None,
    ) -> list[ServiceDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_services_by_physical_object_id_from_db(
                conn, physical_object_id, service_type_id, territory_type_id
            )

    async def get_services_with_geometry_by_physical_object_id(
        self,
        physical_object_id: int,
        service_type_id: int | None,
        territory_type_id: int | None,
    ) -> list[ServiceWithGeometryDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_services_with_geometry_by_physical_object_id_from_db(
                conn, physical_object_id, service_type_id, territory_type_id
            )

    async def get_physical_object_geometries(self, physical_object_id: int) -> list[ObjectGeometryDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_physical_object_geometries_from_db(conn, physical_object_id)

    async def add_physical_object_to_object_geometry(
        self, object_geometry_id: int, physical_object: PhysicalObjectPost
    ) -> UrbanObjectDTO:
        async with self._connection_manager.get_connection() as conn:
            return await add_physical_object_to_object_geometry_to_db(conn, object_geometry_id, physical_object)
