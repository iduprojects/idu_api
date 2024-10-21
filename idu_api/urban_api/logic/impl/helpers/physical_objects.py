"""Physical objects handlers logic of getting entities from the database is defined here."""

from datetime import datetime, timezone
from typing import Callable

from geoalchemy2 import Geography, Geometry
from geoalchemy2.functions import ST_AsGeoJSON, ST_GeomFromText
from shapely.geometry import LineString, MultiPolygon, Point, Polygon
from sqlalchemy import cast, delete, func, insert, select, text, update
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import (
    living_buildings_data,
    object_geometries_data,
    physical_object_functions_dict,
    physical_object_types_dict,
    physical_objects_data,
    service_types_dict,
    services_data,
    territories_data,
    territory_types_dict,
    urban_functions_dict,
    urban_objects_data,
)
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
from idu_api.urban_api.exceptions.logic.common import (
    EntitiesNotFoundByIds,
    EntityNotFoundById,
    TooManyObjectsError,
)
from idu_api.urban_api.logic.impl.helpers.urban_objects import get_urban_object_by_id_from_db
from idu_api.urban_api.schemas import (
    LivingBuildingsDataPatch,
    LivingBuildingsDataPost,
    LivingBuildingsDataPut,
    PhysicalObjectsDataPatch,
    PhysicalObjectsDataPost,
    PhysicalObjectsDataPut,
    PhysicalObjectWithGeometryPost,
)

func: Callable
Geom = Point | Polygon | MultiPolygon | LineString

OBJECTS_NUMBER_LIMIT = 20_000


async def get_physical_objects_by_ids_from_db(
    conn: AsyncConnection, ids: list[int]
) -> list[PhysicalObjectWithGeometryDTO]:
    """Get physical objects by list of ids."""

    if len(ids) > OBJECTS_NUMBER_LIMIT:
        raise TooManyObjectsError(len(ids), OBJECTS_NUMBER_LIMIT)

    statement = (
        select(
            physical_objects_data,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            physical_object_types_dict.c.physical_object_function_id,
            physical_object_functions_dict.c.name.label("physical_object_function_name"),
            object_geometries_data.c.address,
            object_geometries_data.c.osm_id,
            cast(ST_AsGeoJSON(object_geometries_data.c.geometry), JSONB).label("geometry"),
            cast(ST_AsGeoJSON(object_geometries_data.c.centre_point), JSONB).label("centre_point"),
        )
        .select_from(
            physical_objects_data.join(
                urban_objects_data,
                physical_objects_data.c.physical_object_id == urban_objects_data.c.physical_object_id,
            )
            .join(
                object_geometries_data,
                urban_objects_data.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
            )
            .join(
                physical_object_types_dict,
                physical_objects_data.c.physical_object_type_id == physical_object_types_dict.c.physical_object_type_id,
            )
            .join(
                physical_object_functions_dict,
                physical_object_functions_dict.c.physical_object_function_id
                == physical_object_types_dict.c.physical_object_function_id,
            )
        )
        .where(physical_objects_data.c.physical_object_id.in_(ids))
        .distinct()
    )

    results = (await conn.execute(statement)).mappings().all()
    if not list(results):
        raise EntitiesNotFoundByIds("physical_object")

    return [PhysicalObjectWithGeometryDTO(**physical_object) for physical_object in results]


async def get_physical_objects_around_from_db(
    conn: AsyncConnection, geometry: Geom, physical_object_type_id: int | None, buffer_meters: int
) -> list[PhysicalObjectWithGeometryDTO]:
    """Get physical objects which are in buffer area of the given geometry."""

    buffered_geometry_cte = select(
        cast(
            func.ST_Buffer(cast(ST_GeomFromText(str(geometry.wkt), text("4326")), Geography(srid=4326)), buffer_meters),
            Geometry(srid=4326),
        ).label("geometry"),
    ).cte("buffered_geometry_cte")

    fine_territories_cte = (
        select(territories_data.c.territory_id.label("territory_id"))
        .where(
            func.ST_CoveredBy(territories_data.c.geometry, select(buffered_geometry_cte.c.geometry).scalar_subquery())
        )
        .cte("fine_territories_cte")
    )

    possible_territories_cte = (
        select(territories_data.c.territory_id.label("territory_id"))
        .where(
            func.ST_Intersects(territories_data.c.geometry, select(buffered_geometry_cte.c.geometry).scalar_subquery())
            | func.ST_Covers(territories_data.c.geometry, select(buffered_geometry_cte.c.geometry).scalar_subquery())
        )
        .cte("possible_territories_cte")
    )

    statement = (
        select(physical_objects_data.c.physical_object_id)
        .select_from(
            physical_objects_data.join(
                urban_objects_data,
                urban_objects_data.c.physical_object_id == physical_objects_data.c.physical_object_id,
            ).join(
                object_geometries_data,
                urban_objects_data.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
            )
        )
        .where(
            object_geometries_data.c.territory_id.in_(select(fine_territories_cte.c.territory_id).scalar_subquery())
            | object_geometries_data.c.territory_id.in_(
                select(possible_territories_cte.c.territory_id).scalar_subquery()
            )
            & (
                func.ST_Intersects(
                    object_geometries_data.c.geometry, select(buffered_geometry_cte.c.geometry).scalar_subquery()
                )
                | func.ST_Covers(
                    object_geometries_data.c.geometry, select(buffered_geometry_cte.c.geometry).scalar_subquery()
                )
                | func.ST_CoveredBy(
                    object_geometries_data.c.geometry, select(buffered_geometry_cte.c.geometry).scalar_subquery()
                )
            ),
        )
        .distinct()
    )
    if physical_object_type_id is not None:
        statement = statement.where(physical_objects_data.c.physical_object_type_id == physical_object_type_id)

    ids = (await conn.execute(statement)).scalars().all()

    if len(ids) == 0:
        return []

    return await get_physical_objects_by_ids_from_db(conn, ids)


async def get_physical_object_by_id_from_db(conn: AsyncConnection, physical_object_id: int) -> PhysicalObjectDataDTO:
    """Get physical object by id."""

    statement = (
        select(
            physical_objects_data,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            physical_object_types_dict.c.physical_object_function_id,
            physical_object_functions_dict.c.name.label("physical_object_function_name"),
        )
        .select_from(
            physical_objects_data.join(
                physical_object_types_dict,
                physical_objects_data.c.physical_object_type_id == physical_object_types_dict.c.physical_object_type_id,
            ).join(
                physical_object_functions_dict,
                physical_object_functions_dict.c.physical_object_function_id
                == physical_object_types_dict.c.physical_object_function_id,
            )
        )
        .where(physical_objects_data.c.physical_object_id == physical_object_id)
    )

    result = (await conn.execute(statement)).mappings().one_or_none()
    if result is None:
        raise EntityNotFoundById(physical_object_id, "physical object")

    return PhysicalObjectDataDTO(**result)


async def add_physical_object_with_geometry_to_db(
    conn: AsyncConnection, physical_object: PhysicalObjectWithGeometryPost
) -> UrbanObjectDTO:
    """Create physical object with geometry."""

    statement = select(territories_data).where(territories_data.c.territory_id == physical_object.territory_id)
    territory = (await conn.execute(statement)).one_or_none()
    if territory is None:
        raise EntityNotFoundById(physical_object.territory_id, "territory")

    statement = select(physical_object_types_dict).where(
        physical_object_types_dict.c.physical_object_type_id == physical_object.physical_object_type_id
    )
    physical_object_type = (await conn.execute(statement)).one_or_none()
    if physical_object_type is None:
        raise EntityNotFoundById(physical_object.physical_object_type_id, "physical object type")

    statement = (
        insert(physical_objects_data)
        .values(
            physical_object_type_id=physical_object.physical_object_type_id,
            name=physical_object.name,
            properties=physical_object.properties,
        )
        .returning(physical_objects_data.c.physical_object_id)
    )
    physical_object_id = (await conn.execute(statement)).scalar_one()

    statement = (
        insert(object_geometries_data)
        .values(
            territory_id=physical_object.territory_id,
            geometry=ST_GeomFromText(str(physical_object.geometry.as_shapely_geometry()), text("4326")),
            centre_point=ST_GeomFromText(str(physical_object.centre_point.as_shapely_geometry()), text("4326")),
            address=physical_object.address,
            osm_id=physical_object.osm_id,
        )
        .returning(object_geometries_data.c.object_geometry_id)
    )
    object_geometry_id = (await conn.execute(statement)).scalar_one()

    statement = (
        insert(urban_objects_data)
        .values(physical_object_id=physical_object_id, object_geometry_id=object_geometry_id)
        .returning(urban_objects_data.c.urban_object_id)
    )

    urban_object_id = (await conn.execute(statement)).scalar_one_or_none()
    await conn.commit()

    return await get_urban_object_by_id_from_db(conn, urban_object_id)


async def put_physical_object_to_db(
    conn: AsyncConnection, physical_object: PhysicalObjectsDataPut, physical_object_id: int
) -> PhysicalObjectDataDTO:
    """Put physical object."""

    statement = select(physical_objects_data).where(physical_objects_data.c.physical_object_id == physical_object_id)
    requested_physical_object = (await conn.execute(statement)).one_or_none()
    if requested_physical_object is None:
        raise EntityNotFoundById(physical_object_id, "physical object")

    statement = select(physical_object_types_dict).where(
        physical_object_types_dict.c.physical_object_type_id == physical_object.physical_object_type_id
    )
    physical_object_type = (await conn.execute(statement)).one_or_none()
    if physical_object_type is None:
        raise EntityNotFoundById(physical_object.physical_object_type_id, "physical object type")

    statement = (
        update(physical_objects_data)
        .where(physical_objects_data.c.physical_object_id == physical_object_id)
        .values(
            physical_object_type_id=physical_object.physical_object_type_id,
            name=physical_object.name,
            properties=physical_object.properties,
            updated_at=datetime.utcnow(),
        )
        .returning(physical_objects_data)
    )

    result = (await conn.execute(statement)).mappings().one()
    await conn.commit()

    return await get_physical_object_by_id_from_db(conn, result.physical_object_id)


async def patch_physical_object_to_db(
    conn: AsyncConnection, physical_object: PhysicalObjectsDataPatch, physical_object_id: int
) -> PhysicalObjectDataDTO:
    """Patch physical object."""

    statement = select(physical_objects_data).where(physical_objects_data.c.physical_object_id == physical_object_id)
    requested_physical_object = (await conn.execute(statement)).one_or_none()
    if requested_physical_object is None:
        raise EntityNotFoundById(physical_object_id, "physical object")

    statement = (
        update(physical_objects_data)
        .where(physical_objects_data.c.physical_object_id == physical_object_id)
        .returning(physical_objects_data)
        .values(updated_at=datetime.now(timezone.utc))
    )

    values_to_update = {}
    for k, v in physical_object.model_dump(exclude_unset=True).items():
        if k == "physical_object_type_id":
            new_statement = select(physical_object_types_dict).where(
                physical_object_types_dict.c.physical_object_type_id == physical_object.physical_object_type_id
            )
            physical_object_type = (await conn.execute(new_statement)).one_or_none()
            if physical_object_type is None:
                raise EntityNotFoundById(physical_object.physical_object_type_id, "physical object type")
        values_to_update.update({k: v})

    statement = statement.values(**values_to_update)
    result = (await conn.execute(statement)).mappings().one()
    await conn.commit()

    return await get_physical_object_by_id_from_db(conn, result.physical_object_id)


async def delete_physical_object_in_db(conn: AsyncConnection, physical_object_id: int) -> dict:
    """Delete physical object."""

    statement = select(physical_objects_data).where(physical_objects_data.c.physical_object_id == physical_object_id)
    requested_physical_object = (await conn.execute(statement)).one_or_none()
    if requested_physical_object is None:
        raise EntityNotFoundById(physical_object_id, "physical object")

    statement = delete(physical_objects_data).where(physical_objects_data.c.physical_object_id == physical_object_id)
    await conn.execute(statement)
    await conn.commit()

    return {"result": "ok"}


async def get_living_building_by_id_from_db(conn: AsyncConnection, living_building_id: int) -> LivingBuildingsDTO:
    """Get living building object by id."""

    statement = (
        select(
            living_buildings_data.c.living_building_id,
            living_buildings_data.c.residents_number,
            living_buildings_data.c.living_area,
            living_buildings_data.c.properties,
            physical_objects_data.c.physical_object_id,
            physical_objects_data.c.name.label("physical_object_name"),
            physical_objects_data.c.properties.label("physical_object_properties"),
            physical_objects_data.c.created_at.label("physical_object_created_at"),
            physical_objects_data.c.updated_at.label("physical_object_updated_at"),
            physical_object_types_dict.c.physical_object_type_id,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            physical_object_functions_dict.c.physical_object_function_id,
            physical_object_functions_dict.c.name.label("physical_object_function_name"),
        )
        .select_from(
            living_buildings_data.join(
                physical_objects_data,
                physical_objects_data.c.physical_object_id == living_buildings_data.c.physical_object_id,
            )
            .join(
                physical_object_types_dict,
                physical_objects_data.c.physical_object_type_id == physical_object_types_dict.c.physical_object_type_id,
            )
            .join(
                physical_object_functions_dict,
                physical_object_functions_dict.c.physical_object_function_id
                == physical_object_types_dict.c.physical_object_function_id,
            )
            .join(
                urban_objects_data,
                urban_objects_data.c.physical_object_id == physical_objects_data.c.physical_object_id,
            )
            .join(
                object_geometries_data,
                urban_objects_data.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
            )
        )
        .where(living_buildings_data.c.living_building_id == living_building_id)
        .distinct()
    )

    result = (await conn.execute(statement)).mappings().one()

    return LivingBuildingsDTO(**result)


async def add_living_building_to_db(
    conn: AsyncConnection,
    living_building: LivingBuildingsDataPost,
) -> LivingBuildingsDTO:
    """Create living building object."""

    statement = select(physical_objects_data).where(
        physical_objects_data.c.physical_object_id == living_building.physical_object_id
    )
    physical_object = (await conn.execute(statement)).one_or_none()
    if physical_object is None:
        raise EntityNotFoundById(living_building.physical_object_id, "physical object")

    statement = (
        insert(living_buildings_data)
        .values(
            physical_object_id=living_building.physical_object_id,
            residents_number=living_building.residents_number,
            living_area=living_building.living_area,
            properties=living_building.properties,
        )
        .returning(living_buildings_data.c.living_building_id)
    )

    living_building_id = (await conn.execute(statement)).scalar_one()

    await conn.commit()

    return await get_living_building_by_id_from_db(conn, living_building_id)


async def put_living_building_to_db(
    conn: AsyncConnection, living_building: LivingBuildingsDataPut, living_building_id: int
) -> LivingBuildingsDTO:
    """Put living building object."""

    statement = select(living_buildings_data).where(living_buildings_data.c.living_building_id == living_building_id)
    requested_living_building = (await conn.execute(statement)).one_or_none()
    if requested_living_building is None:
        raise EntityNotFoundById(living_building_id, "living building")

    statement = select(physical_objects_data).where(
        physical_objects_data.c.physical_object_id == living_building.physical_object_id
    )
    physical_object = (await conn.execute(statement)).one_or_none()
    if physical_object is None:
        raise EntityNotFoundById(living_building.physical_object_id, "physical object")

    statement = (
        update(living_buildings_data)
        .where(living_buildings_data.c.living_building_id == living_building_id)
        .values(
            physical_object_id=living_building.physical_object_id,
            residents_number=living_building.residents_number,
            living_area=living_building.living_area,
            properties=living_building.properties,
        )
        .returning(living_buildings_data)
    )

    result = (await conn.execute(statement)).mappings().one()
    await conn.commit()

    return await get_living_building_by_id_from_db(conn, result.living_building_id)


async def patch_living_building_to_db(
    conn: AsyncConnection, living_building: LivingBuildingsDataPatch, living_building_id: int
) -> LivingBuildingsDTO:
    """Patch living building object."""

    statement = select(living_buildings_data).where(living_buildings_data.c.living_building_id == living_building_id)
    requested_living_building = (await conn.execute(statement)).one_or_none()
    if requested_living_building is None:
        raise EntityNotFoundById(living_building_id, "living building")

    statement = (
        update(living_buildings_data)
        .where(living_buildings_data.c.living_building_id == living_building_id)
        .returning(living_buildings_data)
    )

    values_to_update = {}
    for k, v in living_building.model_dump(exclude_unset=True).items():
        if k == "physical_object_id":
            new_statement = select(physical_objects_data).where(
                physical_objects_data.c.physical_object_id == living_building.physical_object_id
            )
            physical_object = (await conn.execute(new_statement)).one_or_none()
            if physical_object is None:
                raise EntityNotFoundById(living_building.physical_object_id, "physical object")
        values_to_update.update({k: v})

    statement = statement.values(**values_to_update)
    result = (await conn.execute(statement)).mappings().one()
    await conn.commit()

    return await get_living_building_by_id_from_db(conn, result.living_building_id)


async def delete_living_building_in_db(conn: AsyncConnection, living_building_id: int) -> dict:
    """Delete living building object."""

    statement = select(living_buildings_data).where(living_buildings_data.c.living_building_id == living_building_id)
    requested_living_building = (await conn.execute(statement)).one_or_none()
    if requested_living_building is None:
        raise EntityNotFoundById(living_building_id, "living building")

    statement = delete(living_buildings_data).where(living_buildings_data.c.living_building_id == living_building_id)
    await conn.execute(statement)
    await conn.commit()

    return {"result": "ok"}


async def get_living_buildings_by_physical_object_id_from_db(
    conn: AsyncConnection,
    physical_object_id: int,
) -> list[LivingBuildingsDTO]:
    """Get living building or list of living buildings by physical object id."""

    statement = select(physical_objects_data).where(physical_objects_data.c.physical_object_id == physical_object_id)
    physical_object = (await conn.execute(statement)).one_or_none()
    if physical_object is None:
        raise EntityNotFoundById(physical_object_id, "physical object")

    statement = (
        select(
            living_buildings_data.c.living_building_id,
            living_buildings_data.c.residents_number,
            living_buildings_data.c.living_area,
            living_buildings_data.c.properties,
            physical_objects_data.c.physical_object_id,
            physical_objects_data.c.name.label("physical_object_name"),
            physical_objects_data.c.properties.label("physical_object_properties"),
            physical_objects_data.c.created_at.label("physical_object_created_at"),
            physical_objects_data.c.updated_at.label("physical_object_updated_at"),
            physical_object_types_dict.c.physical_object_type_id,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            physical_object_functions_dict.c.physical_object_function_id,
            physical_object_functions_dict.c.name.label("physical_object_function_name"),
        )
        .select_from(
            living_buildings_data.join(
                physical_objects_data,
                physical_objects_data.c.physical_object_id == living_buildings_data.c.physical_object_id,
            )
            .join(
                physical_object_types_dict,
                physical_objects_data.c.physical_object_type_id == physical_object_types_dict.c.physical_object_type_id,
            )
            .join(
                physical_object_functions_dict,
                physical_object_functions_dict.c.physical_object_function_id
                == physical_object_types_dict.c.physical_object_function_id,
            )
            .join(
                urban_objects_data,
                urban_objects_data.c.physical_object_id == physical_objects_data.c.physical_object_id,
            )
            .join(
                object_geometries_data,
                urban_objects_data.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
            )
        )
        .where(living_buildings_data.c.physical_object_id == physical_object_id)
        .distinct()
    )

    result = (await conn.execute(statement)).mappings().all()

    return [LivingBuildingsDTO(**building) for building in result]


async def get_services_by_physical_object_id_from_db(
    conn: AsyncConnection,
    physical_object_id: int,
    service_type_id: int | None,
    territory_type_id: int | None,
) -> list[ServiceDTO]:
    """Get service or list of services by physical object id.

    Could be specified by service type id and territory type id.
    """

    statement = select(physical_objects_data).where(physical_objects_data.c.physical_object_id == physical_object_id)
    physical_object = (await conn.execute(statement)).one_or_none()
    if physical_object is None:
        raise EntityNotFoundById(physical_object_id, "physical object")

    statement = (
        select(
            services_data,
            service_types_dict.c.urban_function_id,
            urban_functions_dict.c.name.label("urban_function_name"),
            service_types_dict.c.name.label("service_type_name"),
            service_types_dict.c.capacity_modeled.label("service_type_capacity_modeled"),
            service_types_dict.c.code.label("service_type_code"),
            service_types_dict.c.infrastructure_type,
            service_types_dict.c.properties.label("service_type_properties"),
            territory_types_dict.c.name.label("territory_type_name"),
        )
        .select_from(
            urban_objects_data.join(services_data, services_data.c.service_id == urban_objects_data.c.service_id)
            .join(service_types_dict, service_types_dict.c.service_type_id == services_data.c.service_type_id)
            .join(
                urban_functions_dict,
                urban_functions_dict.c.urban_function_id == service_types_dict.c.urban_function_id,
            )
            .outerjoin(
                territory_types_dict, territory_types_dict.c.territory_type_id == services_data.c.territory_type_id
            )
        )
        .where(urban_objects_data.c.physical_object_id == physical_object_id)
    ).distinct()

    if service_type_id is not None:
        statement = statement.where(services_data.c.service_type_id == service_type_id)

    if territory_type_id is not None:
        statement = statement.where(territory_types_dict.c.territory_type_id == territory_type_id)

    result = (await conn.execute(statement)).mappings().all()

    return [ServiceDTO(**service) for service in result]


async def get_services_with_geometry_by_physical_object_id_from_db(
    conn: AsyncConnection,
    physical_object_id: int,
    service_type_id: int | None,
    territory_type_id: int | None,
) -> list[ServiceWithGeometryDTO]:
    """Get service or list of services with geometry by physical object id.

    Could be specified by service type id and territory type id.
    """

    statement = select(physical_objects_data).where(physical_objects_data.c.physical_object_id == physical_object_id)
    physical_object = (await conn.execute(statement)).one_or_none()
    if physical_object is None:
        raise EntityNotFoundById(physical_object_id, "physical object")

    statement = (
        select(
            services_data,
            service_types_dict.c.urban_function_id,
            urban_functions_dict.c.name.label("urban_function_name"),
            service_types_dict.c.name.label("service_type_name"),
            service_types_dict.c.capacity_modeled.label("service_type_capacity_modeled"),
            service_types_dict.c.code.label("service_type_code"),
            service_types_dict.c.infrastructure_type,
            service_types_dict.c.properties.label("service_type_properties"),
            territory_types_dict.c.name.label("territory_type_name"),
            object_geometries_data.c.address,
            object_geometries_data.c.osm_id,
            cast(ST_AsGeoJSON(object_geometries_data.c.geometry), JSONB).label("geometry"),
            cast(ST_AsGeoJSON(object_geometries_data.c.centre_point), JSONB).label("centre_point"),
        )
        .select_from(
            services_data.join(urban_objects_data, services_data.c.service_id == urban_objects_data.c.service_id)
            .join(
                object_geometries_data,
                object_geometries_data.c.object_geometry_id == urban_objects_data.c.object_geometry_id,
            )
            .join(service_types_dict, service_types_dict.c.service_type_id == services_data.c.service_type_id)
            .join(
                urban_functions_dict,
                urban_functions_dict.c.urban_function_id == service_types_dict.c.urban_function_id,
            )
            .outerjoin(
                territory_types_dict, territory_types_dict.c.territory_type_id == services_data.c.territory_type_id
            )
        )
        .where(urban_objects_data.c.physical_object_id == physical_object_id)
    ).distinct()

    if service_type_id is not None:
        statement = statement.where(services_data.c.service_type_id == service_type_id)

    if territory_type_id is not None:
        statement = statement.where(territory_types_dict.c.territory_type_id == territory_type_id)

    result = (await conn.execute(statement)).mappings().all()

    return [ServiceWithGeometryDTO(**service) for service in result]


async def get_physical_object_geometries_from_db(
    conn: AsyncConnection,
    physical_object_id: int,
) -> list[ObjectGeometryDTO]:
    """Get geometry or list of geometries by physical object id."""

    statement = select(physical_objects_data).where(physical_objects_data.c.physical_object_id == physical_object_id)
    physical_object = (await conn.execute(statement)).one_or_none()
    if physical_object is None:
        raise EntityNotFoundById(physical_object_id, "physical object")

    statement = (
        select(
            object_geometries_data.c.object_geometry_id,
            object_geometries_data.c.territory_id,
            object_geometries_data.c.address,
            object_geometries_data.c.osm_id,
            cast(ST_AsGeoJSON(object_geometries_data.c.geometry), JSONB).label("geometry"),
            cast(ST_AsGeoJSON(object_geometries_data.c.centre_point), JSONB).label("centre_point"),
            object_geometries_data.c.created_at,
            object_geometries_data.c.updated_at,
        )
        .select_from(
            urban_objects_data.join(
                object_geometries_data,
                urban_objects_data.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
            )
        )
        .where(urban_objects_data.c.physical_object_id == physical_object_id)
        .distinct()
    )

    result = (await conn.execute(statement)).mappings().all()

    return [ObjectGeometryDTO(**geometry) for geometry in result]


async def add_physical_object_to_object_geometry_in_db(
    conn: AsyncConnection, object_geometry_id: int, physical_object: PhysicalObjectsDataPost
) -> UrbanObjectDTO:
    """Create object geometry connected with physical object."""

    statement = select(object_geometries_data).where(object_geometries_data.c.object_geometry_id == object_geometry_id)
    object_geometry = (await conn.execute(statement)).one_or_none()
    if object_geometry is None:
        raise EntityNotFoundById(object_geometry_id, "object geometry")

    statement = select(physical_object_types_dict).where(
        physical_object_types_dict.c.physical_object_type_id == physical_object.physical_object_type_id
    )
    physical_object_type = (await conn.execute(statement)).one_or_none()
    if physical_object_type is None:
        raise EntityNotFoundById(physical_object.physical_object_type_id, "physical object type")

    statement = (
        insert(physical_objects_data)
        .values(
            physical_object_type_id=physical_object.physical_object_type_id,
            name=physical_object.name,
            properties=physical_object.properties,
        )
        .returning(physical_objects_data.c.physical_object_id)
    )
    physical_object_id = (await conn.execute(statement)).scalar_one()

    statement = (
        insert(urban_objects_data)
        .values(physical_object_id=physical_object_id, object_geometry_id=object_geometry_id)
        .returning(urban_objects_data.c.urban_object_id)
    )

    urban_object_id = (await conn.execute(statement)).scalar_one_or_none()
    await conn.commit()

    return await get_urban_object_by_id_from_db(conn, urban_object_id)


async def get_physical_object_with_territories_by_id_from_db(
    conn: AsyncConnection,
    physical_object_id: int,
) -> PhysicalObjectWithTerritoryDTO:
    """Get service object by id."""

    statement = (
        select(
            physical_objects_data,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            physical_object_types_dict.c.physical_object_function_id,
            physical_object_functions_dict.c.name.label("physical_object_function_name"),
        )
        .select_from(
            physical_objects_data.join(
                physical_object_types_dict,
                physical_objects_data.c.physical_object_type_id == physical_object_types_dict.c.physical_object_type_id,
            ).join(
                physical_object_functions_dict,
                physical_object_functions_dict.c.physical_object_function_id
                == physical_object_types_dict.c.physical_object_function_id,
            )
        )
        .where(physical_objects_data.c.physical_object_id == physical_object_id)
    )
    result = (await conn.execute(statement)).mappings().one_or_none()
    if result is None:
        raise EntityNotFoundById(physical_object_id, "physical object")

    statement = (
        select(
            territories_data.c.territory_id,
            territories_data.c.name,
        )
        .select_from(
            physical_objects_data.join(
                urban_objects_data,
                urban_objects_data.c.physical_object_id == physical_objects_data.c.physical_object_id,
            )
            .join(
                object_geometries_data,
                object_geometries_data.c.object_geometry_id == urban_objects_data.c.object_geometry_id,
            )
            .join(territories_data, territories_data.c.territory_id == object_geometries_data.c.territory_id)
        )
        .where(physical_objects_data.c.physical_object_id == physical_object_id)
    ).distinct()

    territories = (await conn.execute(statement)).mappings().all()

    return PhysicalObjectWithTerritoryDTO(**result, territories=territories)
