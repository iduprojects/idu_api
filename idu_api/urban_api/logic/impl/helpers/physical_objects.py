"""Physical objects internal logic is defined here."""

from collections import defaultdict
from typing import Callable

from geoalchemy2 import Geography, Geometry
from geoalchemy2.functions import ST_AsEWKB, ST_GeomFromWKB
from shapely.geometry import LineString, MultiPolygon, Point, Polygon
from sqlalchemy import cast, delete, func, insert, select, text, update
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import (
    buildings_data,
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
    BuildingDTO,
    ObjectGeometryDTO,
    PhysicalObjectDTO,
    PhysicalObjectWithGeometryDTO,
    ServiceDTO,
    ServiceWithGeometryDTO,
    UrbanObjectDTO,
)
from idu_api.urban_api.exceptions.logic.common import (
    EntitiesNotFoundByIds,
    EntityAlreadyExists,
    EntityNotFoundById,
    TooManyObjectsError,
)
from idu_api.urban_api.logic.impl.helpers.urban_objects import get_urban_objects_by_ids_from_db
from idu_api.urban_api.logic.impl.helpers.utils import (
    OBJECTS_NUMBER_LIMIT,
    SRID,
    check_existence,
    extract_values_from_model,
)
from idu_api.urban_api.schemas import (
    BuildingPatch,
    BuildingPost,
    BuildingPut,
    PhysicalObjectPatch,
    PhysicalObjectPost,
    PhysicalObjectPut,
    PhysicalObjectWithGeometryPost,
)

func: Callable
Geom = Point | Polygon | MultiPolygon | LineString


async def get_physical_objects_with_geometry_by_ids_from_db(
    conn: AsyncConnection, ids: list[int]
) -> list[PhysicalObjectWithGeometryDTO]:
    """Get physical objects by list of ids."""

    if len(ids) > OBJECTS_NUMBER_LIMIT:
        raise TooManyObjectsError(len(ids), OBJECTS_NUMBER_LIMIT)

    building_columns = [col for col in buildings_data.c if col.name not in ("physical_object_id", "properties")]

    statement = (
        select(
            physical_objects_data,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            physical_object_types_dict.c.physical_object_function_id,
            physical_object_functions_dict.c.name.label("physical_object_function_name"),
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            object_geometries_data.c.object_geometry_id,
            object_geometries_data.c.address,
            object_geometries_data.c.osm_id,
            ST_AsEWKB(object_geometries_data.c.geometry).label("geometry"),
            ST_AsEWKB(object_geometries_data.c.centre_point).label("centre_point"),
            *building_columns,
            buildings_data.c.properties.label("building_properties"),
        )
        .select_from(
            physical_objects_data.join(
                urban_objects_data,
                urban_objects_data.c.physical_object_id == physical_objects_data.c.physical_object_id,
            )
            .join(
                object_geometries_data,
                urban_objects_data.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
            )
            .join(territories_data, territories_data.c.territory_id == object_geometries_data.c.territory_id)
            .join(
                physical_object_types_dict,
                physical_objects_data.c.physical_object_type_id == physical_object_types_dict.c.physical_object_type_id,
            )
            .join(
                physical_object_functions_dict,
                physical_object_functions_dict.c.physical_object_function_id
                == physical_object_types_dict.c.physical_object_function_id,
            )
            .outerjoin(
                buildings_data,
                buildings_data.c.physical_object_id == physical_objects_data.c.physical_object_id,
            )
        )
        .where(physical_objects_data.c.physical_object_id.in_(ids))
        .distinct()
    )

    results = (await conn.execute(statement)).mappings().all()
    if not results:
        raise EntitiesNotFoundByIds("physical_object")

    return [PhysicalObjectWithGeometryDTO(**physical_object) for physical_object in results]


async def get_physical_objects_around_from_db(
    conn: AsyncConnection, geometry: Geom, physical_object_type_id: int | None, buffer_meters: int
) -> list[PhysicalObjectWithGeometryDTO]:
    """Get physical objects which are in buffer area of the given geometry."""

    buffered_geometry_cte = select(
        cast(
            func.ST_Buffer(cast(ST_GeomFromWKB(geometry.wkb, text(str(SRID))), Geography(srid=SRID)), buffer_meters),
            Geometry(srid=SRID),
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

    return await get_physical_objects_with_geometry_by_ids_from_db(conn, ids)


async def get_physical_object_by_id_from_db(conn: AsyncConnection, physical_object_id: int) -> PhysicalObjectDTO:
    """Get physical object by identifier."""

    building_columns = [col for col in buildings_data.c if col.name not in ("physical_object_id", "properties")]

    statement = (
        select(
            physical_objects_data,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            physical_object_types_dict.c.physical_object_function_id,
            physical_object_functions_dict.c.name.label("physical_object_function_name"),
            *building_columns,
            buildings_data.c.properties.label("building_properties"),
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
        )
        .select_from(
            physical_objects_data.join(
                physical_object_types_dict,
                physical_objects_data.c.physical_object_type_id == physical_object_types_dict.c.physical_object_type_id,
            )
            .join(
                physical_object_functions_dict,
                physical_object_functions_dict.c.physical_object_function_id
                == physical_object_types_dict.c.physical_object_function_id,
            )
            .outerjoin(
                buildings_data,
                buildings_data.c.physical_object_id == physical_objects_data.c.physical_object_id,
            )
            .join(
                urban_objects_data,
                urban_objects_data.c.physical_object_id == physical_objects_data.c.physical_object_id,
            )
            .join(
                object_geometries_data,
                object_geometries_data.c.object_geometry_id == urban_objects_data.c.object_geometry_id,
            )
            .join(
                territories_data,
                territories_data.c.territory_id == object_geometries_data.c.territory_id,
            )
        )
        .where(physical_objects_data.c.physical_object_id == physical_object_id)
        .distinct()
    )

    result = (await conn.execute(statement)).mappings().all()
    if not result:
        raise EntityNotFoundById(physical_object_id, "physical object")

    territories = [{"territory_id": row["territory_id"], "name": row.territory_name} for row in result]
    physical_object = {k: v for k, v in result[0].items() if k in PhysicalObjectDTO.fields()}

    return PhysicalObjectDTO(**physical_object, territories=territories)


async def add_physical_object_with_geometry_to_db(
    conn: AsyncConnection, physical_object: PhysicalObjectWithGeometryPost
) -> UrbanObjectDTO:
    """Create physical object with geometry."""

    if not await check_existence(conn, territories_data, conditions={"territory_id": physical_object.territory_id}):
        raise EntityNotFoundById(physical_object.territory_id, "territory")

    if not await check_existence(
        conn,
        physical_object_types_dict,
        conditions={"physical_object_type_id": physical_object.physical_object_type_id},
    ):
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
            geometry=ST_GeomFromWKB(physical_object.geometry.as_shapely_geometry().wkb, text(str(SRID))),
            centre_point=ST_GeomFromWKB(physical_object.centre_point.as_shapely_geometry().wkb, text(str(SRID))),
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

    return (await get_urban_objects_by_ids_from_db(conn, [urban_object_id]))[0]


async def put_physical_object_to_db(
    conn: AsyncConnection, physical_object: PhysicalObjectPut, physical_object_id: int
) -> PhysicalObjectDTO:
    """Update physical object by given all its attributes."""

    if not await check_existence(conn, physical_objects_data, conditions={"physical_object_id": physical_object_id}):
        raise EntityNotFoundById(physical_object_id, "physical object")

    if not await check_existence(
        conn,
        physical_object_types_dict,
        conditions={"physical_object_type_id": physical_object.physical_object_type_id},
    ):
        raise EntityNotFoundById(physical_object.physical_object_type_id, "physical object type")

    values = extract_values_from_model(physical_object, to_update=True)

    statement = (
        update(physical_objects_data)
        .where(physical_objects_data.c.physical_object_id == physical_object_id)
        .values(**values)
    )

    await conn.execute(statement)
    await conn.commit()

    return await get_physical_object_by_id_from_db(conn, physical_object_id)


async def patch_physical_object_to_db(
    conn: AsyncConnection, physical_object: PhysicalObjectPatch, physical_object_id: int
) -> PhysicalObjectDTO:
    """Update scenario physical object by only given attributes."""

    if not await check_existence(conn, physical_objects_data, conditions={"physical_object_id": physical_object_id}):
        raise EntityNotFoundById(physical_object_id, "physical object")

    if physical_object.physical_object_type_id is not None:
        if not await check_existence(
            conn,
            physical_object_types_dict,
            conditions={"physical_object_type_id": physical_object.physical_object_type_id},
        ):
            raise EntityNotFoundById(physical_object.physical_object_type_id, "physical object type")

    values = extract_values_from_model(physical_object, exclude_unset=True, to_update=True)

    statement = (
        update(physical_objects_data)
        .where(physical_objects_data.c.physical_object_id == physical_object_id)
        .values(**values)
    )

    await conn.execute(statement)
    await conn.commit()

    return await get_physical_object_by_id_from_db(conn, physical_object_id)


async def delete_physical_object_from_db(conn: AsyncConnection, physical_object_id: int) -> dict:
    """Delete physical object."""

    if not await check_existence(conn, physical_objects_data, conditions={"physical_object_id": physical_object_id}):
        raise EntityNotFoundById(physical_object_id, "physical object")

    statement = delete(physical_objects_data).where(physical_objects_data.c.physical_object_id == physical_object_id)
    await conn.execute(statement)
    await conn.commit()

    return {"status": "ok"}


async def add_building_to_db(
    conn: AsyncConnection,
    building: BuildingPost,
) -> PhysicalObjectDTO:
    """Create living building object."""

    if not await check_existence(
        conn, physical_objects_data, conditions={"physical_object_id": building.physical_object_id}
    ):
        raise EntityNotFoundById(building.physical_object_id, "physical object")

    if await check_existence(conn, buildings_data, conditions={"physical_object_id": building.physical_object_id}):
        raise EntityAlreadyExists("living building", building.physical_object_id)

    statement = insert(buildings_data).values(**building.model_dump())

    await conn.execute(statement)
    await conn.commit()

    return await get_physical_object_by_id_from_db(conn, building.physical_object_id)


async def put_building_to_db(conn: AsyncConnection, building: BuildingPut) -> PhysicalObjectDTO:
    """Update living building object by all its attributes."""

    if not await check_existence(
        conn, physical_objects_data, conditions={"physical_object_id": building.physical_object_id}
    ):
        raise EntityNotFoundById(building.physical_object_id, "physical object")

    if await check_existence(
        conn,
        buildings_data,
        conditions={"physical_object_id": building.physical_object_id},
    ):
        statement = (
            update(buildings_data)
            .where(buildings_data.c.physical_object_id == building.physical_object_id)
            .values(**building.model_dump())
        )
    else:
        statement = insert(buildings_data).values(**building.model_dump())

    await conn.execute(statement)
    await conn.commit()

    return await get_physical_object_by_id_from_db(conn, building.physical_object_id)


async def patch_building_to_db(conn: AsyncConnection, building: BuildingPatch, building_id: int) -> PhysicalObjectDTO:
    """Update living building object by only given attributes."""

    if not await check_existence(conn, buildings_data, conditions={"building_id": building_id}):
        raise EntityNotFoundById(building_id, "living building")

    if building.physical_object_id is not None:
        if not await check_existence(
            conn, physical_objects_data, conditions={"physical_object_id": building.physical_object_id}
        ):
            raise EntityNotFoundById(building.physical_object_id, "physical object")

        if await check_existence(
            conn,
            buildings_data,
            conditions={"physical_object_id": building.physical_object_id},
            not_conditions={"building_id": building_id},
        ):
            raise EntityAlreadyExists("living building", building.physical_object_id)

    statement = (
        update(buildings_data)
        .where(buildings_data.c.building_id == building_id)
        .values(**building.model_dump(exclude_unset=True))
        .returning(buildings_data.c.physical_object_id)
    )

    physical_object_id = (await conn.execute(statement)).scalar_one()
    await conn.commit()

    return await get_physical_object_by_id_from_db(conn, physical_object_id)


async def delete_building_from_db(conn: AsyncConnection, building_id: int) -> dict:
    """Delete living building object."""

    if not await check_existence(conn, buildings_data, conditions={"building_id": building_id}):
        raise EntityNotFoundById(building_id, "living building")

    statement = delete(buildings_data).where(buildings_data.c.building_id == building_id)
    await conn.execute(statement)
    await conn.commit()

    return {"status": "ok"}


async def get_buildings_by_physical_object_id_from_db(
    conn: AsyncConnection,
    physical_object_id: int,
) -> list[BuildingDTO]:
    """Get living building or list of living buildings by physical object id."""

    if not await check_existence(conn, physical_objects_data, conditions={"physical_object_id": physical_object_id}):
        raise EntityNotFoundById(physical_object_id, "physical object")

    statement = (
        select(
            buildings_data,
            physical_objects_data.c.name.label("physical_object_name"),
            physical_objects_data.c.properties.label("physical_object_properties"),
            physical_object_types_dict.c.physical_object_type_id,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
        )
        .select_from(
            buildings_data.join(
                physical_objects_data,
                physical_objects_data.c.physical_object_id == buildings_data.c.physical_object_id,
            ).join(
                physical_object_types_dict,
                physical_objects_data.c.physical_object_type_id == physical_object_types_dict.c.physical_object_type_id,
            )
        )
        .where(buildings_data.c.physical_object_id == physical_object_id)
        .distinct()
    )

    result = (await conn.execute(statement)).mappings().all()

    return [BuildingDTO(**building) for building in result]


async def get_services_by_physical_object_id_from_db(
    conn: AsyncConnection,
    physical_object_id: int,
    service_type_id: int | None,
    territory_type_id: int | None,
) -> list[ServiceDTO]:
    """Get service or list of services by physical object id.

    Could be specified by service type id and territory type id.
    """

    if not await check_existence(conn, physical_objects_data, conditions={"physical_object_id": physical_object_id}):
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
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
        )
        .select_from(
            services_data.join(urban_objects_data, urban_objects_data.c.service_id == services_data.c.service_id)
            .join(
                object_geometries_data,
                object_geometries_data.c.object_geometry_id == urban_objects_data.c.object_geometry_id,
            )
            .join(territories_data, territories_data.c.territory_id == object_geometries_data.c.territory_id)
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
        .distinct()
    )

    if service_type_id is not None:
        statement = statement.where(service_types_dict.c.service_type_id == service_type_id)

    if territory_type_id is not None:
        statement = statement.where(territory_types_dict.c.territory_type_id == territory_type_id)

    result = (await conn.execute(statement)).mappings().all()

    grouped_data = defaultdict(lambda: {"territories": []})
    for row in result:
        key = row.service_id
        if key not in grouped_data:
            grouped_data[key].update({k: v for k, v in row.items() if k in ServiceDTO.fields()})

        territory = {"territory_id": row["territory_id"], "name": row["territory_name"]}
        grouped_data[key]["territories"].append(territory)

    return [ServiceDTO(**service) for service in grouped_data.values()]


async def get_services_with_geometry_by_physical_object_id_from_db(
    conn: AsyncConnection,
    physical_object_id: int,
    service_type_id: int | None,
    territory_type_id: int | None,
) -> list[ServiceWithGeometryDTO]:
    """Get service or list of services with geometry by physical object id.

    Could be specified by service type id and territory type id.
    """

    if not await check_existence(conn, physical_objects_data, conditions={"physical_object_id": physical_object_id}):
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
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            object_geometries_data.c.object_geometry_id,
            object_geometries_data.c.address,
            object_geometries_data.c.osm_id,
            ST_AsEWKB(object_geometries_data.c.geometry).label("geometry"),
            ST_AsEWKB(object_geometries_data.c.centre_point).label("centre_point"),
        )
        .select_from(
            services_data.join(urban_objects_data, services_data.c.service_id == urban_objects_data.c.service_id)
            .join(
                object_geometries_data,
                object_geometries_data.c.object_geometry_id == urban_objects_data.c.object_geometry_id,
            )
            .join(territories_data, territories_data.c.territory_id == object_geometries_data.c.territory_id)
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
        .distinct()
    )

    if service_type_id is not None:
        statement = statement.where(service_types_dict.c.service_type_id == service_type_id)

    if territory_type_id is not None:
        statement = statement.where(territory_types_dict.c.territory_type_id == territory_type_id)

    result = (await conn.execute(statement)).mappings().all()

    return [ServiceWithGeometryDTO(**service) for service in result]


async def get_physical_object_geometries_from_db(
    conn: AsyncConnection,
    physical_object_id: int,
) -> list[ObjectGeometryDTO]:
    """Get geometry or list of geometries by physical object id."""

    if not await check_existence(conn, physical_objects_data, conditions={"physical_object_id": physical_object_id}):
        raise EntityNotFoundById(physical_object_id, "physical object")

    statement = (
        select(
            object_geometries_data.c.object_geometry_id,
            object_geometries_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            object_geometries_data.c.address,
            object_geometries_data.c.osm_id,
            ST_AsEWKB(object_geometries_data.c.geometry).label("geometry"),
            ST_AsEWKB(object_geometries_data.c.centre_point).label("centre_point"),
            object_geometries_data.c.created_at,
            object_geometries_data.c.updated_at,
        )
        .select_from(
            object_geometries_data.join(
                urban_objects_data,
                urban_objects_data.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
            ).join(
                territories_data,
                territories_data.c.territory_id == object_geometries_data.c.territory_id,
            )
        )
        .where(urban_objects_data.c.physical_object_id == physical_object_id)
        .distinct()
    )

    result = (await conn.execute(statement)).mappings().all()

    return [ObjectGeometryDTO(**geometry) for geometry in result]


async def add_physical_object_to_object_geometry_to_db(
    conn: AsyncConnection, object_geometry_id: int, physical_object: PhysicalObjectPost
) -> UrbanObjectDTO:
    """Create object geometry connected with physical object."""

    if not await check_existence(conn, object_geometries_data, conditions={"object_geometry_id": object_geometry_id}):
        raise EntityNotFoundById(object_geometry_id, "object geometry")

    if not await check_existence(
        conn,
        physical_object_types_dict,
        conditions={"physical_object_type_id": physical_object.physical_object_type_id},
    ):
        raise EntityNotFoundById(physical_object.physical_object_type_id, "physical object type")

    statement = (
        insert(physical_objects_data)
        .values(**physical_object.model_dump())
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

    return (await get_urban_objects_by_ids_from_db(conn, [urban_object_id]))[0]
