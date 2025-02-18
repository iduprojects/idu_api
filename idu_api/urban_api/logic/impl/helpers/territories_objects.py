"""Territories objects internal logic is defined here."""

from datetime import date
from typing import Callable, Literal

import shapely.geometry as geom
from geoalchemy2.functions import ST_AsEWKB, ST_GeomFromWKB
from sqlalchemy import func, insert, select, text, update
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import target_city_types_dict, territories_data, territory_types_dict
from idu_api.urban_api.dto import PageDTO, TerritoryDTO, TerritoryWithoutGeometryDTO
from idu_api.urban_api.exceptions.logic.common import EntitiesNotFoundByIds, EntityNotFoundById, TooManyObjectsError
from idu_api.urban_api.logic.impl.helpers.utils import (
    OBJECTS_NUMBER_LIMIT,
    SRID,
    build_recursive_query,
    check_existence,
    extract_values_from_model,
)
from idu_api.urban_api.schemas import TerritoryPatch, TerritoryPost, TerritoryPut
from idu_api.urban_api.utils.pagination import paginate_dto

func: Callable
Geom = geom.Polygon | geom.MultiPolygon | geom.Point | geom.LineString | geom.MultiLineString


async def get_territories_by_ids(conn: AsyncConnection, ids: list[int]) -> list[TerritoryDTO]:
    """Get territory objects by ids list."""

    if len(ids) > OBJECTS_NUMBER_LIMIT:
        raise TooManyObjectsError(len(ids), OBJECTS_NUMBER_LIMIT)

    territories_data_parents = territories_data.alias("territories_data_parents")
    admin_centers = territories_data.alias("admin_centers")
    statement = (
        select(
            territories_data.c.territory_id,
            territories_data.c.territory_type_id,
            territory_types_dict.c.name.label("territory_type_name"),
            territories_data.c.parent_id,
            territories_data_parents.c.name.label("parent_name"),
            territories_data.c.name,
            ST_AsEWKB(territories_data.c.geometry).label("geometry"),
            territories_data.c.level,
            territories_data.c.properties,
            ST_AsEWKB(territories_data.c.centre_point).label("centre_point"),
            territories_data.c.admin_center_id,
            admin_centers.c.name.label("admin_center_name"),
            territories_data.c.target_city_type_id,
            target_city_types_dict.c.name.label("target_city_type_name"),
            target_city_types_dict.c.description.label("target_city_type_description"),
            territories_data.c.okato_code,
            territories_data.c.oktmo_code,
            territories_data.c.is_city,
            territories_data.c.created_at,
            territories_data.c.updated_at,
        )
        .select_from(
            territories_data.join(
                territory_types_dict, territory_types_dict.c.territory_type_id == territories_data.c.territory_type_id
            )
            .outerjoin(
                target_city_types_dict,
                target_city_types_dict.c.target_city_type_id == territories_data.c.target_city_type_id,
            )
            .outerjoin(
                territories_data_parents,
                territories_data_parents.c.territory_id == territories_data.c.parent_id,
            )
            .outerjoin(admin_centers, admin_centers.c.territory_id == territories_data.c.admin_center_id)
        )
        .where(territories_data.c.territory_id.in_(ids))
    )

    results = (await conn.execute(statement)).mappings().all()
    if len(results) == 0 and len(ids) == 1:
        raise EntityNotFoundById(ids[0], "territory")
    if len(ids) > len(results):
        raise EntitiesNotFoundByIds("territory")

    return [TerritoryDTO(**territory) for territory in results]


async def get_territory_by_id(conn: AsyncConnection, territory_id: int) -> TerritoryDTO:
    """Get territory object by id."""

    results = await get_territories_by_ids(conn, [territory_id])

    return results[0]


async def add_territory_to_db(conn: AsyncConnection, territory: TerritoryPost) -> TerritoryDTO:
    """Create territory object."""

    if territory.parent_id is not None:
        if not await check_existence(conn, territories_data, conditions={"territory_id": territory.parent_id}):
            raise EntityNotFoundById(territory.parent_id, "parent territory")

    if territory.admin_center_id is not None:
        if not await check_existence(conn, territories_data, conditions={"territory_id": territory.admin_center_id}):
            raise EntityNotFoundById(territory.admin_center_id, "admin center")

    if territory.target_city_type_id is not None:
        if not await check_existence(
            conn, target_city_types_dict, conditions={"target_city_type_id": territory.target_city_type_id}
        ):
            raise EntityNotFoundById(territory.target_city_type_id, "target city type")

    if not await check_existence(
        conn, territory_types_dict, conditions={"territory_type_id": territory.territory_type_id}
    ):
        raise EntityNotFoundById(territory.territory_type_id, "territory type")

    values = extract_values_from_model(territory)
    statement = insert(territories_data).values(**values).returning(territories_data.c.territory_id)
    territory_id = (await conn.execute(statement)).scalar_one()
    await conn.commit()

    return await get_territory_by_id(conn, territory_id)


async def put_territory_to_db(
    conn: AsyncConnection,
    territory_id: int,
    territory: TerritoryPut,
) -> TerritoryDTO:
    """Update territory object (put, update all the fields)."""

    if not await check_existence(conn, territories_data, conditions={"territory_id": territory_id}):
        raise EntityNotFoundById(territory_id, "territory")

    if territory.parent_id is not None:
        if not await check_existence(conn, territories_data, conditions={"territory_id": territory.parent_id}):
            raise EntityNotFoundById(territory.parent_id, "parent territory")

    if territory.admin_center_id is not None:
        if not await check_existence(conn, territories_data, conditions={"territory_id": territory.admin_center_id}):
            raise EntityNotFoundById(territory.admin_center_id, "admin center")

    if territory.target_city_type_id is not None:
        if not await check_existence(
            conn, target_city_types_dict, conditions={"target_city_type_id": territory.target_city_type_id}
        ):
            raise EntityNotFoundById(territory.target_city_type_id, "target city type")

    if not await check_existence(
        conn, territory_types_dict, conditions={"territory_type_id": territory.territory_type_id}
    ):
        raise EntityNotFoundById(territory.territory_type_id, "territory type")

    values = extract_values_from_model(territory, to_update=True)
    statement = update(territories_data).where(territories_data.c.territory_id == territory_id).values(**values)
    await conn.execute(statement)
    await conn.commit()

    return await get_territory_by_id(conn, territory_id)


async def patch_territory_to_db(
    conn: AsyncConnection,
    territory_id: int,
    territory: TerritoryPatch,
) -> TerritoryDTO:
    """Patch territory object (patch, update only set fields)."""

    if not await check_existence(conn, territories_data, conditions={"territory_id": territory_id}):
        raise EntityNotFoundById(territory_id, "territory")

    if territory.parent_id is not None:
        if not await check_existence(conn, territories_data, conditions={"territory_id": territory.parent_id}):
            raise EntityNotFoundById(territory.parent_id, "parent territory")

    if territory.admin_center_id is not None:
        if not await check_existence(conn, territories_data, conditions={"territory_id": territory.admin_center_id}):
            raise EntityNotFoundById(territory.admin_center_id, "admin center")

    if territory.target_city_type_id is not None:
        if not await check_existence(
            conn, target_city_types_dict, conditions={"target_city_type_id": territory.target_city_type_id}
        ):
            raise EntityNotFoundById(territory.target_city_type_id, "target city type")

    if not await check_existence(
        conn, territory_types_dict, conditions={"territory_type_id": territory.territory_type_id}
    ):
        raise EntityNotFoundById(territory.territory_type_id, "territory type")

    values = extract_values_from_model(territory, exclude_unset=True, to_update=True)
    statement = update(territories_data).where(territories_data.c.territory_id == territory_id).values(**values)
    await conn.execute(statement)
    await conn.commit()

    return await get_territory_by_id(conn, territory_id)


async def get_territories_by_parent_id_from_db(
    conn: AsyncConnection,
    parent_id: int | None,
    get_all_levels: bool,
    territory_type_id: int | None,
    name: str | None,
    cities_only: bool,
    created_at: date | None,
    order_by: Literal["created_at", "updated_at"] | None,
    ordering: Literal["asc", "desc"] | None,
    paginate: bool,
) -> list[TerritoryDTO] | PageDTO[TerritoryDTO]:
    """Get a territory or list of territories by parent,
    ordering and filters can be specified in parameters."""

    if parent_id is not None:
        if not await check_existence(conn, territories_data, conditions={"territory_id": parent_id}):
            raise EntityNotFoundById(parent_id, "territory")

    statement = select(territories_data)
    if get_all_levels:
        statement = build_recursive_query(
            statement, territories_data, parent_id, "territories_recursive", "territory_id"
        )
    else:
        statement = statement.where(
            territories_data.c.parent_id == parent_id
            if parent_id is not None
            else territories_data.c.parent_id.is_(None)
        )

    requested_territories = statement.cte("requested_territories")
    territories_data_parents = territories_data.alias("territories_data_parents")
    admin_centers = territories_data.alias("admin_centers")
    statement = select(
        requested_territories.c.territory_id,
        requested_territories.c.territory_type_id,
        requested_territories.c.name.label("territory_type_name"),
        requested_territories.c.parent_id,
        requested_territories.c.name.label("parent_name"),
        requested_territories.c.name,
        ST_AsEWKB(requested_territories.c.geometry).label("geometry"),
        requested_territories.c.level,
        requested_territories.c.properties,
        ST_AsEWKB(requested_territories.c.centre_point).label("centre_point"),
        requested_territories.c.admin_center_id,
        admin_centers.c.name.label("admin_center_name"),
        requested_territories.c.target_city_type_id,
        target_city_types_dict.c.name.label("target_city_type_name"),
        target_city_types_dict.c.description.label("target_city_type_description"),
        requested_territories.c.okato_code,
        requested_territories.c.oktmo_code,
        requested_territories.c.is_city,
        requested_territories.c.created_at,
        requested_territories.c.updated_at,
    ).select_from(
        requested_territories.join(
            territory_types_dict, territory_types_dict.c.territory_type_id == requested_territories.c.territory_type_id
        )
        .outerjoin(
            target_city_types_dict,
            target_city_types_dict.c.target_city_type_id == requested_territories.c.target_city_type_id,
        )
        .outerjoin(
            territories_data_parents,
            territories_data_parents.c.territory_id == requested_territories.c.parent_id,
        )
        .outerjoin(admin_centers, admin_centers.c.territory_id == requested_territories.c.admin_center_id)
    )

    if cities_only:
        statement = statement.where(requested_territories.c.is_city.is_(cities_only))
    if name is not None:
        statement = statement.where(requested_territories.c.name.ilike(f"%{name}%"))
    if created_at is not None:
        statement = statement.where(func.date(requested_territories.c.created_at) == created_at)
    if territory_type_id is not None:
        statement = statement.where(requested_territories.c.territory_type_id == territory_type_id)

    if order_by is not None:
        order = requested_territories.c.created_at if order_by == "created_at" else requested_territories.c.updated_at
        if ordering == "desc":
            order = order.desc()
        statement = statement.order_by(order)
    else:
        if ordering == "desc":
            statement = statement.order_by(requested_territories.c.territory_id.desc())
        else:
            statement = statement.order_by(requested_territories.c.territory_id)

    if paginate:
        return await paginate_dto(conn, statement, transformer=lambda x: [TerritoryDTO(**item) for item in x])

    result = (await conn.execute(statement)).mappings().all()

    return [TerritoryDTO(**territory) for territory in result]


async def get_territories_without_geometry_by_parent_id_from_db(
    conn: AsyncConnection,
    parent_id: int | None,
    get_all_levels: bool,
    territory_type_id: int | None,
    name: str | None,
    cities_only: bool,
    created_at: date | None,
    order_by: Literal["created_at", "updated_at"] | None,
    ordering: Literal["asc", "desc"] | None,
    paginate: bool,
) -> list[TerritoryWithoutGeometryDTO] | PageDTO[TerritoryWithoutGeometryDTO]:
    """Get a territory or list of territories without geometry by parent,
    ordering and filters can be specified in parameters.
    """

    if parent_id is not None:
        if not await check_existence(conn, territories_data, conditions={"territory_id": parent_id}):
            raise EntityNotFoundById(parent_id, "territory")

    statement = select(territories_data)
    if get_all_levels:
        statement = build_recursive_query(
            statement, territories_data, parent_id, "territories_recursive", "territory_id"
        )
    else:
        statement = statement.where(
            territories_data.c.parent_id == parent_id
            if parent_id is not None
            else territories_data.c.parent_id.is_(None)
        )

    requested_territories = statement.cte("requested_territories")
    territories_data_parents = territories_data.alias("territories_data_parents")
    admin_centers = territories_data.alias("admin_centers")
    statement = select(
        requested_territories.c.territory_id,
        requested_territories.c.territory_type_id,
        requested_territories.c.name.label("territory_type_name"),
        requested_territories.c.parent_id,
        requested_territories.c.name.label("parent_name"),
        requested_territories.c.name,
        requested_territories.c.level,
        requested_territories.c.properties,
        requested_territories.c.admin_center_id,
        admin_centers.c.name.label("admin_center_name"),
        requested_territories.c.target_city_type_id,
        target_city_types_dict.c.name.label("target_city_type_name"),
        target_city_types_dict.c.description.label("target_city_type_description"),
        requested_territories.c.okato_code,
        requested_territories.c.oktmo_code,
        requested_territories.c.is_city,
        requested_territories.c.created_at,
        requested_territories.c.updated_at,
    ).select_from(
        requested_territories.join(
            territory_types_dict, territory_types_dict.c.territory_type_id == requested_territories.c.territory_type_id
        )
        .outerjoin(
            target_city_types_dict,
            target_city_types_dict.c.target_city_type_id == requested_territories.c.target_city_type_id,
        )
        .outerjoin(
            territories_data_parents,
            territories_data_parents.c.territory_id == requested_territories.c.parent_id,
        )
        .outerjoin(admin_centers, admin_centers.c.territory_id == requested_territories.c.admin_center_id)
    )

    if cities_only:
        statement = statement.where(requested_territories.c.is_city.is_(cities_only))
    if name is not None:
        statement = statement.where(requested_territories.c.name.ilike(f"%{name}%"))
    if created_at is not None:
        statement = statement.where(func.date(requested_territories.c.created_at) == created_at)
    if territory_type_id is not None:
        statement = statement.where(requested_territories.c.territory_type_id == territory_type_id)

    if order_by is not None:
        order = requested_territories.c.created_at if order_by == "created_at" else requested_territories.c.updated_at
        if ordering == "desc":
            order = order.desc()
        statement = statement.order_by(order)
    else:
        if ordering == "desc":
            statement = statement.order_by(requested_territories.c.territory_id.desc())
        else:
            statement = statement.order_by(requested_territories.c.territory_id)

    if paginate:
        return await paginate_dto(
            conn, statement, transformer=lambda x: [TerritoryWithoutGeometryDTO(**item) for item in x]
        )

    result = (await conn.execute(statement)).mappings().all()

    return [TerritoryWithoutGeometryDTO(**territory) for territory in result]


async def get_common_territory_for_geometry(conn: AsyncConnection, geometry: Geom) -> TerritoryDTO | None:
    """Get the deepest territory which covers given geometry. None if there is no such territory."""

    statement = (
        select(territories_data.c.territory_id)
        .where(func.ST_Covers(territories_data.c.geometry, ST_GeomFromWKB(geometry.wkb, text(str(SRID)))))
        .order_by(territories_data.c.level.desc())
        .limit(1)
    )

    territory_id = (await conn.execute(statement)).scalar_one_or_none()

    if territory_id is None:
        return None

    return await get_territory_by_id(conn, territory_id)


async def get_intersecting_territories_for_geometry(
    conn: AsyncConnection,
    parent_territory: int,
    geometry: Geom,
) -> list[TerritoryDTO]:
    """Get all territories of the (level of given parent + 1) which intersect with given geometry."""

    if not await check_existence(conn, territories_data, conditions={"territory_id": parent_territory}):
        raise EntityNotFoundById(parent_territory, "territory")

    level_subquery = (
        select(territories_data.c.level + 1)
        .where(territories_data.c.territory_id == parent_territory)
        .scalar_subquery()
    )

    given_geometry = select(ST_GeomFromWKB(geometry.wkb, text(str(SRID)))).cte("given_geometry")

    statement = select(territories_data.c.territory_id).where(
        territories_data.c.level == level_subquery,
        (
            func.ST_Intersects(territories_data.c.geometry, select(given_geometry).scalar_subquery())
            | func.ST_Covers(select(given_geometry).scalar_subquery(), territories_data.c.geometry)
            | func.ST_Covers(territories_data.c.geometry, select(given_geometry).scalar_subquery())
        ),
    )

    territory_ids = (await conn.execute(statement)).scalars().all()

    return await get_territories_by_ids(conn, territory_ids)
