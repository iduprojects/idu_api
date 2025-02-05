"""Territories objects internal logic is defined here."""

from datetime import date
from typing import Callable, Literal

import shapely.geometry as geom
from geoalchemy2.functions import ST_AsGeoJSON, ST_GeomFromText
from sqlalchemy import cast, func, insert, select, text, update
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import territories_data, territory_types_dict
from idu_api.urban_api.dto import PageDTO, TerritoryDTO, TerritoryWithoutGeometryDTO
from idu_api.urban_api.exceptions.logic.common import EntitiesNotFoundByIds, EntityNotFoundById, TooManyObjectsError
from idu_api.urban_api.logic.impl.helpers.utils import (
    DECIMAL_PLACES,
    OBJECTS_NUMBER_LIMIT,
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
    statement = (
        select(
            territories_data.c.territory_id,
            territories_data.c.territory_type_id,
            territory_types_dict.c.name.label("territory_type_name"),
            territories_data.c.parent_id,
            territories_data_parents.c.name.label("parent_name"),
            territories_data.c.name,
            cast(ST_AsGeoJSON(territories_data.c.geometry, DECIMAL_PLACES), JSONB).label("geometry"),
            territories_data.c.level,
            territories_data.c.properties,
            cast(ST_AsGeoJSON(territories_data.c.centre_point, DECIMAL_PLACES), JSONB).label("centre_point"),
            territories_data.c.admin_center,
            territories_data.c.okato_code,
            territories_data.c.oktmo_code,
            territories_data.c.is_city,
            territories_data.c.created_at,
            territories_data.c.updated_at,
        )
        .select_from(
            territories_data.join(
                territory_types_dict, territory_types_dict.c.territory_type_id == territories_data.c.territory_type_id
            ).outerjoin(
                territories_data_parents,
                territories_data.c.parent_id == territories_data_parents.c.territory_id,
            )
        )
        .where(territories_data.c.territory_id.in_(ids))
    )

    results = (await conn.execute(statement)).mappings().all()
    if len(ids) > len(results):
        raise EntitiesNotFoundByIds("territory")

    return [TerritoryDTO(**territory) for territory in results]


async def get_territory_by_id(conn: AsyncConnection, territory_id: int) -> TerritoryDTO:
    """Get territory object by id."""

    results = await get_territories_by_ids(conn, [territory_id])
    if len(results) == 0:
        raise EntityNotFoundById(territory_id, "territory")

    return results[0]


async def add_territory_to_db(conn: AsyncConnection, territory: TerritoryPost) -> TerritoryDTO:
    """Create territory object."""

    if territory.parent_id is not None:
        if not await check_existence(conn, territories_data, conditions={"territory_id": territory.parent_id}):
            raise EntityNotFoundById(territory.parent_id, "territory")

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
            raise EntityNotFoundById(territory.parent_id, "territory")

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
            raise EntityNotFoundById(territory.parent_id, "territory")

    if territory.territory_type_id is not None:
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

    territories_data_parents = territories_data.alias("territories_data_parents")
    statement = select(
        territories_data.c.territory_id,
        territories_data.c.territory_type_id,
        territory_types_dict.c.name.label("territory_type_name"),
        territories_data.c.parent_id,
        territories_data_parents.c.name.label("parent_name"),
        territories_data.c.name,
        cast(ST_AsGeoJSON(territories_data.c.geometry, DECIMAL_PLACES), JSONB).label("geometry"),
        territories_data.c.level,
        territories_data.c.properties,
        cast(ST_AsGeoJSON(territories_data.c.centre_point, DECIMAL_PLACES), JSONB).label("centre_point"),
        territories_data.c.admin_center,
        territories_data.c.okato_code,
        territories_data.c.oktmo_code,
        territories_data.c.is_city,
        territories_data.c.created_at,
        territories_data.c.updated_at,
    ).select_from(
        territories_data.join(
            territory_types_dict, territory_types_dict.c.territory_type_id == territories_data.c.territory_type_id
        ).outerjoin(
            territories_data_parents,
            territories_data.c.parent_id == territories_data_parents.c.territory_id,
        )
    )

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
    statement = select(requested_territories)

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

    territories_data_parents = territories_data.alias("territories_data_parents")
    statement = select(
        territories_data.c.territory_id,
        territories_data.c.territory_type_id,
        territory_types_dict.c.name.label("territory_type_name"),
        territories_data.c.parent_id,
        territories_data_parents.c.name.label("parent_name"),
        territories_data.c.name,
        territories_data.c.level,
        territories_data.c.properties,
        territories_data.c.admin_center,
        territories_data.c.okato_code,
        territories_data.c.oktmo_code,
        territories_data.c.is_city,
        territories_data.c.created_at,
        territories_data.c.updated_at,
    ).select_from(
        territories_data.join(
            territory_types_dict, territory_types_dict.c.territory_type_id == territories_data.c.territory_type_id
        ).outerjoin(
            territories_data_parents,
            territories_data.c.parent_id == territories_data_parents.c.territory_id,
        )
    )

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
    statement = select(requested_territories)

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
        .where(func.ST_Covers(territories_data.c.geometry, ST_GeomFromText(geometry.wkt, text("4326"))))
        .order_by(territories_data.c.level.desc())
        .limit(1)
    )

    territory_id = (await conn.execute(statement)).scalar_one_or_none()

    if territory_id is None:
        return None

    return (await get_territories_by_ids(conn, [territory_id]))[0]


async def get_intersecting_territories_for_geometry(
    conn: AsyncConnection,
    parent_territory: int,
    geometry: Geom,
) -> list[TerritoryDTO]:
    """Get all territories of the (level of given parent + 1) which intersect with given geometry."""

    if not await check_existence(conn, territories_data, conditions={"territory_id": parent_territory}):
        raise EntityNotFoundById(parent_territory, "territory")

    level_subqery = (
        select(territories_data.c.level + 1)
        .where(territories_data.c.territory_id == parent_territory)
        .scalar_subquery()
    )

    given_geometry = select(ST_GeomFromText(geometry.wkt, text("4326"))).cte("given_geometry")

    statement = select(territories_data.c.territory_id).where(
        territories_data.c.level == level_subqery,
        (
            func.ST_Intersects(territories_data.c.geometry, select(given_geometry).scalar_subquery())
            | func.ST_Covers(select(given_geometry).scalar_subquery(), territories_data.c.geometry)
            | func.ST_Covers(territories_data.c.geometry, select(given_geometry).scalar_subquery())
        ),
    )

    territory_ids = (await conn.execute(statement)).scalars().all()

    return await get_territories_by_ids(conn, territory_ids)
