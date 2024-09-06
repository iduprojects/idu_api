"""Territories objects internal logic is defined here."""

from datetime import date, datetime, timezone
from typing import Callable, Literal, Optional

import shapely.geometry as geom
from geoalchemy2.functions import ST_AsGeoJSON, ST_GeomFromText
from sqlalchemy import cast, func, insert, select, text, update
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import territories_data, territory_types_dict
from idu_api.urban_api.dto import PageDTO, TerritoryDTO, TerritoryWithoutGeometryDTO
from idu_api.urban_api.exceptions.logic.common import EntityNotFoundById
from idu_api.urban_api.schemas import TerritoryDataPatch, TerritoryDataPost, TerritoryDataPut
from idu_api.urban_api.utils.pagination import paginate_dto

func: Callable


async def get_territories_by_ids(conn: AsyncConnection, territory_ids: list[int]) -> list[TerritoryDTO]:
    """Get territory objects by ids list."""

    territories_data_parents = territories_data.alias("territories_data_parents")
    statement = (
        select(
            territories_data.c.territory_id,
            territories_data.c.territory_type_id,
            territory_types_dict.c.name.label("territory_type_name"),
            territories_data.c.parent_id,
            territories_data_parents.c.name.label("parent_name"),
            territories_data.c.name,
            cast(ST_AsGeoJSON(territories_data.c.geometry), JSONB).label("geometry"),
            territories_data.c.level,
            territories_data.c.properties,
            cast(ST_AsGeoJSON(territories_data.c.centre_point), JSONB).label("centre_point"),
            territories_data.c.admin_center,
            territories_data.c.okato_code,
            territories_data.c.created_at,
            territories_data.c.updated_at,
        )
        .select_from(
            territories_data.join(
                territory_types_dict, territory_types_dict.c.territory_type_id == territories_data.c.territory_type_id
            ).join(
                territories_data_parents,
                territories_data.c.parent_id == territories_data_parents.c.territory_id,
                isouter=True,
            )
        )
        .where(territories_data.c.territory_id.in_(territory_ids))
    )

    results = (await conn.execute(statement)).mappings().all()

    return [TerritoryDTO(**territory) for territory in results]


async def get_territory_by_id(conn: AsyncConnection, territory_id: int) -> TerritoryDTO:
    """Get territory object by id."""

    results = await get_territories_by_ids(conn, [territory_id])
    if len(results) == 0:
        raise EntityNotFoundById(territory_id, "territory")

    return results[0]


async def add_territory_to_db(
    conn: AsyncConnection,
    territory: TerritoryDataPost,
) -> TerritoryDTO:
    """Create territory object."""

    if territory.parent_id is not None:
        statement = select(territories_data).where(territories_data.c.territory_id == territory.parent_id)
        parent_territory = (await conn.execute(statement)).one_or_none()
        if parent_territory is None:
            raise EntityNotFoundById(territory.parent_id, "territory")

    statement = select(territory_types_dict).where(
        territory_types_dict.c.territory_type_id == territory.territory_type_id
    )
    territory_type = (await conn.execute(statement)).one_or_none()
    if territory_type is None:
        raise EntityNotFoundById(territory.territory_type_id, "territory type")

    statement = (
        insert(territories_data)
        .values(
            territory_type_id=territory.territory_type_id,
            parent_id=territory.parent_id,
            name=territory.name,
            geometry=ST_GeomFromText(str(territory.geometry.as_shapely_geometry()), text("4326")),
            properties=territory.properties,
            centre_point=ST_GeomFromText(str(territory.centre_point.as_shapely_geometry()), text("4326")),
            admin_center=territory.admin_center,
            okato_code=territory.okato_code,
        )
        .returning(territories_data.c.territory_id)
    )
    result_id = (await conn.execute(statement)).scalar_one()
    result = await get_territory_by_id(conn, result_id)

    await conn.commit()

    return result


async def put_territory_to_db(
    conn: AsyncConnection,
    territory_id: int,
    territory: TerritoryDataPut,
) -> TerritoryDTO:
    """Update territory object (put, update all the fields)."""

    if territory.parent_id is not None:
        statement = select(territories_data).filter(territories_data.c.territory_id == territory.parent_id)
        check_parent_id = (await conn.execute(statement)).one_or_none()
        if check_parent_id is None:
            raise EntityNotFoundById(territory.parent_id, "territory")

    statement = select(territories_data).where(territories_data.c.territory_id == territory_id)
    requested_territory = (await conn.execute(statement)).one_or_none()
    if requested_territory is None:
        raise EntityNotFoundById(territory_id, "territory")

    statement = select(territory_types_dict).where(
        territory_types_dict.c.territory_type_id == territory.territory_type_id
    )
    territory_type = (await conn.execute(statement)).one_or_none()
    if territory_type is None:
        raise EntityNotFoundById(territory.territory_type_id, "territory type")

    statement = (
        update(territories_data)
        .where(territories_data.c.territory_id == territory_id)
        .values(
            territory_type_id=territory.territory_type_id,
            parent_id=territory.parent_id,
            name=territory.name,
            geometry=ST_GeomFromText(str(territory.geometry.as_shapely_geometry()), text("4326")),
            level=territory.level,
            properties=territory.properties,
            centre_point=ST_GeomFromText(str(territory.centre_point.as_shapely_geometry()), text("4326")),
            admin_center=territory.admin_center,
            okato_code=territory.okato_code,
            updated_at=datetime.utcnow(),
        )
        .returning(territories_data.c.territory_id)
    )
    result_id = (await conn.execute(statement)).scalar_one()

    await conn.commit()

    return await get_territory_by_id(conn, result_id)


async def patch_territory_to_db(
    conn: AsyncConnection,
    territory_id: int,
    territory: TerritoryDataPatch,
) -> TerritoryDTO:
    """Patch territory object (patch, update only set fields)."""

    if territory.parent_id is not None:
        statement = select(territories_data).filter(territories_data.c.territory_id == territory.parent_id)
        check_parent_id = (await conn.execute(statement)).one_or_none()
        if check_parent_id is None:
            raise EntityNotFoundById(territory.parent_id, "territory")

    statement = select(territories_data).where(territories_data.c.territory_id == territory_id)
    requested_territory = (await conn.execute(statement)).one_or_none()
    if requested_territory is None:
        raise EntityNotFoundById(territory_id, "territory")

    statement = (
        update(territories_data)
        .where(territories_data.c.territory_id == territory_id)
        .returning(territories_data)
        .values(updated_at=datetime.now(timezone.utc))
    )

    values_to_update = {}
    for k, v in territory.model_dump(exclude={"geometry", "centre_point"}, exclude_unset=True).items():
        if k == "territory_type_id":
            new_statement = select(territory_types_dict).where(
                territory_types_dict.c.territory_type_id == territory.territory_type_id
            )
            territory_type = (await conn.execute(new_statement)).one_or_none()
            if territory_type is None:
                raise EntityNotFoundById(territory.territory_type_id, "territory type")
        values_to_update.update({k: v})

    if territory.geometry is not None:
        values_to_update.update(
            {"geometry": ST_GeomFromText(str(territory.geometry.as_shapely_geometry()), text("4326"))}
        )
        values_to_update.update(
            {"centre_point": ST_GeomFromText(str(territory.centre_point.as_shapely_geometry()), text("4326"))}
        )

    statement = statement.values(**values_to_update)
    result = (await conn.execute(statement)).mappings().one()
    await conn.commit()

    return await get_territory_by_id(conn, result.territory_id)


async def get_territories_by_parent_id_from_db(
    conn: AsyncConnection,
    parent_id: int | None,
    get_all_levels: bool | None,
    territory_type_id: int | None,
    paginate: bool = False,
) -> list[TerritoryDTO] | PageDTO[TerritoryDTO]:
    """Get a territory or list of territories by parent, territory type could be specified in parameters."""

    if parent_id is not None:
        statement = select(territories_data.c.territory_id).where(territories_data.c.territory_id == parent_id)
        parent_territory = (await conn.execute(statement)).one_or_none()
        if parent_territory is None:
            raise EntityNotFoundById(parent_id, "territory")

    territories_data_parents = territories_data.alias("territories_data_parents")
    statement = select(
        territories_data.c.territory_id,
        territories_data.c.territory_type_id,
        territory_types_dict.c.name.label("territory_type_name"),
        territories_data.c.parent_id,
        territories_data_parents.c.name.label("parent_name"),
        territories_data.c.name,
        cast(ST_AsGeoJSON(territories_data.c.geometry), JSONB).label("geometry"),
        territories_data.c.level,
        territories_data.c.properties,
        cast(ST_AsGeoJSON(territories_data.c.centre_point), JSONB).label("centre_point"),
        territories_data.c.admin_center,
        territories_data.c.okato_code,
        territories_data.c.created_at,
        territories_data.c.updated_at,
    ).select_from(
        territories_data.join(
            territory_types_dict, territory_types_dict.c.territory_type_id == territories_data.c.territory_type_id
        ).join(
            territories_data_parents,
            territories_data.c.parent_id == territories_data_parents.c.territory_id,
            isouter=True,
        )
    )

    if get_all_levels:
        cte_statement = statement.where(
            territories_data.c.parent_id == parent_id
            if parent_id is not None
            else territories_data.c.parent_id.is_(None)
        )
        if territory_type_id is not None:
            filter_statement = select(territory_types_dict).where(
                territory_types_dict.c.territory_type_id == territory_type_id
            )
            territory_type = (await conn.execute(filter_statement)).one_or_none()
            if territory_type is None:
                raise EntityNotFoundById(territory_type_id, "territory type")
            cte_statement = cte_statement.where(territories_data.c.territory_type_id == territory_type_id)
        cte_statement = cte_statement.cte(name="territories_recursive", recursive=True)

        recursive_part = statement.join(cte_statement, territories_data.c.parent_id == cte_statement.c.territory_id)

        statement = select(cte_statement.union_all(recursive_part))

    else:
        statement = statement.where(
            territories_data.c.parent_id == parent_id
            if parent_id is not None
            else territories_data.c.parent_id.is_(None)
        )

        if territory_type_id is not None:
            filter_statement = select(territory_types_dict).where(
                territory_types_dict.c.territory_type_id == territory_type_id
            )
            territory_type = (await conn.execute(filter_statement)).one_or_none()
            if territory_type is None:
                raise EntityNotFoundById(territory_type_id, "territory type")
            statement = statement.where(territories_data.c.territory_type_id == territory_type_id)

    requested_territories = statement.cte("requested_territories")
    statement = select(requested_territories).order_by(requested_territories.c.territory_id)

    if paginate:
        return await paginate_dto(conn, statement, transformer=lambda x: [TerritoryDTO(**item) for item in x])

    result = (await conn.execute(statement)).mappings().all()
    return [TerritoryDTO(**territory) for territory in result]


async def get_territories_without_geometry_by_parent_id_from_db(
    conn: AsyncConnection,
    parent_id: int | None,
    get_all_levels: bool,
    order_by: Optional[Literal["created_at", "updated_at"]],
    created_at: date | None,
    name: str | None,
    ordering: Optional[Literal["asc", "desc"]] = "asc",
    paginate: bool = False,
) -> list[TerritoryWithoutGeometryDTO] | PageDTO[TerritoryWithoutGeometryDTO]:
    """Get a territory or list of territories without geometry by parent,
    ordering and filters can be specified in parameters.
    """

    if parent_id is not None:
        statement = select(territories_data).where(territories_data.c.territory_id == parent_id)
        parent_territory = (await conn.execute(statement)).one_or_none()
        if parent_territory is None:
            raise EntityNotFoundById(parent_id, "territory")

    statement = select(
        territories_data.c.territory_id,
        territories_data.c.territory_type_id,
        territory_types_dict.c.name.label("territory_type_name"),
        territories_data.c.parent_id,
        territories_data.c.name,
        territories_data.c.level,
        territories_data.c.properties,
        territories_data.c.admin_center,
        territories_data.c.okato_code,
        territories_data.c.created_at,
        territories_data.c.updated_at,
    ).select_from(
        territories_data.join(
            territory_types_dict, territory_types_dict.c.territory_type_id == territories_data.c.territory_type_id
        )
    )

    if get_all_levels:
        cte_statement = statement.where(
            (
                territories_data.c.parent_id == parent_id
                if parent_id is not None
                else territories_data.c.parent_id.is_(None)
            )
        )
        cte_statement = cte_statement.cte(name="territories_recursive", recursive=True)

        recursive_part = statement.join(cte_statement, territories_data.c.parent_id == cte_statement.c.territory_id)

        statement = select(cte_statement.union_all(recursive_part))
    else:
        statement = statement.where(
            territories_data.c.parent_id == parent_id
            if parent_id is not None
            else territories_data.c.parent_id.is_(None)
        )

    requested_territories = statement.cte("requested_territories")
    statement = select(requested_territories)

    if name is not None:
        statement = statement.where(requested_territories.c.name.ilike(f"%{name}%"))
    if created_at is not None:
        statement = statement.where(func.date(requested_territories.c.created_at) == created_at)
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


async def get_common_territory_for_geometry(
    conn: AsyncConnection, geometry: geom.Polygon | geom.MultiPolygon | geom.Point
) -> TerritoryDTO | None:
    """Get the deepest territory which covers given geometry. None if there is no such territory."""

    statement = (
        select(territories_data.c.territory_id)
        .where(func.ST_Covers(territories_data.c.geometry, ST_GeomFromText(str(geometry), text("4326"))))
        .order_by(territories_data.c.level.desc())
        .limit(1)
    )

    territory_id = (await conn.execute(statement)).scalar_one_or_none()

    if territory_id is None:
        return None

    return (await get_territories_by_ids(conn, [territory_id]))[0]


async def get_intersecting_territories_for_geometry(
    conn: AsyncConnection, parent_territory: int, geometry: geom.Polygon | geom.MultiPolygon | geom.Point
) -> list[TerritoryDTO]:
    """Get all territories of the (level of given parent + 1) which intersect with given geometry."""

    level_subqery = (
        select(territories_data.c.level + 1)
        .where(territories_data.c.territory_id == parent_territory)
        .scalar_subquery()
    )

    given_geometry = select(ST_GeomFromText(str(geometry), text("4326"))).cte("given_geometry")

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
