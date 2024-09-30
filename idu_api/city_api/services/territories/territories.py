from datetime import datetime, date

from fastapi import HTTPException
from geoalchemy2.functions import ST_AsGeoJSON, ST_Covers, ST_GeomFromText
from sqlalchemy import select, cast, and_, text, or_
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy.sql.selectable import NamedFromClause, Select

from idu_api.city_api.dto.territory import CATerritoryDTO, CATerritoryWithoutGeometryDTO
from idu_api.city_api.dto.territory_hierarchy import TerritoryHierarchyDTO
from idu_api.common.db.entities import territories_data, territory_types_dict, territory_indicators_data
import shapely.geometry as geom

from idu_api.urban_api.dto import TerritoryDTO, TerritoryWithoutGeometryDTO
from idu_api.urban_api.schemas.geometries import Geometry


async def get_territories_by_parent_id_and_level(
        conn: AsyncConnection,
        parent_id: int | str,
        level: int | None = None,
        ids: list[int] | None = None,
        type: int | None = None,
        geometry: geom.Polygon | geom.MultiPolygon | None = None,
        no_geometry: bool = False,
) -> list[CATerritoryDTO | CATerritoryWithoutGeometryDTO]:
    statement = select(territories_data.c.territory_id)
    if parent_id.isnumeric():
        statement = statement.where(territories_data.c.territory_id == int(parent_id))
    else:
        statement = statement.where(territories_data.c.name == parent_id)
    parent_territory = (await conn.execute(statement)).one_or_none()
    if parent_territory is None:
        raise HTTPException(status_code=404, detail="Given parent id is not found")

    territories_data_parents = territories_data.alias("territories_data_parents")
    statement = generate_select(territories_data_parents, no_geometry=no_geometry).select_from(
        territories_data.join(
            territory_types_dict, territory_types_dict.c.territory_type_id == territories_data.c.territory_type_id
        ).join(
            territories_data_parents,
            territories_data.c.parent_id == territories_data_parents.c.territory_id,
            isouter=True,
        )
    )

    cte_statement = statement.where(
        territories_data.c.parent_id == parent_territory.territory_id
        if parent_id is not None
        else territories_data.c.parent_id.is_(None)
    )
    cte_statement = cte_statement.cte(name="territories_recursive", recursive=True)

    recursive_part = statement.join(cte_statement, territories_data.c.parent_id == cte_statement.c.territory_id)

    statement = select(cte_statement.union_all(recursive_part))
    requested_territories = statement.cte("requested_territories")
    statement = select(
        requested_territories,
        territory_indicators_data.c.value.label("population"),
    ).select_from(
        requested_territories.join(
            territory_indicators_data,
            requested_territories.c.territory_id == territory_indicators_data.c.territory_id,
            isouter=True
        )
    ).where(
        or_(
            territory_indicators_data.c.indicator_id.is_(None),
            and_(
                territory_indicators_data.c.indicator_id == 1,
                territory_indicators_data.c.date_value == date(datetime.now().year - 1, 1, 1)
            )
        )
    )

    if level:
        statement = statement.where(requested_territories.c.level == level)
    if type:
        statement = statement.where(requested_territories.c.territory_type_id == type)
    if ids:
        statement = statement.where(
            requested_territories.c.territory_id.in_(ids)
        )
    if geometry and not no_geometry:
        given_geometry = select(ST_GeomFromText(str(geometry), text("4326"))).cte("given_geometry")

        statement = statement.where(
            ST_Covers(select(given_geometry).scalar_subquery(), requested_territories.c.geometry)
        )

    result = (await conn.execute(statement)).mappings().all()
    if not no_geometry:
        return [CATerritoryDTO(**territory) for territory in result]
    else:
        return [CATerritoryWithoutGeometryDTO(**territory) for territory in result]


async def get_territory_hierarchy_by_parent_id(
        conn: AsyncConnection,
        parent_id: int | str
) -> list[TerritoryHierarchyDTO]:
    statement = select(territories_data.c.territory_id)
    if parent_id.isnumeric():
        statement = statement.where(territories_data.c.territory_id == int(parent_id))
    else:
        statement = statement.where(territories_data.c.name == parent_id)
    parent_territory = (await conn.execute(statement)).one_or_none()
    if parent_territory is None:
        raise HTTPException(status_code=404, detail="Given parent id is not found")

    territories_data_parents = territories_data.alias("territories_data_parents")
    statement = select(
        territories_data.c.territory_id,
        territories_data.c.territory_type_id,
        territory_types_dict.c.name.label("territory_type_name"),
        territories_data.c.level,
    ).select_from(
        territories_data.join(
            territory_types_dict, territory_types_dict.c.territory_type_id == territories_data.c.territory_type_id
        ).join(
            territories_data_parents,
            territories_data.c.parent_id == territories_data_parents.c.territory_id,
            isouter=True,
        )
    )

    cte_statement = statement.where(
        territories_data.c.parent_id == parent_territory.territory_id
        if parent_id is not None
        else territories_data.c.parent_id.is_(None)
    )
    cte_statement = cte_statement.cte(name="territories_recursive", recursive=True)

    recursive_part = statement.join(cte_statement, territories_data.c.parent_id == cte_statement.c.territory_id)

    statement = select(cte_statement.union_all(recursive_part))
    requested_territories = statement.cte("requested_territories")
    statement = (
        select(
            requested_territories.c.territory_type_id,
            requested_territories.c.territory_type_name,
            requested_territories.c.level,
        ).group_by(
            requested_territories.c.territory_type_id,
            requested_territories.c.territory_type_name,
            requested_territories.c.level,
        ).order_by(
            requested_territories.c.level.asc(),
        )
    )
    result = (await conn.execute(statement)).mappings().all()

    return [TerritoryHierarchyDTO(**territory) for territory in result]


async def get_territory_ids_by_parent_id(
        conn: AsyncConnection,
        parent_id: int | str
) -> list[int]:
    result: list[CATerritoryDTO] = await get_territories_by_parent_id_and_level(conn, parent_id)
    return [territory.territory_id for territory in result]


async def get_children_territories_by_type(
        conn: AsyncConnection,
        territory: TerritoryDTO | TerritoryWithoutGeometryDTO,
        type: int, no_geometry: bool = False
) -> list[CATerritoryDTO | CATerritoryWithoutGeometryDTO]:
    territories_data_parents = territories_data.alias("territories_data_parents")
    if not no_geometry:
        statement = select(
            territories_data.c.territory_id,
            territories_data.c.territory_type_id,
            territory_types_dict.c.name.label("territory_type_name"),
            territories_data.c.parent_id,
            territories_data_parents.c.name.label("parent_name"),
            territories_data.c.name,
            territory_indicators_data.c.value.label("population"),
            cast(ST_AsGeoJSON(territories_data.c.geometry), JSONB).label("geometry"),
            territories_data.c.level,
            territories_data.c.properties,
            cast(ST_AsGeoJSON(territories_data.c.centre_point), JSONB).label("centre_point"),
            territories_data.c.admin_center,
            territories_data.c.okato_code,
            territories_data.c.created_at,
            territories_data.c.updated_at,
        )
    else:
        statement = select(
            territories_data.c.territory_id,
            territories_data.c.territory_type_id,
            territory_types_dict.c.name.label("territory_type_name"),
            territories_data.c.parent_id,
            territories_data.c.name,
            territory_indicators_data.c.value.label("population"),
            territories_data.c.level,
            territories_data.c.properties,
            territories_data.c.admin_center,
            territories_data.c.okato_code,
            territories_data.c.created_at,
            territories_data.c.updated_at,
        )
    statement = (
        statement.select_from(
            territories_data.join(
                territory_types_dict, territory_types_dict.c.territory_type_id == territories_data.c.territory_type_id
            ).join(
                territory_indicators_data, territories_data.c.territory_id == territory_indicators_data.c.territory_id
            ).join(
                territories_data_parents,
                territories_data.c.parent_id == territories_data_parents.c.territory_id,
                isouter=True,
            )
        )
        .where(territories_data.c.parent_id == territory.territory_id)
        .where(territories_data.c.territory_type_id == type)
        .where(
            or_(
                territory_indicators_data.c.indicator_id.is_(None),
                and_(
                    territory_indicators_data.c.indicator_id == 1,
                    territory_indicators_data.c.date_value == date(datetime.now().year - 1, 1, 1)
                )
            )
        )
    )

    result = (await conn.execute(statement)).mappings().all()
    if not no_geometry:
        return [CATerritoryDTO(**territory) for territory in result]
    else:
        return [CATerritoryWithoutGeometryDTO(**territory) for territory in result]


async def get_ca_territory_by_id(
        conn: AsyncConnection, territory_id: int | str, no_geometry: bool = False
) -> CATerritoryDTO | CATerritoryWithoutGeometryDTO:
    territories_data_parents = territories_data.alias("territories_data_parents")
    if not no_geometry:
        statement = select(
            territories_data.c.territory_id,
            territories_data.c.territory_type_id,
            territory_types_dict.c.name.label("territory_type_name"),
            territories_data.c.parent_id,
            territories_data_parents.c.name.label("parent_name"),
            territories_data.c.name,
            territory_indicators_data.c.value.label("population"),
            cast(ST_AsGeoJSON(territories_data.c.geometry), JSONB).label("geometry"),
            territories_data.c.level,
            territories_data.c.properties,
            cast(ST_AsGeoJSON(territories_data.c.centre_point), JSONB).label("centre_point"),
            territories_data.c.admin_center,
            territories_data.c.okato_code,
            territories_data.c.created_at,
            territories_data.c.updated_at,
        )
    else:
        statement = select(
            territories_data.c.territory_id,
            territories_data.c.territory_type_id,
            territory_types_dict.c.name.label("territory_type_name"),
            territories_data.c.parent_id,
            territories_data.c.name,
            territory_indicators_data.c.value.label("population"),
            territories_data.c.level,
            territories_data.c.properties,
            territories_data.c.admin_center,
            territories_data.c.okato_code,
            territories_data.c.created_at,
            territories_data.c.updated_at,
        )
    statement = (
        statement.select_from(
            territories_data.join(
                territory_types_dict, territory_types_dict.c.territory_type_id == territories_data.c.territory_type_id
            ).join(
                territory_indicators_data, territories_data.c.territory_id == territory_indicators_data.c.territory_id
            ).join(
                territories_data_parents,
                territories_data.c.parent_id == territories_data_parents.c.territory_id,
                isouter=True,
            )
        )
        .where(
            or_(
                territory_indicators_data.c.indicator_id.is_(None),
                and_(
                    territory_indicators_data.c.indicator_id == 1,
                    territory_indicators_data.c.date_value == date(datetime.now().year - 1, 1, 1)
                )
            )
        )
    )
    if territory_id.isnumeric():
        statement = statement.where(territories_data.c.territory_id == int(territory_id))
    else:
        statement = statement.where(territories_data.c.name == territory_id)

    try:
        result = (await conn.execute(statement)).mappings().one()
    except Exception as e:
        print(e)
        raise HTTPException(404, "TERRITORY_NOT_FOUND")
    if not no_geometry:
        return CATerritoryDTO(**result)
    else:
        return CATerritoryWithoutGeometryDTO(**result)


async def construct_hierarchy(
        conn: AsyncConnection,
        territory: CATerritoryDTO | CATerritoryWithoutGeometryDTO,
        hierarchy: list[TerritoryHierarchyDTO], cur: int,
        no_geometry: bool = False,
) -> dict:
    id = territory.territory_id
    level = territory.level
    if type(territory) is CATerritoryDTO:
        territory.geometry = None if not territory.geometry else Geometry.from_shapely_geometry(territory.geometry)
        territory.centre_point = Geometry.from_shapely_geometry(territory.centre_point)
    result = territory.__dict__

    if cur == len(hierarchy):
        return result

    end = len(hierarchy)
    for i in range(cur, len(hierarchy)):
        if hierarchy[i].level != level + 1:
            end = i
            break

    i = cur
    while i < end:
        territories: list[CATerritoryDTO | CATerritoryWithoutGeometryDTO | dict] = await get_children_territories_by_type(
            conn, territory, hierarchy[i].territory_type_id, no_geometry
        )
        for j in range(len(territories)):
            territories[j] = await construct_hierarchy(conn, territories[j], hierarchy, end, no_geometry)
        if len(territories) != 0:
            result[hierarchy[i].territory_type_name] = territories
        i += 1
    return result


async def get_territory_hierarchy(conn: AsyncConnection, territory_id: int | str, no_geometry: bool = False) -> dict:
    type_hierarchy = await get_territory_hierarchy_by_parent_id(conn, territory_id)
    if len(type_hierarchy) != 0 and type_hierarchy[-1].territory_type_name == "Квартал":
        type_hierarchy.pop()
    territory = await get_ca_territory_by_id(conn, territory_id, no_geometry)
    return await construct_hierarchy(conn, territory, type_hierarchy, 0, no_geometry)


def generate_select(
        territories_data_parents: NamedFromClause,
        no_geometry: bool = False
) -> Select:
    if not no_geometry:
        return select(
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
    else:
        return select(
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
        )


"""
WITH RECURSIVE territories_recursive(territory_id, territory_type_id, territory_type_name, parent_id, parent_name, name, geometry, level, properties, centre_point, admin_center, okato_code, created_at, updated_at) AS 
(SELECT territories_data.territory_id AS territory_id, territories_data.territory_type_id AS territory_type_id, territory_types_dict.name AS territory_type_name, territories_data.parent_id AS parent_id, territories_data_parents.name AS parent_name, territories_data.name AS name, CAST(ST_AsGeoJSON(territories_data.geometry) AS JSONB) AS geometry, territories_data.level AS level, territories_data.properties AS properties, CAST(ST_AsGeoJSON(territories_data.centre_point) AS JSONB) AS centre_point, territories_data.admin_center AS admin_center, territories_data.okato_code AS okato_code, territories_data.created_at AS created_at, territories_data.updated_at AS updated_at 
FROM territories_data JOIN territory_types_dict ON territory_types_dict.territory_type_id = territories_data.territory_type_id LEFT OUTER JOIN territories_data AS territories_data_parents ON territories_data.parent_id = territories_data_parents.territory_id 
WHERE territories_data.parent_id = $1::INTEGER UNION ALL SELECT territories_data.territory_id AS territory_id, territories_data.territory_type_id AS territory_type_id, territory_types_dict.name AS territory_type_name, territories_data.parent_id AS parent_id, territories_data_parents.name AS parent_name, territories_data.name AS name, CAST(ST_AsGeoJSON(territories_data.geometry) AS JSONB) AS geometry, territories_data.level AS level, territories_data.properties AS properties, CAST(ST_AsGeoJSON(territories_data.centre_point) AS JSONB) AS centre_point, territories_data.admin_center AS admin_center, territories_data.okato_code AS okato_code, territories_data.created_at AS created_at, territories_data.updated_at AS updated_at 
FROM territories_data JOIN territory_types_dict ON territory_types_dict.territory_type_id = territories_data.territory_type_id LEFT OUTER JOIN territories_data AS territories_data_parents ON territories_data.parent_id = territories_data_parents.territory_id JOIN territories_recursive ON territories_data.parent_id = territories_recursive.territory_id), 
requested_territories AS 
(SELECT territories_recursive.territory_id AS territory_id, territories_recursive.territory_type_id AS territory_type_id, territories_recursive.territory_type_name AS territory_type_name, territories_recursive.parent_id AS parent_id, territories_recursive.parent_name AS parent_name, territories_recursive.name AS name, territories_recursive.geometry AS geometry, territories_recursive.level AS level, territories_recursive.properties AS properties, territories_recursive.centre_point AS centre_point, territories_recursive.admin_center AS admin_center, territories_recursive.okato_code AS okato_code, territories_recursive.created_at AS created_at, territories_recursive.updated_at AS updated_at 
FROM territories_recursive)
 SELECT requested_territories.territory_type_id, requested_territories.territory_type_name, requested_territories.level
FROM requested_territories 
group by requested_territories.territory_type_id, requested_territories.territory_type_name, requested_territories.level
order by requested_territories.level asc
"""
