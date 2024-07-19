from fastapi import HTTPException
from geoalchemy2.functions import ST_AsGeoJSON
from sqlalchemy import select, cast
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import territories_data, territory_types_dict
from idu_api.urban_api.dto import TerritoryDTO


async def get_territories_by_parent_id_and_level(
        conn: AsyncConnection,
        parent_id: int,
        level: int
) -> list[TerritoryDTO]:
    statement = select(territories_data.c.territory_id).where(territories_data.c.territory_id == parent_id)
    parent_territory = (await conn.execute(statement)).one_or_none()
    if parent_territory is None:
        raise HTTPException(status_code=404, detail="Given parent id is not found")

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

    cte_statement = statement.where(
        territories_data.c.parent_id == parent_id
        if parent_id is not None
        else territories_data.c.parent_id.is_(None)
    )
    cte_statement = cte_statement.cte(name="territories_recursive", recursive=True)

    recursive_part = statement.join(cte_statement, territories_data.c.parent_id == cte_statement.c.territory_id)

    statement = select(cte_statement.union_all(recursive_part))
    requested_territories = statement.cte("requested_territories")
    statement = select(requested_territories).where(requested_territories.c.level == level)

    result = (await conn.execute(statement)).mappings().all()

    return [TerritoryDTO(**territory) for territory in result]
