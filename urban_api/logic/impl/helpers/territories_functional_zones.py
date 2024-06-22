"""Territories functional zones internal logic is defined here."""

from typing import Callable, Optional

from fastapi import HTTPException
from geoalchemy2.functions import ST_AsGeoJSON
from sqlalchemy import cast, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncConnection

from urban_api.db.entities import functional_zones_data, territories_data
from urban_api.dto import FunctionalZoneDataDTO

func: Callable


async def get_functional_zones_by_territory_id_from_db(
    conn: AsyncConnection,
    territory_id: int,
    functional_zone_type_id: Optional[int],
) -> list[FunctionalZoneDataDTO]:
    """Get functional zones with geometry by territory id."""

    statement = select(territories_data).where(territories_data.c.territory_id == territory_id)
    territory = (await conn.execute(statement)).one_or_none()
    if territory is None:
        raise HTTPException(status_code=404, detail="Given territory id is not found")

    statement = select(
        functional_zones_data.c.functional_zone_id,
        functional_zones_data.c.territory_id,
        functional_zones_data.c.functional_zone_type_id,
        cast(ST_AsGeoJSON(functional_zones_data.c.geometry), JSONB).label("geometry"),
    ).where(functional_zones_data.c.territory_id == territory_id)

    if functional_zone_type_id is not None:
        statement = statement.where(functional_zones_data.c.functional_zone_type_id == functional_zone_type_id)

    result = (await conn.execute(statement)).mappings().all()

    return [FunctionalZoneDataDTO(**zone) for zone in result]
