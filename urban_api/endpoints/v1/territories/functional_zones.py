"""Functional zones territories-related endpoints are defined here."""

from typing import List, Optional

from fastapi import Depends, Path, Query
from sqlalchemy.ext.asyncio import AsyncConnection
from starlette import status

from urban_api.db.connection import get_connection
from urban_api.logic.territories import get_functional_zones_by_territory_id_from_db
from urban_api.schemas import FunctionalZoneData

from .routers import territories_router


@territories_router.get(
    "/territory/{territory_id}/functional_zones",
    response_model=List[FunctionalZoneData],
    status_code=status.HTTP_200_OK,
)
async def get_functional_zones_for_territory(
    territory_id: int = Path(description="territory id", gt=0),
    functional_zone_type_id: Optional[int] = Query(None, description="functional_zone_type_id", gt=0),
    connection: AsyncConnection = Depends(get_connection),
) -> List[FunctionalZoneData]:
    """
    Summary:
        Get functional zones for territory

    Description:
        Get functional zones for territory, functional_zone_type could be specified in parameters
    """

    zones = await get_functional_zones_by_territory_id_from_db(territory_id, connection, functional_zone_type_id)

    return [FunctionalZoneData.from_dto(zone) for zone in zones]
