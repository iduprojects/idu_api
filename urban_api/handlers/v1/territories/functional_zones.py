"""Functional zones territories-related handlers are defined here."""

from typing import List, Optional

from fastapi import Path, Query, Request
from starlette import status

from urban_api.logic.territories import TerritoriesService
from urban_api.schemas import FunctionalZoneData

from .routers import territories_router


@territories_router.get(
    "/territory/{territory_id}/functional_zones",
    response_model=List[FunctionalZoneData],
    status_code=status.HTTP_200_OK,
)
async def get_functional_zones_for_territory(
    request: Request,
    territory_id: int = Path(description="territory id", gt=0),
    functional_zone_type_id: Optional[int] = Query(None, description="functional_zone_type_id", gt=0),
) -> List[FunctionalZoneData]:
    """Get functional zones for territory.

    functional_zone_type could be specified in parameters.
    """
    territories_service: TerritoriesService = request.state.territories_service

    zones = await territories_service.get_functional_zones_by_territory_id(territory_id, functional_zone_type_id)

    return [FunctionalZoneData.from_dto(zone) for zone in zones]
