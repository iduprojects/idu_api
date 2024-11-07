"""Functional zones territories-related handlers are defined here."""

from fastapi import Path, Query, Request
from starlette import status

from idu_api.urban_api.logic.territories import TerritoriesService
from idu_api.urban_api.schemas import FunctionalZoneData

from .routers import territories_router


@territories_router.get(
    "/territory/{territory_id}/functional_zones",
    response_model=list[FunctionalZoneData],
    status_code=status.HTTP_200_OK,
)
async def get_functional_zones_for_territory(
    request: Request,
    territory_id: int = Path(..., description="territory id", gt=0),
    functional_zone_type_id: int | None = Query(None, description="functional_zone_type_id", gt=0),
    include_child_territories: bool = Query(False, description="to get functional zones for child territories"),
) -> list[FunctionalZoneData]:
    """Get functional zones for territory.

    functional_zone_type and include_child_territories could be specified in parameters.
    """
    territories_service: TerritoriesService = request.state.territories_service

    zones = await territories_service.get_functional_zones_by_territory_id(
        territory_id, functional_zone_type_id, include_child_territories
    )

    return [FunctionalZoneData.from_dto(zone) for zone in zones]
