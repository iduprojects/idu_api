"""
Functional zones endpoints are defined here.
"""

from fastapi import Request
from starlette import status

from idu_api.urban_api.logic.functional_zones import FunctionalZonesService
from idu_api.urban_api.schemas import FunctionalZoneType, FunctionalZoneTypePost

from .routers import functional_zones_router


@functional_zones_router.get(
    "/functional_zones_types",
    response_model=list[FunctionalZoneType],
    status_code=status.HTTP_200_OK,
)
async def get_functional_zone_types(request: Request) -> list[FunctionalZoneType]:
    """Get a list of functional zone type."""
    functional_zones_service: FunctionalZonesService = request.state.functional_zones_service

    functional_zone_types = await functional_zones_service.get_functional_zone_types()

    return [FunctionalZoneType.from_dto(zone_type) for zone_type in functional_zone_types]


@functional_zones_router.post(
    "/functional_zones_types",
    response_model=FunctionalZoneType,
    status_code=status.HTTP_201_CREATED,
)
async def add_functional_zone_type(request: Request, zone_type: FunctionalZoneTypePost) -> FunctionalZoneType:
    """Add a new functional zone type."""
    functional_zones_service: FunctionalZonesService = request.state.functional_zones_service

    new_zone_type = await functional_zones_service.add_functional_zone_type(zone_type)

    return FunctionalZoneType.from_dto(new_zone_type)
