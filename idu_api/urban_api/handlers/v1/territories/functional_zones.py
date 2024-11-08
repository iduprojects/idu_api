"""Functional zones territories-related handlers are defined here."""

from fastapi import Path, Query, Request
from starlette import status

from idu_api.urban_api.logic.territories import TerritoriesService
from idu_api.urban_api.schemas import (
    FunctionalZoneData,
    FunctionalZoneDataPatch,
    FunctionalZoneDataPost,
    FunctionalZoneDataPut,
)

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


@territories_router.post(
    "/territory/{territory_id}/functional_zone",
    response_model=FunctionalZoneData,
    status_code=status.HTTP_201_CREATED,
)
async def add_functional_zone_for_territory(
    request: Request,
    functional_zone: FunctionalZoneDataPost,
    territory_id: int = Path(..., description="territory id", gt=0),
) -> FunctionalZoneData:
    """Add functional zone for territory."""
    territories_service: TerritoriesService = request.state.territories_service

    functional_zone_dto = await territories_service.add_functional_zone_for_territory(territory_id, functional_zone)

    return FunctionalZoneData.from_dto(functional_zone_dto)


@territories_router.post(
    "/territory/{territory_id}/functional_zones",
    response_model=list[FunctionalZoneData],
    status_code=status.HTTP_201_CREATED,
)
async def add_functional_zones_for_territory(
    request: Request,
    functional_zones: list[FunctionalZoneDataPost],
    territory_id: int = Path(..., description="territory id", gt=0),
) -> list[FunctionalZoneData]:
    """Add a bunch of functional zones for territory."""
    territories_service: TerritoriesService = request.state.territories_service

    functional_zones_dto = await territories_service.add_functional_zones_for_territory(territory_id, functional_zones)

    return [FunctionalZoneData.from_dto(functional_zone_dto) for functional_zone_dto in functional_zones_dto]


@territories_router.put(
    "/territory/{territory_id}/functional_zone",
    response_model=FunctionalZoneData,
    status_code=status.HTTP_200_OK,
)
async def put_functional_zone_for_territory(
    request: Request,
    functional_zone: FunctionalZoneDataPut,
    territory_id: int = Path(..., description="territory id", gt=0),
    functional_zone_id: int = Query(..., description="functional zone id", gt=0),
) -> FunctionalZoneData:
    """Put functional zone for territory."""
    territories_service: TerritoriesService = request.state.territories_service

    functional_zone_dto = await territories_service.put_functional_zone_for_territory(
        territory_id, functional_zone_id, functional_zone
    )

    return FunctionalZoneData.from_dto(functional_zone_dto)


@territories_router.patch(
    "/territory/{territory_id}/functional_zone",
    response_model=FunctionalZoneData,
    status_code=status.HTTP_200_OK,
)
async def patch_functional_zone_for_territory(
    request: Request,
    functional_zone: FunctionalZoneDataPatch,
    territory_id: int = Path(..., description="territory id", gt=0),
    functional_zone_id: int = Query(..., description="functional zone id", gt=0),
) -> FunctionalZoneData:
    """Patch functional zone for territory."""
    territories_service: TerritoriesService = request.state.territories_service

    functional_zone_dto = await territories_service.patch_functional_zone_for_territory(
        territory_id, functional_zone_id, functional_zone
    )

    return FunctionalZoneData.from_dto(functional_zone_dto)


@territories_router.delete(
    "/territory/{territory_id}/functional_zone",
    response_model=dict,
    status_code=status.HTTP_200_OK,
)
async def delete_specific_functional_zone_for_territory(
    request: Request,
    territory_id: int = Path(..., description="territory id", gt=0),
    functional_zone_id: int = Query(..., description="functional zone id", gt=0),
) -> dict:
    """Delete specific functional zone for territory."""
    territories_service: TerritoriesService = request.state.territories_service

    return await territories_service.delete_specific_functional_zone_for_territory(territory_id, functional_zone_id)


@territories_router.delete(
    "/territory/{territory_id}/functional_zones",
    response_model=dict,
    status_code=status.HTTP_200_OK,
)
async def delete_all_functional_zones_for_territory(
    request: Request,
    territory_id: int = Path(..., description="territory id", gt=0),
) -> dict:
    """Delete all functional zones for territory."""
    territories_service: TerritoriesService = request.state.territories_service

    return await territories_service.delete_all_functional_zones_for_territory(territory_id)
