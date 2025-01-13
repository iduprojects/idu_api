"""Functional zones endpoints are defined here."""

from fastapi import Path, Query, Request
from starlette import status

from idu_api.urban_api.logic.functional_zones import FunctionalZonesService
from idu_api.urban_api.schemas import (
    FunctionalZoneData,
    FunctionalZoneDataPatch,
    FunctionalZoneDataPost,
    FunctionalZoneDataPut,
    FunctionalZoneType,
    FunctionalZoneTypePost,
    ProfilesReclamationData,
    ProfilesReclamationDataDelete,
    ProfilesReclamationDataMatrix,
    ProfilesReclamationDataPost,
    ProfilesReclamationDataPut,
)

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


@functional_zones_router.get(
    "/profiles_reclamation/matrix",
    response_model=ProfilesReclamationDataMatrix,
    status_code=status.HTTP_200_OK,
)
async def get_profiles_reclamation_data_matrix(
    request: Request,
    labels: str | None = Query(None, description="list of profiles labels separated with comma"),
    territory_id: int | None = Query(None, description="territory identifier"),
) -> ProfilesReclamationDataMatrix:
    """Get a matrix of profiles reclamation data for specific labels and territory identifier.

    If labels is not specified, all profiles reclamation data will be returned.

    If territory identifier is not specified, basic profile reclamation data matrix will be returned.
    """
    functional_zones_service: FunctionalZonesService = request.state.functional_zones_service

    labels_array: list[int]
    if labels is None:
        labels_array = await functional_zones_service.get_all_sources(territory_id)
    else:
        labels_array = [int(label.strip()) for label in labels.split(sep=",")]

    profiles_reclamation_matrix = await functional_zones_service.get_profiles_reclamation_data_matrix(
        labels_array, territory_id
    )

    return ProfilesReclamationDataMatrix.from_dto(profiles_reclamation_matrix)


@functional_zones_router.post(
    "/profiles_reclamation",
    response_model=ProfilesReclamationData,
    status_code=status.HTTP_201_CREATED,
)
async def add_profiles_reclamation_data(
    request: Request, profiles_reclamation: ProfilesReclamationDataPost
) -> ProfilesReclamationData:
    """Add a new profiles reclamation data."""
    functional_zones_service: FunctionalZonesService = request.state.functional_zones_service

    profiles_reclamation_data = await functional_zones_service.add_profiles_reclamation_data(profiles_reclamation)

    return ProfilesReclamationData.from_dto(profiles_reclamation_data)


@functional_zones_router.put(
    "/profiles_reclamation",
    response_model=ProfilesReclamationData,
    status_code=status.HTTP_200_OK,
)
async def put_profiles_reclamation_data(
    request: Request,
    profiles_reclamation: ProfilesReclamationDataPut,
) -> ProfilesReclamationData:
    """Update profiles reclamation data if exists else create new profiles reclamation data."""
    functional_zones_service: FunctionalZonesService = request.state.functional_zones_service

    profiles_reclamation_data = await functional_zones_service.put_profiles_reclamation_data(profiles_reclamation)

    return ProfilesReclamationData.from_dto(profiles_reclamation_data)


@functional_zones_router.delete(
    "/profiles_reclamation",
    response_model=dict[str, str],
    status_code=status.HTTP_201_CREATED,
)
async def delete_profiles_reclamation_data(
    request: Request, profiles_reclamation: ProfilesReclamationDataDelete
) -> dict[str, str]:
    """Delete profiles reclamation data by source and target profiles identifiers and territory identifier."""
    functional_zones_service: FunctionalZonesService = request.state.functional_zones_service

    return await functional_zones_service.delete_profiles_reclamation_data(
        profiles_reclamation.source_profile_id,
        profiles_reclamation.target_profile_id,
        profiles_reclamation.territory_id,
    )


@functional_zones_router.post(
    "/functional_zones",
    response_model=FunctionalZoneData,
    status_code=status.HTTP_201_CREATED,
)
async def add_functional_zone(
    request: Request,
    functional_zone: FunctionalZoneDataPost,
) -> FunctionalZoneData:
    """Add functional zone."""
    functional_zones_service: FunctionalZonesService = request.state.functional_zones_service

    functional_zone_dto = await functional_zones_service.add_functional_zone(functional_zone)

    return FunctionalZoneData.from_dto(functional_zone_dto)


@functional_zones_router.put(
    "/functional_zones/{functional_zone_id}",
    response_model=FunctionalZoneData,
    status_code=status.HTTP_200_OK,
)
async def put_functional_zone(
    request: Request,
    functional_zone: FunctionalZoneDataPut,
    functional_zone_id: int = Path(..., description="functional zone id", gt=0),
) -> FunctionalZoneData:
    """Update functional zone by all its attributes."""
    functional_zones_service: FunctionalZonesService = request.state.functional_zones_service

    functional_zone_dto = await functional_zones_service.put_functional_zone(functional_zone_id, functional_zone)

    return FunctionalZoneData.from_dto(functional_zone_dto)


@functional_zones_router.patch(
    "/functional_zones/{functional_zone_id}",
    response_model=FunctionalZoneData,
    status_code=status.HTTP_200_OK,
)
async def patch_functional_zone_for_territory(
    request: Request,
    functional_zone: FunctionalZoneDataPatch,
    functional_zone_id: int = Path(..., description="functional zone id", gt=0),
) -> FunctionalZoneData:
    """Update functional zone by only given attributes."""
    functional_zones_service: FunctionalZonesService = request.state.functional_zones_service

    functional_zone_dto = await functional_zones_service.patch_functional_zone(functional_zone_id, functional_zone)

    return FunctionalZoneData.from_dto(functional_zone_dto)


@functional_zones_router.delete(
    "/functional_zones/{functional_zone_id}",
    response_model=dict,
    status_code=status.HTTP_200_OK,
)
async def delete_functional_zone(
    request: Request,
    functional_zone_id: int = Path(..., description="functional zone id", gt=0),
) -> dict:
    """Delete specific functional zone by identifier."""
    functional_zones_service: FunctionalZonesService = request.state.functional_zones_service

    return await functional_zones_service.delete_functional_zone(functional_zone_id)
