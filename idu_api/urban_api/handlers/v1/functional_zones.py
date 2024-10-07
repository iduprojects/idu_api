"""Functional zones endpoints are defined here."""

from fastapi import Query, Request, Path
from starlette import status

from idu_api.urban_api.logic.functional_zones import FunctionalZonesService
from idu_api.urban_api.schemas import (
    FunctionalZoneType,
    FunctionalZoneTypePost,
    ProfilesReclamationData,
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
    "/profiles_reclamation",
    response_model=list[ProfilesReclamationData],
    status_code=status.HTTP_200_OK,
)
async def get_profiles_reclamation_data(request: Request) -> list[ProfilesReclamationData]:
    """Get a list of profiles reclamation data."""
    functional_zones_service: FunctionalZonesService = request.state.functional_zones_service

    profiles_reclamations = await functional_zones_service.get_profiles_reclamation_data()

    return [ProfilesReclamationData.from_dto(profiles_reclamation) for profiles_reclamation in profiles_reclamations]


@functional_zones_router.get(
    "/profiles_reclamation/matrix",
    response_model=ProfilesReclamationDataMatrix,
    status_code=status.HTTP_200_OK,
)
async def get_profiles_reclamation_data_matrix(
    request: Request, labels: str | None = Query(None, description="list of profiles labels separated with comma")
) -> ProfilesReclamationDataMatrix:
    """Get a matrix of profiles reclamation data for specific labels.

    If labels is not specified, all profiles reclamation data will be returned."""
    functional_zones_service: FunctionalZonesService = request.state.functional_zones_service

    labels_array: list[int]
    if labels is None:
        labels_array = await functional_zones_service.get_all_sources()
    else:
        labels_array = [int(label.strip()) for label in labels.split(sep=",")]

    profiles_reclamation_matrix = await functional_zones_service.get_profiles_reclamation_data_matrix(labels_array)

    return ProfilesReclamationDataMatrix.from_dto(profiles_reclamation_matrix)


@functional_zones_router.get(
    "/profiles_reclamation/territory_matrix",
    response_model=ProfilesReclamationDataMatrix,
    status_code=status.HTTP_200_OK,
)
async def get_profiles_reclamation_data_matrix_by_territory_id(
    request: Request,
    territory_id: int | None = Query(None, description="territory identifier")
) -> ProfilesReclamationDataMatrix:
    """Get a matrix of profiles reclamation data for given territory.

    Territory id should be null to get default matrix."""
    functional_zones_service: FunctionalZonesService = request.state.functional_zones_service

    matrix_dto = await functional_zones_service.get_profiles_reclamation_data_matrix_by_territory_id(territory_id)

    return ProfilesReclamationDataMatrix.from_dto(matrix_dto)


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

    new_profiles_reclamation = await functional_zones_service.add_profiles_reclamation_data(profiles_reclamation)

    return ProfilesReclamationData.from_dto(new_profiles_reclamation)


@functional_zones_router.put(
    "/profiles_reclamation/{profile_reclamation_id}",
    response_model=ProfilesReclamationData,
    status_code=status.HTTP_200_OK,
)
async def put_profiles_reclamation_data(
    request: Request,
    profiles_reclamation: ProfilesReclamationDataPut,
    profile_reclamation_id: int = Path(..., description="profile reclamation identifier"),
) -> ProfilesReclamationData:
    """Put profiles reclamation data."""
    functional_zones_service: FunctionalZonesService = request.state.functional_zones_service

    changed_profiles_reclamation = await functional_zones_service.put_profiles_reclamation_data(
        profile_reclamation_id, profiles_reclamation
    )

    return ProfilesReclamationData.from_dto(changed_profiles_reclamation)
