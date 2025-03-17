"""Functional zones endpoints are defined here."""

from fastapi import Path, Query, Request, HTTPException
from starlette import status

from idu_api.urban_api.logic.functional_zones import FunctionalZonesService
from idu_api.urban_api.schemas import (
    FunctionalZone,
    FunctionalZonePatch,
    FunctionalZonePost,
    FunctionalZonePut,
    FunctionalZoneType,
    FunctionalZoneTypePost,
    OkResponse,
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
    """
    ## Get the list of functional zone types.

    ### Returns:
    - **list[FunctionalZoneType]**: A list of functional zone types.
    """
    functional_zones_service: FunctionalZonesService = request.state.functional_zones_service

    functional_zone_types = await functional_zones_service.get_functional_zone_types()

    return [FunctionalZoneType.from_dto(zone_type) for zone_type in functional_zone_types]


@functional_zones_router.post(
    "/functional_zones_types",
    response_model=FunctionalZoneType,
    status_code=status.HTTP_201_CREATED,
)
async def add_functional_zone_type(request: Request, zone_type: FunctionalZoneTypePost) -> FunctionalZoneType:
    """
    ## Create a new functional zone type.

    ### Parameters:
    - **zone_type** (FunctionalZoneTypePost, Body): Data for the new functional zone type.

    ### Returns:
    - **FunctionalZoneType**: The created functional zone type.

    ### Errors:
    - **409 Conflict**: If a functional zone type with the such name already exists.
    """
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
    territory_id: int | None = Query(None, description="territory identifier", gt=0),
) -> ProfilesReclamationDataMatrix:
    """
    ## Get a matrix of profiles reclamation data.

    ### Parameters:
    - **labels** (str | None, Query): Comma-separated list of profile labels to filter results. If labels is skipped, it will return all profiles reclamation data.
    - **territory_id** (int | None, Query): Filters results by a specific territory.

    ### Returns:
    - **ProfilesReclamationDataMatrix**: A matrix of profiles reclamation data.

    ### Errors:
    - **400 Bad Request**: If labels were passed incorrectly.
    - **404 Not Found**: If the territory or label does not exist.
    """
    functional_zones_service: FunctionalZonesService = request.state.functional_zones_service

    labels_array: list[int]
    if labels is None:
        labels_array = await functional_zones_service.get_all_sources()
    else:
        try:
            labels_array = [int(label.strip()) for label in labels.split(",")]
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))

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
    """
    ## Create new profiles reclamation data.

    ### Parameters:
    - **profiles_reclamation** (ProfilesReclamationDataPost, Body): Data for the new profiles reclamation entry.

    ### Returns:
    - **ProfilesReclamationData**: The created profiles reclamation data.

    ### Errors:
    - **404 Not Found**: If related entity does not exist.
    - **409 Conflict**: If profile reclamation data with the such parameters does not exist.
    """
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
    """
    ## Update or create profiles reclamation data.

    **NOTE:** If a profiles reclamation data with the specified attributes already exists, it will be updated.
    Otherwise, a new profiles reclamation data will be created.

    ### Parameters:
    - **profiles_reclamation** (ProfilesReclamationDataPut, Body): Data for updating or creating a profiles reclamation entry.

    ### Returns:
    - **ProfilesReclamationData**: The updated or newly created profiles reclamation data.

    ### Errors:
    - **404 Not Found**: If related entity does not exist.
    - **409 Conflict**: If profile reclamation data with the such parameters does not exist.
    """
    functional_zones_service: FunctionalZonesService = request.state.functional_zones_service

    profiles_reclamation_data = await functional_zones_service.put_profiles_reclamation_data(profiles_reclamation)

    return ProfilesReclamationData.from_dto(profiles_reclamation_data)


@functional_zones_router.delete(
    "/profiles_reclamation",
    response_model=OkResponse,
    status_code=status.HTTP_200_OK,
)
async def delete_profiles_reclamation_data(
    request: Request, profiles_reclamation: ProfilesReclamationDataDelete
) -> OkResponse:
    """
    ## Delete profiles reclamation data.

    ### Parameters:
    - **profiles_reclamation** (ProfilesReclamationDataDelete, Body): Identifiers of the source and target profiles and the territory.

    ### Returns:
    - **OkResponse**: A confirmation message of the deletion.

    ### Errors:
    - **404 Not Found**: If the specified profiles reclamation data does not exist.
    """
    functional_zones_service: FunctionalZonesService = request.state.functional_zones_service

    await functional_zones_service.delete_profiles_reclamation_data(
        profiles_reclamation.source_profile_id,
        profiles_reclamation.target_profile_id,
        profiles_reclamation.territory_id,
    )

    return OkResponse()


@functional_zones_router.post(
    "/functional_zones",
    response_model=FunctionalZone,
    status_code=status.HTTP_201_CREATED,
)
async def add_functional_zone(
    request: Request,
    functional_zone: FunctionalZonePost,
) -> FunctionalZone:
    """
    ## Create a new functional zone.

    ### Parameters:
    - **functional_zone** (FunctionalZonePost, Body): Data for the new functional zone.

    ### Returns:
    - **FunctionalZone**: The created functional zone.

    ### Errors:
    - **404 Not Found**: If related entity does not exist.
    """
    functional_zones_service: FunctionalZonesService = request.state.functional_zones_service

    functional_zone_dto = await functional_zones_service.add_functional_zone(functional_zone)

    return FunctionalZone.from_dto(functional_zone_dto)


@functional_zones_router.put(
    "/functional_zones/{functional_zone_id}",
    response_model=FunctionalZone,
    status_code=status.HTTP_200_OK,
    deprecated=True,
)
async def put_functional_zone(
    request: Request,
    functional_zone: FunctionalZonePut,
    functional_zone_id: int = Path(..., description="functional zone identifier", gt=0),
) -> FunctionalZone:
    """
    ## Update a functional zone by replacing all attributes.

    **WARNING:** This method has been deprecated since version 0.34.0 and will be removed in version 1.0.
    Instead, use PATCH method.

    ### Parameters:
    - **functional_zone_id** (int, Path): Unique identifier of the functional zone.
    - **functional_zone** (FunctionalZonePut, Body): New data for the functional zone.

    ### Returns:
    - **FunctionalZone**: The updated functional zone.

    ### Errors:
    - **404 Not Found**: If the functional zone (or related entity) does not exist.
    """
    functional_zones_service: FunctionalZonesService = request.state.functional_zones_service

    functional_zone_dto = await functional_zones_service.put_functional_zone(functional_zone_id, functional_zone)

    return FunctionalZone.from_dto(functional_zone_dto)


@functional_zones_router.patch(
    "/functional_zones/{functional_zone_id}",
    response_model=FunctionalZone,
    status_code=status.HTTP_200_OK,
)
async def patch_functional_zone_for_territory(
    request: Request,
    functional_zone: FunctionalZonePatch,
    functional_zone_id: int = Path(..., description="functional zone identifier", gt=0),
) -> FunctionalZone:
    """
    ## Partially update a functional zone.

    ### Parameters:
    - **functional_zone_id** (int, Path): Unique identifier of the functional zone.
    - **functional_zone** (FunctionalZonePatch, Body): Fields to update in the functional zone.

    ### Returns:
    - **FunctionalZone**: The updated functional zone with modified attributes.

    ### Errors:
    - **404 Not Found**: If the functional zone (or related entity) does not exist.
    """
    functional_zones_service: FunctionalZonesService = request.state.functional_zones_service

    functional_zone_dto = await functional_zones_service.patch_functional_zone(functional_zone_id, functional_zone)

    return FunctionalZone.from_dto(functional_zone_dto)


@functional_zones_router.delete(
    "/functional_zones/{functional_zone_id}",
    response_model=OkResponse,
    status_code=status.HTTP_200_OK,
)
async def delete_functional_zone(
    request: Request,
    functional_zone_id: int = Path(..., description="functional zone identifier", gt=0),
) -> OkResponse:
    """
    ## Delete a functional zone by its identifier.

    ### Parameters:
    - **functional_zone_id** (int, Path): Unique identifier of the functional zone.

    ### Returns:
    - **OkResponse**: A confirmation message of the deletion.

    ### Errors:
    - **404 Not Found**: If the functional zone does not exist.
    """
    functional_zones_service: FunctionalZonesService = request.state.functional_zones_service

    await functional_zones_service.delete_functional_zone(functional_zone_id)

    return OkResponse()
