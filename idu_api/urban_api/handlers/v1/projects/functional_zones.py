"""Functional zones projects-related endpoints are defined here."""

from fastapi import Depends, Path, Query, Request, Security
from fastapi.security import HTTPBearer
from geojson_pydantic import Feature
from geojson_pydantic.geometries import Geometry
from starlette import status

from idu_api.urban_api.dto.users import UserDTO
from idu_api.urban_api.handlers.v1.projects.routers import projects_router
from idu_api.urban_api.logic.projects import UserProjectService
from idu_api.urban_api.schemas import (
    FunctionalZoneSource,
    FunctionalZoneWithoutGeometry,
    OkResponse,
    ScenarioFunctionalZone,
    ScenarioFunctionalZonePatch,
    ScenarioFunctionalZonePost,
    ScenarioFunctionalZonePut,
    ScenarioFunctionalZoneWithoutGeometry,
)
from idu_api.urban_api.schemas.geometries import GeoJSONResponse
from idu_api.urban_api.utils.auth_client import get_user


@projects_router.get(
    "/scenarios/{scenario_id}/functional_zone_sources",
    response_model=list[FunctionalZoneSource],
    status_code=status.HTTP_200_OK,
)
async def get_functional_zone_sources_by_scenario_id(
    request: Request,
    scenario_id: int = Path(..., description="scenario identifier", gt=0),
    user: UserDTO = Depends(get_user),
) -> list[FunctionalZoneSource]:
    """
    ## Get a list of functional zone sources for a given scenario.

    ### Parameters:
    - **scenario_id** (int, Path): Unique identifier of the scenario.

    ### Returns:
    - **list[FunctionalZoneSource]**: A list of functional zone sources, each represented as a (year, source) pair.

    ### Errors:
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the scenario does not exist.

    ### Constraints:
    - The user must be the relevant project owner or the project must be publicly accessible.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    sources = await user_project_service.get_functional_zones_sources_by_scenario_id(scenario_id, user)

    return [FunctionalZoneSource.from_dto(source) for source in sources]


@projects_router.get(
    "/scenarios/{scenario_id}/functional_zones",
    response_model=GeoJSONResponse[Feature[Geometry, ScenarioFunctionalZoneWithoutGeometry]],
    status_code=status.HTTP_200_OK,
)
async def get_functional_zones_by_scenario_id(
    request: Request,
    scenario_id: int = Path(..., description="scenario identifier", gt=0),
    year: int = Query(..., description="to filter by year when zones were uploaded"),
    source: str = Query(..., description="to filter by source from which zones were uploaded"),
    functional_zone_type_id: int | None = Query(None, description="functional zone type identifier", gt=0),
    user: UserDTO = Depends(get_user),
) -> GeoJSONResponse[Feature[Geometry, ScenarioFunctionalZoneWithoutGeometry]]:
    """
    ## Get functional zones in GeoJSON format for a given scenario, filtered by year and source.

    ### Parameters:
    - **scenario_id** (int, Path): Unique identifier of the scenario.
    - **year** (int, Query): Year of zone upload.
    - **source** (str, Query): Source from which zones were uploaded.
    - **functional_zone_type_id** (int | None, Query): Optional functional zone type filter.

    ### Returns:
    - **GeoJSONResponse[Feature[Geometry, ScenarioFunctionalZoneWithoutGeometry]]**: Functional zones in GeoJSON format.

    ### Errors:
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the scenario does not exist.

    ### Constraints:
    - The user must be the relevant project owner or the project must be publicly accessible.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    functional_zones = await user_project_service.get_functional_zones_by_scenario_id(
        scenario_id, year, source, functional_zone_type_id, user
    )

    return await GeoJSONResponse.from_list([zone.to_geojson_dict() for zone in functional_zones])


@projects_router.get(
    "/scenarios/{scenario_id}/context/functional_zone_sources",
    response_model=list[FunctionalZoneSource],
    status_code=status.HTTP_200_OK,
)
async def get_context_functional_zone_sources(
    request: Request,
    scenario_id: int = Path(..., description="scenario identifier", gt=0),
    user: UserDTO = Depends(get_user),
) -> list[FunctionalZoneSource]:
    """
    ## Get functional zone sources for the project's context.

    ### Parameters:
    - **scenario_id** (int, Path): Unique identifier of the scenario.

    ### Returns:
    - **list[FunctionalZoneSource]**: A list of functional zone sources, each represented as a (year, source) pair.

    ### Errors:
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the scenario does not exist.

    ### Constraints:
    - The user must be the relevant project owner or the project must be publicly accessible.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    sources = await user_project_service.get_context_functional_zones_sources(scenario_id, user)

    return [FunctionalZoneSource.from_dto(source) for source in sources]


@projects_router.get(
    "/scenarios/{scenario_id}/context/functional_zones",
    response_model=GeoJSONResponse[Feature[Geometry, FunctionalZoneWithoutGeometry]],
    status_code=status.HTTP_200_OK,
)
async def get_context_functional_zones(
    request: Request,
    scenario_id: int = Path(..., description="scenario identifier", gt=0),
    year: int = Query(..., description="to filter by year when zones were uploaded"),
    source: str = Query(..., description="to filter by source from which zones were uploaded"),
    functional_zone_type_id: int | None = Query(None, description="functional zone type identifier", gt=0),
    user: UserDTO = Depends(get_user),
) -> GeoJSONResponse[Feature[Geometry, FunctionalZoneWithoutGeometry]]:
    """
    ## Get functional zones in GeoJSON format for the project's context, filtered by year and source.

    ### Parameters:
    - **scenario_id** (int, Path): Unique identifier of the scenario.
    - **year** (int, Query): Year of zone upload.
    - **source** (str, Query): Source from which zones were uploaded.
    - **functional_zone_type_id** (int | None, Query): Optional functional zone type filter.

    ### Returns:
    - **GeoJSONResponse[Feature[Geometry, FunctionalZoneWithoutGeometry]]**: Functional zones in GeoJSON format.

    ### Errors:
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the scenario does not exist.

    ### Constraints:
    - The user must be the relevant project owner or the project must be publicly accessible.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    functional_zones = await user_project_service.get_context_functional_zones(
        scenario_id, year, source, functional_zone_type_id, user
    )

    return await GeoJSONResponse.from_list([zone.to_geojson_dict() for zone in functional_zones])


@projects_router.post(
    "/scenarios/{scenario_id}/functional_zones",
    response_model=list[ScenarioFunctionalZone],
    status_code=status.HTTP_201_CREATED,
    dependencies=[Security(HTTPBearer())],
)
async def add_scenario_functional_zones(
    request: Request,
    functional_zones: list[ScenarioFunctionalZonePost],
    scenario_id: int = Path(..., description="scenario identifier", gt=0),
    user: UserDTO = Depends(get_user),
) -> list[ScenarioFunctionalZone]:
    """
    ## Create new functional zones for a given scenario.

    **WARNING:** This method will delete **ONLY USER'S** functional zones (since version 0.49.0)
    for the specified scenario before adding new ones.

    ### Parameters:
    - **scenario_id** (int, Path): Unique identifier of the scenario.
    - **functional_zones** (list[ScenarioFunctionalZonePost], Body): List of functional zones to be added.

    ### Returns:
    - **list[ScenarioFunctionalZone]**: A list of created functional zones.

    ### Errors:
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the scenario (or related entity) does not exist.

    ### Constraints:
    - The user must be the relevant project owner.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    functional_zones = await user_project_service.add_scenario_functional_zones(functional_zones, scenario_id, user)

    return [ScenarioFunctionalZone.from_dto(zone) for zone in functional_zones]


@projects_router.put(
    "/scenarios/{scenario_id}/functional_zones/{functional_zone_id}",
    response_model=ScenarioFunctionalZone,
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def put_scenario_functional_zone(
    request: Request,
    functional_zone: ScenarioFunctionalZonePut,
    scenario_id: int = Path(..., description="scenario identifier", gt=0),
    functional_zone_id: int = Path(..., description="functional zone identifier", gt=0),
    user: UserDTO = Depends(get_user),
) -> ScenarioFunctionalZone:
    """
    ## Update a functional zone by replacing all its attributes.

    ### Parameters:
    - **scenario_id** (int, Path): Unique identifier of the scenario.
    - **functional_zone_id** (int, Path): Unique identifier of the functional zone.
    - **functional_zone** (ScenarioFunctionalZonePut, Body): New attributes for the functional zone.

    ### Returns:
    - **ScenarioFunctionalZone**: The updated functional zone.

    ### Errors:
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the scenario or functional zone (or related entity) does not exist.

    ### Constraints:
    - The user must be the relevant project owner.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    functional_zone = await user_project_service.put_scenario_functional_zone(
        functional_zone, scenario_id, functional_zone_id, user
    )

    return ScenarioFunctionalZone.from_dto(functional_zone)


@projects_router.patch(
    "/scenarios/{scenario_id}/functional_zones/{functional_zone_id}",
    response_model=ScenarioFunctionalZone,
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def patch_scenario_functional_zone(
    request: Request,
    functional_zone: ScenarioFunctionalZonePatch,
    scenario_id: int = Path(..., description="scenario identifier", gt=0),
    functional_zone_id: int = Path(..., description="functional zone identifier", gt=0),
    user: UserDTO = Depends(get_user),
) -> ScenarioFunctionalZone:
    """
    ## Partially update a functional zone by modifying only specified attributes.

    ### Parameters:
    - **scenario_id** (int, Path): Unique identifier of the scenario.
    - **functional_zone_id** (int, Path): Unique identifier of the functional zone.
    - **functional_zone** (ScenarioFunctionalZonePatch, Body): Attributes to be updated.

    ### Returns:
    - **ScenarioFunctionalZone**: The updated functional zone.

    ### Errors:
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the scenario or functional zone (or related entity) does not exist.

    ### Constraints:
    - The user must be the relevant project owner.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    functional_zone = await user_project_service.patch_scenario_functional_zone(
        functional_zone, scenario_id, functional_zone_id, user
    )

    return ScenarioFunctionalZone.from_dto(functional_zone)


@projects_router.delete(
    "/scenarios/{scenario_id}/functional_zones",
    response_model=OkResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def delete_functional_zones_by_scenario_id(
    request: Request,
    scenario_id: int = Path(..., description="scenario identifier", gt=0),
    user: UserDTO = Depends(get_user),
) -> OkResponse:
    """
    ## Delete all functional zones associated with a given scenario.

    ### Parameters:
    - **scenario_id** (int, Path): Unique identifier of the scenario.

    ### Returns:
    - **OkResponse**: A confirmation message of the deletion.

    ### Errors:
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the scenario does not exist.

    ### Constraints:
    - The user must be the relevant project owner.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    await user_project_service.delete_functional_zones_by_scenario_id(scenario_id, user)

    return OkResponse()
