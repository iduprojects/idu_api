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
    ProjectProfile,
    ProjectProfilePatch,
    ProjectProfilePost,
    ProjectProfilePut,
    ProjectProfileWithoutGeometry,
)
from idu_api.urban_api.schemas.geometries import GeoJSONResponse
from idu_api.urban_api.utils.auth_client import get_user


@projects_router.get(
    "/scenarios/{scenario_id}/functional_zone_sources",
    response_model=list[FunctionalZoneSource],
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def get_functional_zone_sources_by_scenario_id(
    request: Request,
    scenario_id: int = Path(..., description="scenario identifier"),
    user: UserDTO = Depends(get_user),
) -> list[FunctionalZoneSource]:
    """Get list of pairs (year, source) of functional zones by scenario identifier.

    You must be the owner of the relevant project or the project must be publicly available.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    sources = await user_project_service.get_functional_zones_sources_by_scenario_id(scenario_id, user.id)

    return [FunctionalZoneSource.from_dto(source) for source in sources]


@projects_router.get(
    "/scenarios/{scenario_id}/functional_zones",
    response_model=GeoJSONResponse[Feature[Geometry, ProjectProfileWithoutGeometry]],
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def get_functional_zones_by_scenario_id(
    request: Request,
    scenario_id: int = Path(..., description="scenario identifier"),
    year: int = Query(..., description="to filter by year when zones were uploaded"),
    source: str = Query(..., description="to filter by source from which zones were uploaded"),
    functional_zone_type_id: int | None = Query(None, description="functional zone type identifier"),
    user: UserDTO = Depends(get_user),
) -> GeoJSONResponse[Feature[Geometry, ProjectProfileWithoutGeometry]]:
    """Get functional zones by scenario identifier, year and source in geojson format.

    You must be the owner of the relevant project or the project must be publicly available.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    functional_zones = await user_project_service.get_functional_zones_by_scenario_id(
        scenario_id, year, source, functional_zone_type_id, user.id
    )

    return await GeoJSONResponse.from_list([zone.to_geojson_dict() for zone in functional_zones])


@projects_router.get(
    "/scenarios/{scenario_id}/context/functional_zone_sources",
    response_model=list[FunctionalZoneSource],
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def get_context_functional_zone_sources_by_scenario_id(
    request: Request,
    scenario_id: int = Path(..., description="scenario identifier"),
    user: UserDTO = Depends(get_user),
) -> list[FunctionalZoneSource]:
    """Get list of pairs (year, source) of functional zones
    by scenario identifier for 'context' of the project territory.

    You must be the owner of the relevant project or the project must be publicly available.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    sources = await user_project_service.get_context_functional_zones_sources_by_scenario_id(scenario_id, user.id)

    return [FunctionalZoneSource.from_dto(source) for source in sources]


@projects_router.get(
    "/scenarios/{scenario_id}/context/functional_zones",
    response_model=GeoJSONResponse[Feature[Geometry, FunctionalZoneWithoutGeometry]],
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def get_context_functional_zones_by_scenario_id(
    request: Request,
    scenario_id: int = Path(..., description="scenario identifier"),
    year: int = Query(..., description="to filter by year when zones were uploaded"),
    source: str = Query(..., description="to filter by source from which zones were uploaded"),
    functional_zone_type_id: int | None = Query(None, description="functional zone type identifier"),
    user: UserDTO = Depends(get_user),
) -> GeoJSONResponse[Feature[Geometry, FunctionalZoneWithoutGeometry]]:
    """Get functional zones by scenario identifier, year and source
    for 'context' of the project territory in geojson format.

    You must be the owner of the relevant project or the project must be publicly available.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    functional_zones = await user_project_service.get_context_functional_zones_by_scenario_id(
        scenario_id, year, source, functional_zone_type_id, user.id
    )

    return await GeoJSONResponse.from_list([zone.to_geojson_dict() for zone in functional_zones])


@projects_router.post(
    "/scenarios/{scenario_id}/functional_zones",
    response_model=list[ProjectProfile],
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def add_scenario_functional_zones(
    request: Request,
    functional_zones: list[ProjectProfilePost],
    scenario_id: int = Path(..., description="scenario identifier"),
    user: UserDTO = Depends(get_user),
) -> list[ProjectProfile]:
    """Create new functional zones for given scenario and list of functional zones.

    You must be the owner of the relevant project.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    functional_zones = await user_project_service.add_scenario_functional_zones(functional_zones, scenario_id, user.id)

    return [ProjectProfile.from_dto(zone) for zone in functional_zones]


@projects_router.put(
    "/scenarios/{scenario_id}/functional_zones/{functional_zone_id}",
    response_model=ProjectProfile,
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def put_scenario_functional_zone(
    request: Request,
    functional_zone: ProjectProfilePut,
    scenario_id: int = Path(..., description="scenario identifier"),
    functional_zone_id: int = Path(..., description="functional zone identifier"),
    user: UserDTO = Depends(get_user),
) -> ProjectProfile:
    """Update functional zone by all its attributes.

    You must be the owner of the relevant project.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    functional_zone = await user_project_service.put_scenario_functional_zone(
        functional_zone, scenario_id, functional_zone_id, user.id
    )

    return ProjectProfile.from_dto(functional_zone)


@projects_router.patch(
    "/scenarios/{scenario_id}/functional_zones/{functional_zone_id}",
    response_model=ProjectProfile,
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def patch_scenario_functional_zone(
    request: Request,
    functional_zone: ProjectProfilePatch,
    scenario_id: int = Path(..., description="scenario identifier"),
    functional_zone_id: int = Path(..., description="functional zone identifier"),
    user: UserDTO = Depends(get_user),
) -> ProjectProfile:
    """Update functional zone by only given attributes.

    You must be the owner of the relevant project.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    functional_zone = await user_project_service.patch_scenario_functional_zone(
        functional_zone, scenario_id, functional_zone_id, user.id
    )

    return ProjectProfile.from_dto(functional_zone)


@projects_router.delete(
    "/scenarios/{scenario_id}/functional_zones",
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def delete_functional_zones_by_scenario_id(
    request: Request,
    scenario_id: int = Path(..., description="scenario identifier"),
    user: UserDTO = Depends(get_user),
) -> dict:
    """Delete all functional zones for given scenario

    You must be the owner of the relevant project.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    return await user_project_service.delete_functional_zones_by_scenario_id(scenario_id, user.id)
