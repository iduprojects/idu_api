"""Scenarios endpoints are defined here."""

from fastapi import Body, Depends, Path, Query, Request, Security
from fastapi.security import HTTPBearer
from starlette import status

from idu_api.urban_api.dto.users import UserDTO
from idu_api.urban_api.handlers.v1.projects.routers import projects_router
from idu_api.urban_api.logic.projects import UserProjectService
from idu_api.urban_api.schemas import (
    PhysicalObjectsDataPost,
    PhysicalObjectWithGeometryPost,
    ScenariosData,
    ScenariosPatch,
    ScenariosPost,
    ScenariosPut,
    ScenariosUrbanObject,
    ServicesDataPost,
)
from idu_api.urban_api.utils.auth_client import get_user


@projects_router.get(
    "/scenarios_by_project",
    response_model=list[ScenariosData],
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def get_scenario_by_project_id(
    request: Request,
    project_id: int = Query(..., description="project identifier"),
    user: UserDTO = Depends(get_user),
) -> list[ScenariosData]:
    """Get list of scenarios for given project if project is public or if you're the project owner."""
    user_project_service: UserProjectService = request.state.user_project_service

    scenarios = await user_project_service.get_scenarios_by_project_id(project_id, user.id)

    return [ScenariosData.from_dto(scenario) for scenario in scenarios]


@projects_router.get(
    "/scenarios/{scenario_id}",
    response_model=ScenariosData,
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def get_scenario_by_id(
    request: Request,
    scenario_id: int = Path(..., description="scenario identifier"),
    user: UserDTO = Depends(get_user),
) -> ScenariosData:
    """Get scenario by identifier if project is public or if you're the project owner."""
    user_project_service: UserProjectService = request.state.user_project_service

    scenario = await user_project_service.get_scenario_by_id(scenario_id, user.id)

    return ScenariosData.from_dto(scenario)


@projects_router.post(
    "/scenarios",
    response_model=ScenariosData,
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def add_scenario(request: Request, scenario: ScenariosPost, user: UserDTO = Depends(get_user)) -> ScenariosData:
    """Create a new scenario for given project.

    You must be the owner of the relevant project.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    scenario = await user_project_service.add_scenario(scenario, user.id)

    return ScenariosData.from_dto(scenario)


@projects_router.put(
    "/scenarios/{scenario_id}",
    response_model=ScenariosData,
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def put_scenario(
    request: Request,
    scenario: ScenariosPut,
    scenario_id: int = Path(..., description="scenario identifier"),
    user: UserDTO = Depends(get_user),
) -> ScenariosData:
    """Update a scenario by setting all of its attributes.

    You must be the owner of the relevant project.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    scenario = await user_project_service.put_scenario(scenario, scenario_id, user.id)

    return ScenariosData.from_dto(scenario)


@projects_router.patch(
    "/scenarios/{scenario_id}",
    response_model=ScenariosData,
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def patch_scenario(
    request: Request,
    scenario: ScenariosPatch,
    scenario_id: int = Path(..., description="scenario identifier"),
    user: UserDTO = Depends(get_user),
) -> ScenariosData:
    """Update a scenario by setting given attributes.

    You must be the owner of the relevant project.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    scenario = await user_project_service.patch_scenario(scenario, scenario_id, user.id)

    return ScenariosData.from_dto(scenario)


@projects_router.delete(
    "/scenarios/{scenario_id}",
    response_model=dict,
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def delete_scenario(
    request: Request,
    scenario_id: int = Path(..., description="scenario identifier"),
    user: UserDTO = Depends(get_user),
) -> dict:
    """Delete scenario by given identifier.

    You must be the owner of the relevant project.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    return await user_project_service.delete_scenario(scenario_id, user.id)


@projects_router.post(
    "/scenarios/{scenario_id}/physical_objects",
    response_model=ScenariosUrbanObject,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Security(HTTPBearer())],
)
async def create_scenario_physical_object(
    request: Request,
    physical_object: PhysicalObjectWithGeometryPost,
    scenario_id: int = Path(..., description="scenario identifier"),
    user: UserDTO = Depends(get_user),
) -> ScenariosUrbanObject:
    """Add physical object to scenario.

    You must be the owner of the relevant project.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    urban_object = await user_project_service.add_physical_object_to_scenario(scenario_id, physical_object, user.id)

    return ScenariosUrbanObject.from_dto(urban_object)


@projects_router.post(
    "/scenarios/{scenario_id}/physical_objects/{object_geometry_id}",
    response_model=ScenariosUrbanObject,
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def add_physical_object_to_scenario(
    request: Request,
    scenario_id: int = Path(..., description="scenario identifier"),
    object_geometry_id: int = Path(..., description="Object geometry id"),
    physical_object: PhysicalObjectsDataPost = Body(..., description="Physical object"),
    user: UserDTO = Depends(get_user),
) -> ScenariosUrbanObject:
    """Add existing physical object to scenario.

    You must be the owner of the relevant project.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    urban_object = await user_project_service.add_existing_physical_object_to_scenario(
        scenario_id, object_geometry_id, physical_object, user.id
    )

    return ScenariosUrbanObject.from_dto(urban_object)


@projects_router.post(
    "/scenarios/{scenario_id}/services",
    response_model=ScenariosUrbanObject,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Security(HTTPBearer())],
)
async def create_scenario_service(
    request: Request,
    service: ServicesDataPost,
    scenario_id: int = Path(..., description="scenario identifier"),
    user: UserDTO = Depends(get_user),
) -> ScenariosUrbanObject:
    """Add service object to scenario.

    You must be the owner of the relevant project.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    urban_object = await user_project_service.add_service_to_scenario(scenario_id, service, user.id)

    return ScenariosUrbanObject.from_dto(urban_object)


@projects_router.post(
    "/scenarios/{scenario_id}/services/{service_id}",
    response_model=ScenariosUrbanObject,
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def add_service_to_scenario(
    request: Request,
    scenario_id: int = Path(..., description="scenario identifier"),
    service_id: int = Path(..., description="Service id"),
    physical_object_id: int = Query(..., description="Physical object id"),
    object_geometry_id: int = Query(..., description="Object geometry id"),
    user: UserDTO = Depends(get_user),
) -> ScenariosUrbanObject:
    """Add existing service object to scenario.

    You must be the owner of the relevant project.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    urban_object = await user_project_service.add_existing_service_to_scenario(
        scenario_id, service_id, physical_object_id, object_geometry_id, user.id
    )

    return ScenariosUrbanObject.from_dto(urban_object)
