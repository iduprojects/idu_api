"""Scenarios endpoints are defined here."""

from fastapi import Depends, Path, Request, Security
from fastapi.security import HTTPBearer
from starlette import status

from idu_api.urban_api.dto.users import UserDTO
from idu_api.urban_api.handlers.v1.projects.routers import projects_router
from idu_api.urban_api.logic.projects import UserProjectService
from idu_api.urban_api.schemas import (
    ScenariosData,
    ScenariosPatch,
    ScenariosPost,
    ScenariosPut,
)
from idu_api.urban_api.utils.auth_client import get_user


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
    """Get scenario by identifier.

    You must be the owner of the relevant project or the project must be publicly available.
    """
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
    """Create a new scenario from the base scenario for given project.

    You must be the owner of the relevant project.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    scenario = await user_project_service.add_scenario(scenario, user.id)

    return ScenariosData.from_dto(scenario)


@projects_router.post(
    "/scenarios/{scenario_id}",
    response_model=ScenariosData,
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def copy_scenario(
    request: Request,
    scenario: ScenariosPost,
    scenario_id: int = Path(..., description="another scenario identifier"),
    user: UserDTO = Depends(get_user),
) -> ScenariosData:
    """Create a new scenario from another scenario (copy) by its identifier for given project.

    You must be the owner of the relevant project.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    scenario = await user_project_service.copy_scenario(scenario, scenario_id, user.id)

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
    """Update a scenario object - all attributes.

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
    """Update a scenario - only given fields.

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
