"""
Scenarios endpoints are defined here.
"""

from fastapi import Depends, Path, Query, Request
from starlette import status

from idu_api.urban_api.dto.users import UserDTO
from idu_api.urban_api.handlers.v1.projects.routers import projects_router
from idu_api.urban_api.logic.projects import UserProjectService
from idu_api.urban_api.schemas import ScenariosData, ScenariosPatch, ScenariosPost, ScenariosPut
from idu_api.urban_api.utils.dependencies import user_dependency


@projects_router.get(
    "/scenarios_by_project",
    response_model=list[ScenariosData],
    status_code=status.HTTP_200_OK,
)
async def get_scenario_by_project_id(
    request: Request,
    project_id: int = Query(..., description="project identifier"),
    user: UserDTO = Depends(user_dependency),
) -> list[ScenariosData]:
    """Get list of scenarios for given project if project is public or if you're the project owner."""
    user_project_service: UserProjectService = request.state.user_project_service

    scenarios = await user_project_service.get_scenarios_by_project_id(project_id, user.id)

    return [ScenariosData.from_dto(scenario) for scenario in scenarios]


@projects_router.get(
    "/scenarios/{scenario_id}",
    response_model=ScenariosData,
    status_code=status.HTTP_200_OK,
)
async def get_scenario_by_id(
    request: Request,
    scenario_id: int = Path(..., description="scenario identifier"),
    user: UserDTO = Depends(user_dependency),
) -> ScenariosData:
    """Get scenario by identifier if project is public or if you're the project owner."""
    user_project_service: UserProjectService = request.state.user_project_service

    scenario = await user_project_service.get_scenario_by_id(scenario_id, user.id)

    return ScenariosData.from_dto(scenario)


@projects_router.post(
    "/scenarios",
    response_model=ScenariosData,
    status_code=status.HTTP_200_OK,
)
async def add_scenario(
    request: Request, scenario: ScenariosPost, user: UserDTO = Depends(user_dependency)
) -> ScenariosData:
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
)
async def put_scenario(
    request: Request,
    scenario: ScenariosPut,
    scenario_id: int = Path(..., description="scenario identifier"),
    user: UserDTO = Depends(user_dependency),
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
)
async def patch_scenario(
    request: Request,
    scenario: ScenariosPatch,
    scenario_id: int = Path(..., description="scenario identifier"),
    user: UserDTO = Depends(user_dependency),
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
)
async def delete_scenario(
    request: Request,
    scenario_id: int = Path(..., description="scenario identifier"),
    user: UserDTO = Depends(user_dependency),
) -> dict:
    """Delete scenario by given identifier.

    You must be the owner of the relevant project.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    return await user_project_service.delete_scenario(scenario_id, user.id)
