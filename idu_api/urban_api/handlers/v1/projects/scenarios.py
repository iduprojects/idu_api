"""Scenarios endpoints are defined here."""

from fastapi import Depends, HTTPException, Path, Query, Request, Security
from fastapi.security import HTTPBearer
from starlette import status

from idu_api.urban_api.dto.users import UserDTO
from idu_api.urban_api.handlers.v1.projects.routers import projects_router
from idu_api.urban_api.logic.projects import UserProjectService
from idu_api.urban_api.schemas import (
    OkResponse,
    Scenario,
    ScenarioPatch,
    ScenarioPost,
    ScenarioPut,
)
from idu_api.urban_api.utils.auth_client import get_user


@projects_router.get(
    "/scenarios",
    response_model=list[Scenario],
    status_code=status.HTTP_200_OK,
)
async def get_scenarios(
    request: Request,
    parent_id: int | None = Query(
        None, description="REGIONAL scenario identifier (should be skipped to get regional scenarios)"
    ),
    project_id: int | None = Query(None, description="to filter by project"),
    territory_id: int | None = Query(None, description="to filter by territory (region)"),
    is_based: bool = Query(..., description="to get only base scenarios"),
    only_own: bool = Query(False, description="if True, return only user's own projects"),
    user: UserDTO | None = Depends(get_user),
) -> list[Scenario]:
    """
    ## Get a list of scenarios.

    ### Parameters:
    - **parent_id** (int, Query): Unique identifier of the REGIONAL scenario.
    **NOTE:** If passed, returns the child PROJECT scenarios with given parent only. Else, returns regional scenarios.
    - **project_id** (int, Query): Unique identifier of the project to get its scenarios only.
    - **territory_id** (int, Query): Unique identifier of the territory to get scenarios for projects located on given territory.
    **NOTE:** Only regions.
    - **is_based** (bool, Query): If true, returns only base scenarios. Else, returns all scenarios.
    - **only_own** (bool, Query): If true, returns only own scenarios + base regional scenarios if parent_id skipped. Else, returns all public scenarios.

    ### Returns:
    - **list[Scenario]**: List of all scenarios matching the given filters.

    ### Errors:
    - **401 Unauthorized**: If authentication is required to view user-specific projects.
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the scenario or project or territory does not exist.

    ### Constraints:
    - The user must be the project owner or the project must be publicly available (if project of scenario passed).
    """
    user_project_service: UserProjectService = request.state.user_project_service

    if parent_id is None and project_id is not None and is_based:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Incompatible parameters parent_id=None, project_id={project_id}, is_based=True. This will "
            f"always return an empty list, as the user project may not have any base regional scenario.",
        )

    if only_own and user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required to view own scenarios.",
        )

    scenarios = await user_project_service.get_scenarios(parent_id, project_id, territory_id, is_based, only_own, user)

    return [Scenario.from_dto(scenario) for scenario in scenarios]


@projects_router.get(
    "/scenarios/{scenario_id}",
    response_model=Scenario,
    status_code=status.HTTP_200_OK,
)
async def get_scenario_by_id(
    request: Request,
    scenario_id: int = Path(..., description="scenario identifier", gt=0),
    user: UserDTO = Depends(get_user),
) -> Scenario:
    """
    ## Get a scenario by its identifier.

    ### Parameters:
    - **scenario_id** (int, Path): Unique identifier of the scenario.

    ### Returns:
    - **Scenario**: The requested scenario.

    ### Errors:
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the scenario does not exist.

    ### Constraints:
    - The user must be the project owner or the project must be publicly available.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    scenario = await user_project_service.get_scenario_by_id(scenario_id, user)

    return Scenario.from_dto(scenario)


@projects_router.post(
    "/scenarios",
    response_model=Scenario,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Security(HTTPBearer())],
    deprecated=True,
)
async def add_scenario(request: Request, scenario: ScenarioPost, user: UserDTO = Depends(get_user)) -> Scenario:
    """
    ## Create a new scenario by copying base scenario for relevant project.

    **WARNING:** This method has been deprecated since version 0.34.0 and will be removed in version 1.0.
    Instead, use method **POST /scenarios/{scenario_id}** with `scenario_id` = base scenario identifier.

    ### Parameters:
    - **scenario** (ScenarioPost, Body): Data for the new scenario.

    ### Returns:
    - **Scenario**: The newly created scenario based on the base scenario.

    ### Errors:
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the related entity does not exist.

    ### Constraints:
    - The user must be the owner of the relevant project.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    scenario = await user_project_service.add_scenario(scenario, user)

    return Scenario.from_dto(scenario)


@projects_router.post(
    "/scenarios/{scenario_id}",
    response_model=Scenario,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Security(HTTPBearer())],
)
async def copy_scenario(
    request: Request,
    scenario: ScenarioPost,
    scenario_id: int = Path(..., description="another scenario identifier", gt=0),
    user: UserDTO = Depends(get_user),
) -> Scenario:
    """
    ## Create a new scenario by copying another existing scenario.

    ### Parameters:
    - **scenario_id** (int, Path): Identifier of the scenario to be copied.
    - **scenario** (ScenarioPost, Body): Data for the new scenario.

    ### Returns:
    - **Scenario**: The newly created scenario based on the copied one.

    ### Errors:
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the original scenario (or related entity) does not exist.

    ### Constraints:
    - The user must be the owner of the relevant project.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    scenario = await user_project_service.copy_scenario(scenario, scenario_id, user)

    return Scenario.from_dto(scenario)


@projects_router.put(
    "/scenarios/{scenario_id}",
    response_model=Scenario,
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
    deprecated=True,
)
async def put_scenario(
    request: Request,
    scenario: ScenarioPut,
    scenario_id: int = Path(..., description="scenario identifier", gt=0),
    user: UserDTO = Depends(get_user),
) -> Scenario:
    """
    ## Update all attributes of the given scenario.

    **NOTE:** If you want to make a base scenario from common, set `is_based = True` in the request body.
    Thus, the old base scenario will become common.

    **WARNING 2:** This method has been deprecated since version 0.34.0 and will be removed in version 1.0.
    Instead, use PATCH method.

    ### Parameters:
    - **scenario_id** (int, Path): Unique identifier of the scenario.
    - **scenario** (ScenarioPut, Body): The updated scenario data.

    ### Returns:
    - **Scenario**: The updated scenario.

    ### Errors:
    - **400 Bad Request**: If you try to make base scenario – non-based.
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the scenario (or related entity) does not exist.

    ### Constraints:
    - The user must be the owner of the relevant project.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    try:
        scenario = await user_project_service.put_scenario(scenario, scenario_id, user)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    return Scenario.from_dto(scenario)


@projects_router.patch(
    "/scenarios/{scenario_id}",
    response_model=Scenario,
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def patch_scenario(
    request: Request,
    scenario: ScenarioPatch,
    scenario_id: int = Path(..., description="scenario identifier", gt=0),
    user: UserDTO = Depends(get_user),
) -> Scenario:
    """
    ## Partially update a scenario.

    **NOTE:** If you want to make a base scenario from common, set `is_based = True` in the request body.
    Thus, the old base scenario will become common.

    ### Parameters:
    - **scenario_id** (int, Path): Unique identifier of the scenario.
    - **scenario** (ScenarioPatch, Body): Fields to update in the scenario.

    ### Returns:
    - **Scenario**: The updated scenario with modified fields.

    ### Errors:
    - **400 Bad Request**: If you try to make base scenario – non-based.
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the scenario (or related entity) does not exist.

    ### Constraints:
    - The user must be the owner of the relevant project.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    try:
        scenario = await user_project_service.patch_scenario(scenario, scenario_id, user)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    return Scenario.from_dto(scenario)


@projects_router.delete(
    "/scenarios/{scenario_id}",
    response_model=OkResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def delete_scenario(
    request: Request,
    scenario_id: int = Path(..., description="scenario identifier", gt=0),
    user: UserDTO = Depends(get_user),
) -> OkResponse:
    """
    ## Delete a scenario by its identifier.

    ### Parameters:
    - **scenario_id** (int, Path): Unique identifier of the scenario.

    ### Returns:
    - **OkResponse**: A confirmation message of the deletion.

    ### Errors:
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the scenario does not exist.

    ### Constraints:
    - The user must be the owner of the relevant project.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    await user_project_service.delete_scenario(scenario_id, user)

    return OkResponse()
