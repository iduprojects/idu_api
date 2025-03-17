"""Services projects-related endpoints are defined here."""

from fastapi import Depends, HTTPException, Path, Query, Request, Security
from fastapi.security import HTTPBearer
from starlette import status

from idu_api.urban_api.dto.users import UserDTO
from idu_api.urban_api.handlers.v1.projects.routers import projects_router
from idu_api.urban_api.logic.projects import UserProjectService
from idu_api.urban_api.schemas import (
    OkResponse,
    ScenarioService,
    ScenarioServicePost,
    ScenarioUrbanObject,
    Service,
    ServicePatch,
    ServicePut,
)
from idu_api.urban_api.utils.auth_client import get_user


@projects_router.get(
    "/scenarios/{scenario_id}/services",
    response_model=list[ScenarioService],
    status_code=status.HTTP_200_OK,
)
async def get_services_by_scenario_id(
    request: Request,
    scenario_id: int = Path(..., description="scenario identifier", gt=0),
    service_type_id: int | None = Query(None, description="to filter by service type", gt=0),
    urban_function_id: int | None = Query(None, description="to filter by urban function", gt=0),
    user: UserDTO = Depends(get_user),
) -> list[ScenarioService]:
    """
    ## Get a list of services for a given scenario.

    **WARNING:** You can only filter by service type or urban function.

    ### Parameters:
    - **scenario_id** (int, Path): Unique identifier of the scenario.
    - **service_type_id** (int | None, Query): Optional filter by service type identifier.
    - **urban_function_id** (int | None, Query): Optional filter by urban function identifier.

    ### Returns:
    - **list[ScenarioService]**: A list of services.

    ### Errors:
    - **400 Bad Request**: If you set both `service_type_id` and `urban_function_id`.
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the scenario does not exist.

    ### Constraints:
    - The user must be the owner of the relevant project or the project must be publicly available.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    if service_type_id is not None and urban_function_id is not None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Please, choose either service_type_id or urban_function_id",
        )

    services = await user_project_service.get_services_by_scenario_id(
        scenario_id,
        user,
        service_type_id,
        urban_function_id,
    )

    return [ScenarioService.from_dto(service) for service in services]


@projects_router.get(
    "/projects/{project_id}/context/services",
    response_model=list[Service],
    status_code=status.HTTP_200_OK,
)
async def get_context_services(
    request: Request,
    project_id: int = Path(..., description="project identifier", gt=0),
    service_type_id: int | None = Query(None, description="to filter by service type", gt=0),
    urban_function_id: int | None = Query(None, description="to filter by urban function", gt=0),
    user: UserDTO = Depends(get_user),
) -> list[Service]:
    """
    ## Get a list of services for the context of a project territory.

    **WARNING:** You can only filter by service type or urban function.

    ### Parameters:
    - **project_id** (int, Path): Unique identifier of the project.
    - **service_type_id** (int | None, Query): Optional filter by service type identifier.
    - **urban_function_id** (int | None, Query): Optional filter by urban function identifier.

    ### Returns:
    - **list[Service]**: A list of services.

    ### Errors:
    - **400 Bad Request**: If you set both `service_type_id` and `urban_function_id`.
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the project does not exist.

    ### Constraints:
    - The user must be the owner of the relevant project or the project must be publicly available.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    if service_type_id is not None and urban_function_id is not None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Please, choose either service_type_id or urban_function_id",
        )

    services = await user_project_service.get_context_services(project_id, user, service_type_id, urban_function_id)

    return [Service.from_dto(service) for service in services]


@projects_router.post(
    "/scenarios/{scenario_id}/services",
    response_model=ScenarioUrbanObject,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Security(HTTPBearer())],
)
async def add_service(
    request: Request,
    service: ScenarioServicePost,
    scenario_id: int = Path(..., description="scenario identifier", gt=0),
    user: UserDTO = Depends(get_user),
) -> ScenarioUrbanObject:
    """
    ## Create a new service with geometry for a given scenario and a pair of physical object and object geometry.

    ### Parameters:
    - **scenario_id** (int, Path): Unique identifier of the scenario.
    - **service** (ScenarioServicePost, Body): The service data including geometry.

    ### Returns:
    - **ScenarioUrbanObject**: The created urban object (physical object + geometry + service).

    ### Errors:
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the scenario (or related entities) does not exist.

    ### Constraints:
    - The user must be the owner of the relevant project.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    urban_object = await user_project_service.add_service(service, scenario_id, user)

    return ScenarioUrbanObject.from_dto(urban_object)


@projects_router.put(
    "/scenarios/{scenario_id}/services/{service_id}",
    response_model=ScenarioService,
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def put_service(
    request: Request,
    service: ServicePut,
    scenario_id: int = Path(..., description="scenario identifier", gt=0),
    service_id: int = Path(..., description="service identifier", gt=0),
    is_scenario_object: bool = Query(..., description="to determine scenario object"),
    user: UserDTO = Depends(get_user),
) -> ScenarioService:
    """
    ## Update all attributes of a service for a given scenario.

    ### Parameters:
    - **scenario_id** (int, Path): Unique identifier of the scenario.
    - **service_id** (int, Path): Unique identifier of the service.
    - **is_scenario_object** (bool, Query): Flag to determine if the object is a scenario object.
    - **service** (ServicePut, Body): The updated service data.

    ### Returns:
    - **ScenarioService**: The updated service.

    ### Errors:
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the scenario or service (or related entity) does not exist.
    - **409 Conflict**: If you try to update non-scenario service that has been already updated
    (then it is scenario object).

    ### Constraints:
    - The user must be the owner of the relevant project.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    service_dto = await user_project_service.put_service(service, scenario_id, service_id, is_scenario_object, user)

    return ScenarioService.from_dto(service_dto)


@projects_router.patch(
    "/scenarios/{scenario_id}/services/{service_id}",
    response_model=ScenarioService,
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def patch_service(
    request: Request,
    service: ServicePatch,
    scenario_id: int = Path(..., description="scenario identifier", gt=0),
    service_id: int = Path(..., description="service identifier", gt=0),
    is_scenario_object: bool = Query(..., description="to determine scenario object"),
    user: UserDTO = Depends(get_user),
) -> ScenarioService:
    """
    ## Update specific fields of a service for a given scenario.

    ### Parameters:
    - **scenario_id** (int, Path): Unique identifier of the scenario.
    - **service_id** (int, Path): Unique identifier of the service.
    - **is_scenario_object** (bool, Query): Flag to determine if the object is a scenario object.
    - **service** (ServicePatch, Body): The partial service data to update.

    ### Returns:
    - **ScenarioService**: The updated service.

    ### Errors:
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the scenario or service does not exist.
    - **409 Conflict**: If you try to update non-scenario service that has been already updated
    (then it is scenario object).

    ### Constraints:
    - The user must be the owner of the relevant project.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    service_dto = await user_project_service.patch_service(
        service,
        scenario_id,
        service_id,
        is_scenario_object,
        user,
    )

    return ScenarioService.from_dto(service_dto)


@projects_router.delete(
    "/scenarios/{scenario_id}/services/{service_id}",
    response_model=OkResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def delete_service(
    request: Request,
    scenario_id: int = Path(..., description="scenario identifier", gt=0),
    service_id: int = Path(..., description="service identifier", gt=0),
    is_scenario_object: bool = Query(..., description="to determine scenario object"),
    user: UserDTO = Depends(get_user),
) -> OkResponse:
    """
    ## Delete a service by its identifier for a given scenario.

    ### Parameters:
    - **scenario_id** (int, Path): Unique identifier of the scenario.
    - **service_id** (int, Path): Unique identifier of the service.
    - **is_scenario_object** (bool, Query): Flag to determine if the object is a scenario object.

    ### Returns:
    - **OkResponse**: A confirmation message of the deletion.

    ### Errors:
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the scenario or service does not exist.

    ### Constraints:
    - The user must be the owner of the relevant project.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    await user_project_service.delete_service(scenario_id, service_id, is_scenario_object, user)

    return OkResponse()
