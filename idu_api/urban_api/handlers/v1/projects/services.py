"""Services projects-related endpoints are defined here."""

from fastapi import Depends, Path, Query, Request, Security
from fastapi.security import HTTPBearer
from starlette import status

from idu_api.urban_api.dto.users import UserDTO
from idu_api.urban_api.handlers.v1.projects.routers import projects_router
from idu_api.urban_api.logic.projects import UserProjectService
from idu_api.urban_api.schemas import (
    ScenarioService,
    ScenarioServicePost,
    ServicesData,
    ServicesDataPatch,
    ServicesDataPut,
)
from idu_api.urban_api.schemas.urban_objects import ScenarioUrbanObject
from idu_api.urban_api.utils.auth_client import get_user


@projects_router.get(
    "/scenarios/{scenario_id}/services",
    response_model=list[ScenarioService],
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def get_services_by_scenario_id(
    request: Request,
    scenario_id: int = Path(..., description="scenario identifier"),
    service_type_id: int | None = Query(None, description="to filter by service type"),
    urban_function_id: int | None = Query(None, description="to filter by urban function"),
    user: UserDTO = Depends(get_user),
) -> list[ScenarioService]:
    """Get list of services for given scenario.

    It could be specified by service type and urban function.

    You must be the owner of the relevant project or the project must be publicly available.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    services = await user_project_service.get_services_by_scenario_id(
        scenario_id,
        user.id,
        service_type_id,
        urban_function_id,
    )

    return [ScenarioService.from_dto(service) for service in services]


@projects_router.get(
    "/scenarios/{scenario_id}/context/services",
    response_model=list[ServicesData],
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def get_context_services_by_scenario_id(
    request: Request,
    scenario_id: int = Path(..., description="scenario identifier"),
    service_type_id: int | None = Query(None, description="to filter by service type"),
    urban_function_id: int | None = Query(None, description="to filter by urban function"),
    user: UserDTO = Depends(get_user),
) -> list[ServicesData]:
    """Get list of services for context of the project territory for given scenario.

    It could be specified by service type and urban function.

    You must be the owner of the relevant project or the project must be publicly available.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    services = await user_project_service.get_context_services_by_scenario_id(
        scenario_id,
        user.id,
        service_type_id,
        urban_function_id,
    )

    return [ServicesData.from_dto(service) for service in services]


@projects_router.post(
    "/scenarios/{scenario_id}/services",
    response_model=ScenarioUrbanObject,
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def add_service(
    request: Request,
    service: ScenarioServicePost,
    scenario_id: int = Path(..., description="scenario identifier"),
    user: UserDTO = Depends(get_user),
) -> ScenarioUrbanObject:
    """Create new service for given scenario, physical object and geometry.

    You must be the owner of the relevant project.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    urban_object = await user_project_service.add_service(service, scenario_id, user.id)

    return ScenarioUrbanObject.from_dto(urban_object)


@projects_router.put(
    "/scenarios/{scenario_id}/services/{service_id}",
    response_model=ScenarioService,
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def put_service(
    request: Request,
    service: ServicesDataPut,
    scenario_id: int = Path(..., description="scenario identifier"),
    service_id: int = Path(..., description="service identifier"),
    is_scenario_object: bool = Query(..., description="to determine scenario object"),
    user: UserDTO = Depends(get_user),
) -> ScenarioService:
    """Update scenario service - all attributes.

    You must be the owner of the relevant project.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    service_dto = await user_project_service.put_service(service, scenario_id, service_id, is_scenario_object, user.id)

    return ScenarioService.from_dto(service_dto)


@projects_router.patch(
    "/scenarios/{scenario_id}/services/{service_id}",
    response_model=ScenarioService,
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def patch_service(
    request: Request,
    service: ServicesDataPatch,
    scenario_id: int = Path(..., description="scenario identifier"),
    service_id: int = Path(..., description="service identifier"),
    is_scenario_object: bool = Query(..., description="to determine scenario object"),
    user: UserDTO = Depends(get_user),
) -> ScenarioService:
    """Update scenario service - only given fields.

    You must be the owner of the relevant project.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    service_dto = await user_project_service.patch_service(
        service,
        scenario_id,
        service_id,
        is_scenario_object,
        user.id,
    )

    return ScenarioService.from_dto(service_dto)


@projects_router.delete(
    "/scenarios/{scenario_id}/services/{service_id}",
    response_model=dict,
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def delete_service(
    request: Request,
    scenario_id: int = Path(..., description="scenario identifier"),
    service_id: int = Path(..., description="service identifier"),
    is_scenario_object: bool = Query(..., description="to determine scenario object"),
    user: UserDTO = Depends(get_user),
) -> dict:
    """Delete scenario service by given identifier.

    You must be the owner of the relevant project.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    return await user_project_service.delete_service(scenario_id, service_id, is_scenario_object, user.id)
