"""Services projects-related endpoints are defined here."""

from fastapi import Depends, Path, Query, Request, Security
from fastapi.security import HTTPBearer
from starlette import status

from idu_api.urban_api.dto.users import UserDTO
from idu_api.urban_api.handlers.v1.projects.routers import projects_router
from idu_api.urban_api.logic.projects import UserProjectService
from idu_api.urban_api.schemas import ScenarioService
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
    for_context: bool = Query(False, description="to get objects for context of project territory"),
    user: UserDTO = Depends(get_user),
) -> list[ScenarioService]:
    """Get list of services for given scenario.

    It could be specified by service type and urban function."""
    user_project_service: UserProjectService = request.state.user_project_service

    services = await user_project_service.get_services_by_scenario_id(
        scenario_id,
        user.id,
        service_type_id,
        urban_function_id,
        for_context,
    )

    return [ScenarioService.from_dto(service) for service in services]
