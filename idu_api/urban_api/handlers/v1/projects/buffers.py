"""Buffers projects-related endpoints are defined here."""

from fastapi import Depends, HTTPException, Path, Query, Request, Security
from fastapi.security import HTTPBearer
from geojson_pydantic import Feature
from geojson_pydantic.geometries import Geometry
from starlette import status

from idu_api.urban_api.dto.users import UserDTO
from idu_api.urban_api.handlers.v1.projects.routers import projects_router
from idu_api.urban_api.logic.projects import UserProjectService
from idu_api.urban_api.schemas import (
    BufferAttributes,
    OkResponse,
    ScenarioBuffer,
    ScenarioBufferAttributes,
    ScenarioBufferDelete,
    ScenarioBufferPut,
)
from idu_api.urban_api.schemas.geometries import GeoJSONResponse
from idu_api.urban_api.utils.auth_client import get_user


@projects_router.get(
    "/scenarios/{scenario_id}/buffers",
    response_model=GeoJSONResponse[Feature[Geometry, ScenarioBufferAttributes]],
    status_code=status.HTTP_200_OK,
)
async def get_buffers_by_scenario_id(
    request: Request,
    scenario_id: int = Path(..., description="scenario identifier", gt=0),
    buffer_type_id: int | None = Query(None, description="buffer type identifier", gt=0),
    physical_object_type_id: int | None = Query(None, description="physical object type identifier", gt=0),
    service_type_id: int | None = Query(None, description="service type identifier", gt=0),
    user: UserDTO = Depends(get_user),
) -> GeoJSONResponse[Feature[Geometry, ScenarioBufferAttributes]]:
    """
    ## Get a list of buffers in GeoJSON format for a given scenario

    **WARNING:** You can only filter by physical object type or service_type.

    ### Parameters:
    - **scenario_id** (int, Path): Unique identifier of the scenario.
    - **buffer_type_id** (int, Query): To filter by buffer type.
    - **physical_object_type_id** (int, Query): To filter by physical object type.
    - **service_type_id** (int, Query): To filter by service type.

    ### Returns:
    - **GeoJSONResponse[Feature[Geometry, ScenarioFunctionalZoneWithoutGeometry]]**: Buffers in GeoJSON format.

    ### Errors:
    - **400 Bad Request**: If you set both `physical_object_type_id` and `service_type_id`.
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the scenario does not exist.

    ### Constraints:
    - The user must be the relevant project owner or the project must be publicly accessible.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    if physical_object_type_id is not None and service_type_id is not None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Please, choose either physical_object_type_id or service_type_id",
        )

    buffers = await user_project_service.get_buffers_by_scenario_id(
        scenario_id, buffer_type_id, physical_object_type_id, service_type_id, user
    )

    return await GeoJSONResponse.from_list([buffer.to_geojson_dict() for buffer in buffers])


@projects_router.get(
    "/scenarios/{scenario_id}/context/buffers",
    response_model=GeoJSONResponse[Feature[Geometry, ScenarioBufferAttributes]],
    status_code=status.HTTP_200_OK,
)
async def get_context_buffers(
    request: Request,
    scenario_id: int = Path(..., description="scenario identifier", gt=0),
    buffer_type_id: int | None = Query(None, description="buffer type identifier", gt=0),
    physical_object_type_id: int | None = Query(None, description="physical object type identifier", gt=0),
    service_type_id: int | None = Query(None, description="service type identifier", gt=0),
    user: UserDTO = Depends(get_user),
) -> GeoJSONResponse[Feature[Geometry, BufferAttributes]]:
    """
    ## Get buffers for the context of a project territory in GeoJSON format.

    ### Parameters:
    - **scenario_id** (int, Path): Unique identifier of the scenario.
    - **buffer_type_id** (int, Query): To filter by buffer type.
    - **physical_object_type_id** (int, Query): To filter by physical object type.
    - **service_type_id** (int, Query): To filter by service type.

    ### Returns:
    - **GeoJSONResponse[Feature[Geometry, BufferAttributes]]**: A GeoJSON response containing the buffers.

    ### Errors:
    - **400 Bad Request**: If you set both `physical_object_type_id` and `service_type_id`.
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the project does not exist.

    ### Constraints:
    - The user must be the owner of the relevant project or the project must be publicly available.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    if physical_object_type_id is not None and service_type_id is not None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Please, choose either physical_object_type_id or service_type_id",
        )

    buffers = await user_project_service.get_context_buffers(
        scenario_id,
        buffer_type_id,
        physical_object_type_id,
        service_type_id,
        user,
    )

    return await GeoJSONResponse.from_list([buffer.to_geojson_dict() for buffer in buffers])


@projects_router.put(
    "/scenarios/{scenario_id}/buffers",
    response_model=ScenarioBuffer,
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def put_buffer(
    request: Request,
    buffer: ScenarioBufferPut,
    scenario_id: int = Path(..., description="scenario identifier", gt=0),
    user: UserDTO = Depends(get_user),
) -> ScenarioBuffer:
    """
    ## Create or update a scenario buffer.

    **NOTE 1:** If the geometry is not passed, the method will try to automatically create a buffer with default radius
    (it depends on physical object/service type) for given urban object.

    **NOTE 2:** If a buffer the such params already exists, it will be updated.
    Otherwise, a new buffer will be created.

    ### Parameters:
    - **scenario_id** (int, Path): Unique identifier of the scenario.
    - **buffer** (ScenarioBufferPut, Body): The updated buffer data (containing ids to find urban object).

    ### Returns:
    - **ScenarioBuffer**: The created scenario buffer.

    ### Errors:
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the scenario (or related entity) does not exist.

    ### Constraints:
    - The user must be the owner of the relevant project.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    buffer_dto = await user_project_service.put_scenario_buffer(buffer, scenario_id, user)

    return ScenarioBuffer.from_dto(buffer_dto)


@projects_router.delete(
    "/scenarios/{scenario_id}/buffers",
    response_model=OkResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def delete_buffer(
    request: Request,
    buffer: ScenarioBufferDelete,
    scenario_id: int = Path(..., description="scenario identifier", gt=0),
    user: UserDTO = Depends(get_user),
) -> OkResponse:
    """
    ## Delete a scenario buffer.

    ### Parameters:
    - **scenario_id** (int, Path): Unique identifier of the scenario.
    - **buffer** (ScenarioBufferPut, Body): Data to search for a buffer to delete.

    ### Returns:
    - **OkResponse**: A confirmation message of the deletion.

    ### Errors:
    - **403 Forbidden**: If the user does not have access rights.
    - **404 Not Found**: If the scenario or buffer does not exist.

    ### Constraints:
    - The user must be the owner of the relevant project.
    """
    user_project_service: UserProjectService = request.state.user_project_service

    await user_project_service.delete_scenario_buffer(buffer, scenario_id, user)

    return OkResponse()
