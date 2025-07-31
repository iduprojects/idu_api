"""Functional zones endpoints are defined here."""

from fastapi import Query, Request
from starlette import status

from idu_api.urban_api.logic.buffers import BufferService
from idu_api.urban_api.schemas import (
    Buffer,
    BufferPut,
    BufferType,
    BufferTypePost,
    DefaultBufferValue,
    DefaultBufferValuePost,
    DefaultBufferValuePut,
    OkResponse,
)

from .routers import buffers_router


@buffers_router.get(
    "/buffer_types",
    response_model=list[BufferType],
    status_code=status.HTTP_200_OK,
)
async def get_buffer_types(request: Request) -> list[BufferType]:
    """
    ## Get the list of buffer types.

    ### Returns:
    - **list[BufferType]**: A list of buffer types.
    """
    buffers_service: BufferService = request.state.buffers_service

    buffer_types = await buffers_service.get_buffer_types()

    return [BufferType.from_dto(zone_type) for zone_type in buffer_types]


@buffers_router.post(
    "/buffer_types",
    response_model=BufferType,
    status_code=status.HTTP_201_CREATED,
)
async def add_buffer_type(request: Request, buffer_type: BufferTypePost) -> BufferType:
    """
    ## Create a new buffer type.

    ### Parameters:
    - **buffer_type** (BufferTypePost, Body): Data for the new buffer type.

    ### Returns:
    - **BufferType**: The created buffer type.

    ### Errors:
    - **409 Conflict**: If a buffer type with the such name already exists.
    """
    buffers_service: BufferService = request.state.buffers_service

    new_buffer_type = await buffers_service.add_buffer_type(buffer_type)

    return BufferType.from_dto(new_buffer_type)


@buffers_router.get(
    "/buffer_types/defaults",
    response_model=list[DefaultBufferValue],
    status_code=status.HTTP_200_OK,
)
async def get_all_default_buffer_values(request: Request) -> list[DefaultBufferValue]:
    """
    ## Get a list of all buffer types with default buffer radius (in meters) for each physical object/service type.

    ### Returns:
    - **list[DefaultBufferValue]**: List of all buffer types with default value for each physical object/service type.
    """
    buffers_service: BufferService = request.state.buffers_service

    values = await buffers_service.get_all_default_buffer_values()

    return [DefaultBufferValue.from_dto(value) for value in values]


@buffers_router.post(
    "/buffer_types/defaults",
    response_model=DefaultBufferValue,
    status_code=status.HTTP_201_CREATED,
)
async def add_default_buffer_value(
    request: Request,
    default_buffer_value: DefaultBufferValuePost,
) -> DefaultBufferValue:
    """
    ## Add a new default buffer value for given buffer type and physical object/service type.

    ### Parameters:
    - **default_buffer_value** (DefaultBufferValuePost, Body): Data for the new default buffer value.

    ### Returns:
    - **DefaultBufferValue**: The created default buffer value.

    ### Errors:
    - **404 Not Found**: If related entity does not exist.
    """
    buffers_service: BufferService = request.state.buffers_service

    default_buffer_value_dto = await buffers_service.add_default_buffer_value(default_buffer_value)

    return DefaultBufferValue.from_dto(default_buffer_value_dto)


@buffers_router.put(
    "/buffer_types/defaults",
    response_model=DefaultBufferValue,
    status_code=status.HTTP_200_OK,
)
async def put_buffer_default_value(
    request: Request,
    default_buffer_value: DefaultBufferValuePut,
) -> DefaultBufferValue:
    """
    ## Add or update a default buffer value.

    **NOTE:** If a default buffer value the such params already exists, it will be updated.
    Otherwise, a new default buffer value will be created.

    ### Parameters:
    - **default_buffer_value** (DefaultBufferValuePut, Body): New data for the buffer.

    ### Returns:
    - **Buffer**: The updated buffer.

    ### Errors:
    - **404 Not Found**: If the buffer type (or related entity) does not exist.
    """
    buffers_service: BufferService = request.state.buffers_service

    buffer_type_default_dto = await buffers_service.put_default_buffer_value(default_buffer_value)

    return DefaultBufferValue.from_dto(buffer_type_default_dto)


@buffers_router.put(
    "/buffers",
    response_model=Buffer,
    status_code=status.HTTP_200_OK,
)
async def put_buffer(request: Request, buffer: BufferPut) -> Buffer:
    """
    ## Update a buffer by replacing all attributes.

    **NOTE 1:** If the geometry is not passed, the method will try to automatically create a buffer with default radius
    (it depends on physical object/service type) for given urban object.

    **NOTE 2:** If a buffer the such params already exists, it will be updated.
    Otherwise, a new buffer will be created.

    ### Parameters:
    - **buffer** (BufferPut, Body): New data for the buffer.

    ### Returns:
    - **Buffer**: The updated buffer.

    ### Errors:
    - **400 Bad Request**: If the geometry is not passed and default radius does not exist.
    - **404 Not Found**: If the buffer (or related entity) does not exist.
    """
    buffers_service: BufferService = request.state.buffers_service

    buffer_dto = await buffers_service.put_buffer(buffer)

    return Buffer.from_dto(buffer_dto)


@buffers_router.delete(
    "/buffers",
    response_model=OkResponse,
    status_code=status.HTTP_200_OK,
)
async def delete_buffer(
    request: Request,
    buffer_type_id: int = Query(..., description="buffer type identifier", gt=0),
    urban_object_id: int = Query(..., description="urban object identifier", gt=0),
) -> OkResponse:
    """
    ## Delete a buffer by buffer type and urban object.

    ### Parameters:
    - **buffer_type_id** (int, Query): Unique identifier of the buffer type.
    - **urban_object_id** (int, Query): Unique identifier of the urban object.

    ### Returns:
    - **OkResponse**: A confirmation message of the deletion.

    ### Errors:
    - **404 Not Found**: If the buffer does not exist.
    """
    buffers_service: BufferService = request.state.buffers_service

    await buffers_service.delete_buffer(buffer_type_id, urban_object_id)

    return OkResponse()
