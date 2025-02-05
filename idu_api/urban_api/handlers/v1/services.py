"""Services handlers are defined here."""

from fastapi import Path, Query, Request
from starlette import status

from idu_api.urban_api.logic.services import ServicesDataService
from idu_api.urban_api.schemas import (
    OkResponse,
    Service,
    ServicePatch,
    ServicePost,
    ServicePut,
    UrbanObject,
)

from .routers import services_router


@services_router.get(
    "/services/{service_id}",
    response_model=Service,
    status_code=status.HTTP_200_OK,
)
async def get_service_by_id(
    request: Request,
    service_id: int = Path(..., description="service identifier", gt=0),
) -> Service:
    """
    ## Get a service by its identifier, including parent territories.

    ### Parameters:
    - **service_id** (int, Path): Unique identifier of the service.

    ### Returns:
    - **Service**: The requested service, including parent territories.

    ### Errors:
    - **404 Not Found**: If the service does not exist.
    """
    services_data_service: ServicesDataService = request.state.services_data_service

    service = await services_data_service.get_service_by_id(service_id)

    return Service.from_dto(service)


@services_router.post(
    "/services",
    response_model=Service,
    status_code=status.HTTP_201_CREATED,
)
async def add_service(request: Request, service: ServicePost) -> Service:
    """
    ## Create a new service.

    ### Parameters:
    - **service** (ServicePost, Body): Data for the new service.

    ### Returns:
    - **UrbanObject**: The created or updated urban object (service + geometry + service).

    ### Errors:
    - **404 Not Found**: If the physical object or object geometry (or related entity) does not exist.
    """
    services_data_service: ServicesDataService = request.state.services_data_service

    service_dto = await services_data_service.add_service(service)

    return Service.from_dto(service_dto)


@services_router.put(
    "/services/{service_id}",
    response_model=Service,
    status_code=status.HTTP_200_OK,
    deprecated=True,
)
async def put_service(
    request: Request,
    service: ServicePut,
    service_id: int = Path(..., description="service identifier", gt=0),
) -> Service:
    """
    ## Update a service by replacing all attributes.

    **WARNING:** This method has been deprecated since version 0.34.0 and will be removed in version 1.0.
    Instead, use PATCH method.

    ### Parameters:
    - **service_id** (int, Path): Unique identifier of the service.
    - **service** (ServicePut, Body): New data for the service.

    ### Returns:
    - **Service**: The updated service.

    ### Errors:
    - **404 Not Found**: If the service (or related entity) does not exist.
    """
    services_data_service: ServicesDataService = request.state.services_data_service

    service_dto = await services_data_service.put_service(service, service_id)

    return Service.from_dto(service_dto)


@services_router.patch(
    "/services/{service_id}",
    response_model=Service,
    status_code=status.HTTP_200_OK,
)
async def patch_service(
    request: Request,
    service: ServicePatch,
    service_id: int = Path(..., description="service identifier", gt=0),
) -> Service:
    """
    ## Partially update a service.

    ### Parameters:
    - **service_id** (int, Path): Unique identifier of the service.
    - **service** (ServicePatch, Body): Fields to update in the service.

    ### Returns:
    - **Service**: The updated service with modified attributes.

    ### Errors:
    - **404 Not Found**: If the service (or related entity) does not exist.
    """
    services_data_service: ServicesDataService = request.state.services_data_service

    service_dto = await services_data_service.patch_service(service, service_id)

    return Service.from_dto(service_dto)


@services_router.delete(
    "/services/{service_id}",
    response_model=OkResponse,
    status_code=status.HTTP_200_OK,
)
async def delete_service(
    request: Request,
    service_id: int = Path(..., description="service identifier", gt=0),
) -> OkResponse:
    """
    ## Delete a service by its identifier.

    ### Parameters:
    - **service_id** (int, Path): Unique identifier of the service.

    ### Returns:
    - **OkResponse**: A confirmation message of the deletion.

    ### Errors:
    - **404 Not Found**: If the service (or related entity) does not exist.
    """
    services_data_service: ServicesDataService = request.state.services_data_service

    await services_data_service.delete_service(service_id)

    return OkResponse()


@services_router.post(
    "/services/{service_id}",
    response_model=UrbanObject,
    status_code=status.HTTP_200_OK,
)
async def add_service_to_objects(
    request: Request,
    service_id: int = Path(..., description="service identifier", gt=0),
    physical_object_id: int = Query(..., description="physical_object identifier", gt=0),
    object_geometry_id: int = Query(..., description="object geometry identifier", gt=0),
) -> UrbanObject:
    """
    ## Add an existing service to a given pair of physical_object and object geometry.

    ### Parameters:
    - **service** (ServicePost, Body): Data for the new service.

    ### Returns:
    - **UrbanObject**: The created urban object (physical object + geometry + service).

    ### Errors:
    - **404 Not Found**: If the service or physical object or object geometry does not exist.
    - **409 Conflict**: If an urban object with such `physical_object_id`, `object_geometry_id` and `service_id` already exists.
    """
    services_data_service: ServicesDataService = request.state.services_data_service

    urban_object_dto = await services_data_service.add_service_to_object(
        service_id, physical_object_id, object_geometry_id
    )

    return UrbanObject.from_dto(urban_object_dto)
