"""Physical object handlers are defined here."""

from fastapi import Body, Path, Query, Request
from starlette import status

from idu_api.urban_api.logic.physical_objects import PhysicalObjectsService
from idu_api.urban_api.schemas import (
    LivingBuilding,
    LivingBuildingPatch,
    LivingBuildingPost,
    LivingBuildingPut,
    ObjectGeometry,
    OkResponse,
    PhysicalObject,
    PhysicalObjectPatch,
    PhysicalObjectPost,
    PhysicalObjectPut,
    PhysicalObjectWithGeometryPost,
    Service,
    ServiceWithGeometry,
    UrbanObject,
)
from idu_api.urban_api.schemas.geometries import AllPossibleGeometry
from idu_api.urban_api.schemas.physical_objects import PhysicalObjectWithGeometry

from .routers import physical_objects_router


@physical_objects_router.get(
    "/physical_object/{physical_object_id}",
    response_model=PhysicalObject,
    status_code=status.HTTP_200_OK,
)
async def get_physical_object_by_id_with_territories(
    request: Request,
    physical_object_id: int = Path(..., description="physical object identifier", gt=0),
) -> PhysicalObject:
    """
    ## Get a physical object by its identifier, including parent territories.

    ### Parameters:
    - **physical_object_id** (int, Path): Unique identifier of the physical object.

    ### Returns:
    - **PhysicalObject**: The requested physical object, including parent territories.

    ### Errors:
    - **404 Not Found**: If the physical object does not exist.
    """
    physical_objects_service: PhysicalObjectsService = request.state.physical_objects_service

    physical_object = await physical_objects_service.get_physical_object_by_id(physical_object_id)

    return PhysicalObject.from_dto(physical_object)


@physical_objects_router.post(
    "/physical_objects",
    response_model=UrbanObject,
    status_code=status.HTTP_201_CREATED,
)
async def add_physical_object_with_geometry(
    request: Request, physical_object: PhysicalObjectWithGeometryPost
) -> UrbanObject:
    """
    ## Create a new physical object with geometry.

    ### Parameters:
    - **physical_object** (PhysicalObjectWithGeometryPost, Body): Data for the new physical object with geometry.

    ### Returns:
    - **UrbanObject**: The created urban object (physical object + geometry).

    ## Errors:
    - **404 Not Found**: If related entity does not exist.
    """
    physical_objects_service: PhysicalObjectsService = request.state.physical_objects_service

    urban_object = await physical_objects_service.add_physical_object_with_geometry(physical_object)

    return UrbanObject.from_dto(urban_object)


@physical_objects_router.put(
    "/physical_objects/{physical_object_id}",
    response_model=PhysicalObject,
    status_code=status.HTTP_200_OK,
    deprecated=True,
)
async def put_physical_object(
    request: Request,
    physical_object: PhysicalObjectPut,
    physical_object_id: int = Path(..., description="physical object identifier", gt=0),
) -> PhysicalObject:
    """
    ## Update a physical object by replacing all attributes.

    **WARNING:** This method has been deprecated since version 0.34.0 and will be removed in version 1.0.
    Instead, use PATCH method.

    ### Parameters:
    - **physical_object_id** (int, Path): Unique identifier of the physical object.
    - **physical_object** (PhysicalObjectPut, Body): New data for the physical object.

    ### Returns:
    - **PhysicalObject**: The updated physical object.

    ## Errors:
    - **404 Not Found**: If the physical object (or related entity) does not exist.
    """
    physical_objects_service: PhysicalObjectsService = request.state.physical_objects_service

    physical_object_dto = await physical_objects_service.put_physical_object(physical_object, physical_object_id)

    return PhysicalObject.from_dto(physical_object_dto)


@physical_objects_router.patch(
    "/physical_objects/{physical_object_id}",
    response_model=PhysicalObject,
    status_code=status.HTTP_200_OK,
)
async def patch_physical_object(
    request: Request,
    physical_object: PhysicalObjectPatch,
    physical_object_id: int = Path(..., description="physical object identifier", gt=0),
) -> PhysicalObject:
    """
    ## Partially update a physical object.

    ### Parameters:
    - **physical_object_id** (int, Path): Unique identifier of the physical object.
    - **physical_object** (PhysicalObjectPatch, Body): Fields to update in the physical object.

    ### Returns:
    - **PhysicalObject**: The updated physical object with modified attributes.

    ## Errors:
    - **404 Not Found**: If the physical object (or related entity) does not exist.
    """
    physical_objects_service: PhysicalObjectsService = request.state.physical_objects_service

    physical_object_dto = await physical_objects_service.patch_physical_object(physical_object, physical_object_id)

    return PhysicalObject.from_dto(physical_object_dto)


@physical_objects_router.delete(
    "/physical_objects/{physical_object_id}",
    response_model=OkResponse,
    status_code=status.HTTP_200_OK,
)
async def delete_physical_object(
    request: Request,
    physical_object_id: int = Path(..., description="physical object identifier", gt=0),
) -> OkResponse:
    """
    ## Delete a physical object by its identifier.

    ### Parameters:
    - **physical_object_id** (int, Path): Unique identifier of the physical object.

    ### Returns:
    - **OkResponse**: A confirmation message of the deletion.

    ## Errors:
    - **404 Not Found**: If the physical object does not exist.
    """
    physical_objects_service: PhysicalObjectsService = request.state.physical_objects_service

    await physical_objects_service.delete_physical_object(physical_object_id)

    return OkResponse()


@physical_objects_router.post(
    "/living_buildings",
    response_model=PhysicalObject,
    status_code=status.HTTP_201_CREATED,
)
async def add_living_building(request: Request, living_building: LivingBuildingPost) -> PhysicalObject:
    """
    ## Create a new living building.

    **WARNING:** There can only be one living building per physical object.

    ### Parameters:
    - **living_building** (LivingBuildingPost, Body): Data for the new living building.

    ### Returns:
    - **PhysicalObject**: The created living building.

    ## Errors:
    - **404 Not Found**: If the physical object does not exist.
    - **409 Conflict**: If a living building already exists for this physical object.
    """
    physical_objects_service: PhysicalObjectsService = request.state.physical_objects_service

    living_building_dto = await physical_objects_service.add_living_building(living_building)

    return PhysicalObject.from_dto(living_building_dto)


@physical_objects_router.put(
    "/living_buildings",
    response_model=PhysicalObject,
    status_code=status.HTTP_200_OK,
)
async def put_living_building(request: Request, living_building: LivingBuildingPut) -> PhysicalObject:
    """
    ## Create or update a living building.

    **NOTE:** If a living building for given physical object already exists, it will be updated.
    Otherwise, a new living building will be created.

    ### Parameters:
    - **living_building** (LivingBuildingPut, Body): Data for updating or creating a living building.

    ### Returns:
    - **PhysicalObject**: The updated or created living building.

    ## Errors:
    - **404 Not Found**: If the physical object does not exist.
    """
    physical_objects_service: PhysicalObjectsService = request.state.physical_objects_service

    living_building_dto = await physical_objects_service.put_living_building(living_building)

    return PhysicalObject.from_dto(living_building_dto)


@physical_objects_router.patch(
    "/living_buildings/{living_building_id}",
    response_model=PhysicalObject,
    status_code=status.HTTP_200_OK,
)
async def patch_living_building(
    request: Request,
    living_building: LivingBuildingPatch,
    living_building_id: int = Path(..., description="living building identifier", gt=0),
) -> PhysicalObject:
    """
    ## Partially update a living building.

    ### Parameters:
    - **living_building_id** (int, Path): Unique identifier of the living building.
    - **living_building** (LivingBuildingPatch, Body): Fields to update in the living building.

    ### Returns:
    - **PhysicalObject**: The updated living building with modified attributes.

    ## Errors:
    - **404 Not Found**: If the living building (or related entity) does not exist.
    - **409 Conflict**: If a living building already exists for given physical object.
    """
    physical_objects_service: PhysicalObjectsService = request.state.physical_objects_service

    living_building_dto = await physical_objects_service.patch_living_building(living_building, living_building_id)

    return PhysicalObject.from_dto(living_building_dto)


@physical_objects_router.delete(
    "/living_buildings/{living_building_id}",
    response_model=OkResponse,
    status_code=status.HTTP_200_OK,
)
async def delete_living_building(
    request: Request,
    living_building_id: int = Path(..., description="living building identifier", gt=0),
) -> OkResponse:
    """
    ## Delete a living building by its identifier.

    ### Parameters:
    - **living_building_id** (int, Path): Unique identifier of the living building.

    ### Returns:
    - **OkResponse**: A confirmation message of the deletion.

    ## Errors:
    - **404 Not Found**: If the living building does not exist.
    """
    physical_objects_service: PhysicalObjectsService = request.state.physical_objects_service

    await physical_objects_service.delete_living_building(living_building_id)

    return OkResponse()


@physical_objects_router.get(
    "/physical_objects/{physical_object_id}/living_buildings",
    response_model=list[LivingBuilding],
    status_code=status.HTTP_200_OK,
    deprecated=True,
)
async def get_living_buildings_by_physical_object_id(
    request: Request,
    physical_object_id: int = Path(..., description="physical object identifier", gt=0),
) -> list[LivingBuilding]:
    """
    ## Get all living buildings within a given physical object.

    **WARNING:** This method has been deprecated since version 0.33.1 and will be removed in version 1.0.
    Every physical object returns with full information about its living building.

    ### Parameters:
    - **physical_object_id** (int, Path): Unique identifier of the physical object.

    ### Returns:
    - **list[LivingBuilding]**: A list of living buildings inside the specified physical object.

    ## Errors:
    - **404 Not Found**: If the physical object does not exist.
    """
    physical_objects_service: PhysicalObjectsService = request.state.physical_objects_service

    buildings = await physical_objects_service.get_living_buildings_by_physical_object_id(physical_object_id)

    return [LivingBuilding.from_dto(building) for building in buildings]


@physical_objects_router.get(
    "/physical_objects/{physical_object_id}/services",
    response_model=list[Service],
    status_code=status.HTTP_200_OK,
)
async def get_services_by_physical_object_id(
    request: Request,
    physical_object_id: int = Path(..., description="physical object identifier", gt=0),
    service_type_id: int = Query(None, description="to filter by service type", gt=0),
    territory_type_id: int = Query(None, description="to filter by territory type", gt=0),
) -> list[Service]:
    """
    ## Get all services within a given physical object.

    ### Parameters:
    - **physical_object_id** (int, Path): Unique identifier of the physical object.
    - **service_type_id** (int | None, Query): Filters results by service type.
    - **territory_type_id** (int | None, Query): Filters results by territory type.

    ### Returns:
    - **list[Service]**: A list of services inside the specified physical object.

    ## Errors:
    - **404 Not Found**: If the physical object does not exist.
    """
    physical_objects_service: PhysicalObjectsService = request.state.physical_objects_service

    services = await physical_objects_service.get_services_by_physical_object_id(
        physical_object_id, service_type_id, territory_type_id
    )

    return [Service.from_dto(service) for service in services]


@physical_objects_router.get(
    "/physical_objects/{physical_object_id}/services_with_geometry",
    response_model=list[ServiceWithGeometry],
    status_code=status.HTTP_200_OK,
)
async def get_services_with_geometry_by_physical_object_id(
    request: Request,
    physical_object_id: int = Path(..., description="physical object identifier", gt=0),
    service_type_id: int = Query(None, description="to filter by service type", gt=0),
    territory_type_id: int = Query(None, description="to filter by territory type", gt=0),
) -> list[ServiceWithGeometry]:
    """
    ## Get all services with geometry within a given physical object.

    ### Parameters:
    - **physical_object_id** (int, Path): Unique identifier of the physical object.
    - **service_type_id** (int | None, Query): Filters results by service type.
    - **territory_type_id** (int | None, Query): Filters results by territory type.

    ### Returns:
    - **list[ServiceWithGeometry]**: A list of services with geometry inside the specified physical object.

    ## Errors:
    - **404 Not Found**: If the physical object does not exist.
    """
    physical_objects_service: PhysicalObjectsService = request.state.physical_objects_service

    services = await physical_objects_service.get_services_with_geometry_by_physical_object_id(
        physical_object_id, service_type_id, territory_type_id
    )

    return [ServiceWithGeometry.from_dto(service) for service in services]


@physical_objects_router.get(
    "/physical_objects/{physical_object_id}/geometries",
    response_model=list[ObjectGeometry],
    status_code=status.HTTP_200_OK,
)
async def get_physical_object_geometries(
    request: Request,
    physical_object_id: int = Path(..., description="physical object identifier", gt=0),
) -> list[ObjectGeometry]:
    """
    ## Get geometries associated with a given physical object.

    ### Parameters:
    - **physical_object_id** (int, Path): Unique identifier of the physical object.

    ### Returns:
    - **list[ObjectGeometry]**: A list of geometries associated with the specified physical object.

    ## Errors:
    - **404 Not Found**: If the physical object does not exist.
    """
    physical_objects_service: PhysicalObjectsService = request.state.physical_objects_service

    geometries = await physical_objects_service.get_physical_object_geometries(physical_object_id)

    return [ObjectGeometry.from_dto(geometry) for geometry in geometries]


@physical_objects_router.post(
    "/physical_objects/around",
    response_model=list[PhysicalObjectWithGeometry],
    status_code=status.HTTP_200_OK,
)
async def get_physical_objects_around_geometry(
    request: Request,
    geometry: AllPossibleGeometry,
    physical_object_type_id: int | None = Query(None, description="physical object type identifier", gt=0),
) -> list[PhysicalObjectWithGeometry]:
    """
    ## Get physical objects within a specified area.

    ### Parameters:
    - **geometry** (AllPossibleGeometry, Body): Geometry defining the search area.
    - **physical_object_type_id** (int | None, Query): Filters results by physical object type.

    ### Returns:
    - **list[PhysicalObjectWithGeometry]**: A list of physical objects within the specified area.

    ## Errors:
    - **404 Not Found**: If the physical object does not exist.
    """
    physical_objects_service: PhysicalObjectsService = request.state.physical_objects_service

    physical_objects_with_geometry_dto = await physical_objects_service.get_physical_objects_around(
        geometry.as_shapely_geometry(), physical_object_type_id, 50
    )
    return [PhysicalObjectWithGeometry.from_dto(obj) for obj in physical_objects_with_geometry_dto]


@physical_objects_router.post(
    "/physical_objects/{object_geometry_id}",
    response_model=UrbanObject,
    status_code=status.HTTP_200_OK,
)
async def add_physical_object_to_object_geometry(
    request: Request,
    object_geometry_id: int = Path(..., description="object geometry identifier", gt=0),
    physical_object: PhysicalObjectPost = Body(..., description="physical object"),
) -> UrbanObject:
    """
    ## Add a physical object to an object geometry.

    ### Parameters:
    - **object_geometry_id** (int, Path): Unique identifier of the object geometry.
    - **physical_object** (PhysicalObjectPost, Body): Data for the new physical object.

    ### Returns:
    - **UrbanObject**: The created urban object (new physical object + existing geometry).

    ## Errors:
    - **404 Not Found**: If the object geometry does not exist.
    """
    physical_objects_service: PhysicalObjectsService = request.state.physical_objects_service

    urban_object = await physical_objects_service.add_physical_object_to_object_geometry(
        object_geometry_id, physical_object
    )

    return UrbanObject.from_dto(urban_object)
