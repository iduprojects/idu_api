"""Social groups handlers are defined here."""

from fastapi import HTTPException, Path, Query, Request
from starlette import status

from idu_api.urban_api.logic.soc_groups import SocGroupsService
from idu_api.urban_api.schemas import (
    OkResponse,
    SocGroup,
    SocGroupIndicatorValue,
    SocGroupIndicatorValuePost,
    SocGroupIndicatorValuePut,
    SocGroupPost,
    SocGroupServiceTypePost,
    SocGroupWithServiceTypes,
    SocValue,
    SocValuePost,
    SocValueWithSocGroups,
)
from idu_api.urban_api.schemas.enums import ValueType

from .routers import soc_groups_router


@soc_groups_router.get(
    "/social_groups",
    response_model=list[SocGroup],
    status_code=status.HTTP_200_OK,
)
async def get_social_groups(request: Request) -> SocGroup:
    """
    ## Get a list of all social groups.

    ### Returns:
    - **list[SocGroup]**: A list of social groups.
    """
    soc_groups_service: SocGroupsService = request.state.soc_groups_service

    soc_groups = await soc_groups_service.get_social_groups()

    return [SocGroup.from_dto(group) for group in soc_groups]


@soc_groups_router.get(
    "/social_groups/{soc_group_id}",
    response_model=SocGroupWithServiceTypes,
    status_code=status.HTTP_200_OK,
)
async def get_social_group_by_id(
    request: Request,
    soc_group_id: int = Path(..., description="social group identifier", gt=0),
) -> SocGroupWithServiceTypes:
    """
    ## Get social group with all associated service types by identifier.

    ### Parameters:
    - **soc_group_id** (int, Path): Social group identifier.

    ### Returns:
    - **SocGroupWithServiceTypes**: Social group with all associated service types.

    ### Errors:
    - **404 Not Found**: If the social group does not exist.
    """
    soc_groups_service: SocGroupsService = request.state.soc_groups_service

    soc_group = await soc_groups_service.get_social_group_by_id(soc_group_id)

    return SocGroupWithServiceTypes.from_dto(soc_group)


@soc_groups_router.post(
    "/social_groups",
    response_model=SocGroupWithServiceTypes,
    status_code=status.HTTP_201_CREATED,
)
async def add_social_group(request: Request, soc_group: SocGroupPost) -> SocGroupWithServiceTypes:
    """
    ## Create a new social group.

    ### Parameters:
    - **soc_group** (SocGroupPost, Body): Data for the new social group.

    ### Returns:
    - **SocGroupWithServiceTypes**: Created social group.

    ### Errors:
    - **409 Conflict**: If a social group with the such name already exists.
    """
    soc_groups_service: SocGroupsService = request.state.soc_groups_service

    new_soc_group = await soc_groups_service.add_social_group(soc_group)

    return SocGroupWithServiceTypes.from_dto(new_soc_group)


@soc_groups_router.post(
    "/social_groups/{soc_group_id}/service_types",
    response_model=SocGroupWithServiceTypes,
    status_code=status.HTTP_201_CREATED,
)
async def add_service_type_to_social_group(
    request: Request,
    service_type: SocGroupServiceTypePost,
    soc_group_id: int = Path(..., description="social group identifier", gt=0),
) -> SocGroupWithServiceTypes:
    """
    ## Add service type to social group.

    ### Parameters:
    - **soc_group_id** (int, Path): Social group identifier.
    - **service_type** (SocGroupServiceTypePost, Body): Service type identifier and infrastructure type.

    ### Returns:
    - **SocGroupWithServiceTypes**: Social group with all associated service types.

    ### Errors:
    - **404 Not Found**: If the social group (or service type) does not exist.
    - **409 Conflict**: If the service type for the social group already exists.
    """
    soc_groups_service: SocGroupsService = request.state.soc_groups_service

    soc_group = await soc_groups_service.add_service_type_to_social_group(soc_group_id, service_type)

    return SocGroupWithServiceTypes.from_dto(soc_group)


@soc_groups_router.delete(
    "/social_groups/{soc_group_id}",
    response_model=OkResponse,
    status_code=status.HTTP_200_OK,
)
async def delete_social_group(
    request: Request,
    soc_group_id: int = Path(..., description="social group identifier", gt=0),
) -> OkResponse:
    """
    ## Delete social group by identifier.

    ### Parameters:
    - **soc_group_id** (int, Path): Social group identifier.

    ### Returns:
    - **OkResponse**: A confirmation message of the deletion.

    ### Errors:
    - **404 Not Found**: If the social group does not exist.
    """
    soc_groups_service: SocGroupsService = request.state.soc_groups_service

    await soc_groups_service.delete_social_group(soc_group_id)

    return OkResponse()


@soc_groups_router.get(
    "/social_values",
    response_model=list[SocValue],
    status_code=status.HTTP_200_OK,
)
async def get_social_values(request: Request) -> SocValue:
    """
    ## Get a list of all social values.

    ### Returns:
    - **list[SocValue]**: A list of social values.
    """
    soc_groups_service: SocGroupsService = request.state.soc_groups_service

    soc_values = await soc_groups_service.get_social_values()

    return [SocValue.from_dto(value) for value in soc_values]


@soc_groups_router.get(
    "/social_values/{soc_value_id}",
    response_model=SocValueWithSocGroups,
    status_code=status.HTTP_200_OK,
)
async def get_social_value_by_id(
    request: Request,
    soc_value_id: int = Path(..., description="social value identifier", gt=0),
) -> SocValueWithSocGroups:
    """
    ## Get social value with all associated social groups by identifier.

    ### Parameters:
    - **soc_value_id** (int, Path): Social value identifier.

    ### Returns:
    - **SocValueWithSocGroups**: Social value with all associated social groups.

    ### Errors:
    - **404 Not Found**: If the social value does not exist.
    """
    soc_groups_service: SocGroupsService = request.state.soc_groups_service

    soc_value = await soc_groups_service.get_social_value_by_id(soc_value_id)

    return SocValueWithSocGroups.from_dto(soc_value)


@soc_groups_router.post(
    "/social_values",
    response_model=SocValueWithSocGroups,
    status_code=status.HTTP_201_CREATED,
)
async def add_social_value(request: Request, soc_value: SocValuePost) -> SocValueWithSocGroups:
    """
    ## Create a new social value.

    ### Parameters:
    - **soc_value** (SocValuePost, Body): Data for the new social value.

    ### Returns:
    - **SocValueWithSocGroups**: Created social value.

    ### Errors:
    - **409 Conflict**: If a social value with such name already exists.
    """
    soc_groups_service: SocGroupsService = request.state.soc_groups_service

    new_soc_value = await soc_groups_service.add_social_value(soc_value)

    return SocValueWithSocGroups.from_dto(new_soc_value)


@soc_groups_router.post(
    "/social_groups/{soc_group_id}/values",
    response_model=SocValueWithSocGroups,
    status_code=status.HTTP_201_CREATED,
)
async def add_value_to_social_group(
    request: Request,
    soc_group_id: int = Path(..., description="social group identifier", gt=0),
    service_type_id: int = Query(..., description="service type identifier", gt=0),
    soc_value_id: int = Query(..., description="social value identifier", gt=0),
) -> SocValueWithSocGroups:
    """
    ## Add social value to social group and service type.

    ### Parameters:
    - **soc_group_id** (int, Path): Social group identifier.
    - **service_type_id** (int, Query): Service type identifier.
    - **soc_value_id** (int, Query): Social value identifier.

    ### Returns:
    - **SocValueWithSocGroups**: Social value with all associated social groups.

    ### Errors:
    - **404 Not Found**: If one of given entities does not exist.
    - **409 Conflict**: If the social value already exists for a given pair of social group and service type.
    """
    soc_groups_service: SocGroupsService = request.state.soc_groups_service

    new_soc_value = await soc_groups_service.add_value_to_social_group(soc_group_id, service_type_id, soc_value_id)

    return SocValueWithSocGroups.from_dto(new_soc_value)


@soc_groups_router.delete(
    "/social_values/{soc_value_id}",
    response_model=OkResponse,
    status_code=status.HTTP_200_OK,
)
async def delete_social_value(
    request: Request,
    soc_value_id: int = Path(..., description="social value identifier", gt=0),
) -> OkResponse:
    """
    ## Delete social value by identifier.

    ### Parameters:
    - **soc_value_id** (int, Path): Social value identifier.

    ### Returns:
    - **OkResponse**: A confirmation message of the deletion.

    ### Errors:
    - **404 Not Found**: If the social value does not exist.
    """
    soc_groups_service: SocGroupsService = request.state.soc_groups_service

    await soc_groups_service.delete_social_value(soc_value_id)

    return OkResponse()


@soc_groups_router.get(
    "/social_groups/{soc_group_id}/indicators",
    response_model=list[SocGroupIndicatorValue],
    status_code=status.HTTP_200_OK,
)
async def get_social_group_indicator_values(
    request: Request,
    soc_group_id: int = Path(..., description="social group identifier", gt=0),
    soc_value_id: int | None = Query(None, description="social value identifier", gt=0),
    territory_id: int | None = Query(None, description="territory identifier", gt=0),
    year: int | None = Query(None, description="year when value was modeled (skip to get values for all years)", gt=0),
    value_type: ValueType = Query(None, description="value type"),
    last_only: bool = Query(False, description="to get only the last values"),
) -> list[SocGroupIndicatorValue]:
    """
    ## Get social value with all associated social groups by identifier.

    **WARNING:** Set `last_only = True` only if you don't specify `year`.

    ### Parameters:
    - **soc_group_id** (int, Path): Social group identifier.
    - **soc_value_id** (int, Query): Filter results by social value.
    - **territory_id** (int, Query): Filter results by territory.
    - **year** (int, Query): Filter results by specified year.
    - **value_type** (ValueType, Query): Filter results by value type (real, forecast or target).
    - **last_only** (int, Query): To get only the last indicator value for each social group's value.

    ### Returns:
    - **list[SocGroupIndicatorValue]**: A list of indicator values for given social group.

    ### Errors:
    - **404 Not Found**: If the social group does not exist.
    - **422 Unprocessable Entity**: If both `last_only` is set to True and `year` is set.
    """
    soc_groups_service: SocGroupsService = request.state.soc_groups_service

    if last_only and year is not None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Please, choose either specific year or last_only",
        )

    soc_group_indicators = await soc_groups_service.get_social_group_indicator_values(
        soc_group_id, soc_value_id, territory_id, year, value_type, last_only
    )

    return [SocGroupIndicatorValue.from_dto(value) for value in soc_group_indicators]


@soc_groups_router.post(
    "/social_groups/{soc_group_id}/indicators",
    response_model=SocGroupIndicatorValue,
    status_code=status.HTTP_201_CREATED,
)
async def add_social_group_indicator_value(
    request: Request,
    soc_group_indicator: SocGroupIndicatorValuePost,
    soc_group_id: int = Path(..., description="social group identifier", gt=0),
) -> SocGroupIndicatorValue:
    """
    ## Create new social group's indicator value.

    ### Parameters:
    - **soc_group_id** (int, Path): Social group identifier.
    - **soc_group_indicator** (SocGroupIndicatorValuePost, Body): Data for the new social group's indicator value.

    ### Returns:
    - **SocGroupIndicatorValue**: Created social group's indicator value.

    ### Errors:
    - **404 Not Found**: If the social group (or related entity) does not exist.
    - **409 Conflict**: If an indicator value with the such parameters already exists.
    """
    soc_groups_service: SocGroupsService = request.state.soc_groups_service

    soc_group_indicator_dto = await soc_groups_service.add_social_group_indicator_value(
        soc_group_id, soc_group_indicator
    )

    return SocGroupIndicatorValue.from_dto(soc_group_indicator_dto)


@soc_groups_router.put(
    "/social_groups/{soc_group_id}/indicators",
    response_model=SocGroupIndicatorValue,
    status_code=status.HTTP_200_OK,
)
async def put_social_group_indicator_value(
    request: Request,
    soc_group_indicator: SocGroupIndicatorValuePut,
    soc_group_id: int = Path(..., description="social group identifier", gt=0),
) -> SocGroupIndicatorValue:
    """
    ## Update or create new social group's indicator value.

    **NOTE:** If an indicator value with the specified attributes already exists, it will be updated.
    Otherwise, a new indicator value will be created.

    ### Parameters:
    - **soc_group_id** (int, Path): Social group identifier.
    - **soc_group_indicator** (SocGroupIndicatorValuePost, Body): Data for the new social group's indicator value.

    ### Returns:
    - **SocGroupIndicatorValue**: Updated or created social group's indicator value.

    ### Errors:
    - **404 Not Found**: If the social group (or related entity) does not exist.
    """
    soc_groups_service: SocGroupsService = request.state.soc_groups_service

    soc_group_indicator_dto = await soc_groups_service.put_social_group_indicator_value(
        soc_group_id, soc_group_indicator
    )

    return SocGroupIndicatorValue.from_dto(soc_group_indicator_dto)


@soc_groups_router.delete(
    "/social_groups/{soc_group_id}/indicators",
    response_model=OkResponse,
    status_code=status.HTTP_200_OK,
)
async def delete_social_group_indicator_value(
    request: Request,
    soc_group_id: int = Path(..., description="social group identifier", gt=0),
    soc_value_id: int | None = Query(None, description="social value identifier", gt=0),
    territory_id: int | None = Query(None, description="territory identifier", gt=0),
    year: int | None = Query(None, description="year when value was modeled (skip to get values for all years)", gt=0),
    value_type: ValueType = Query(None, description="value type"),
) -> OkResponse:
    """
    ## Delete social group's indicator value by given parameters.

    ### Parameters:
    - **soc_group_id** (int, Path): Social group identifier.
    - **soc_value_id** (int, Query): Social value identifier.
    - **territory_id** (int, Query): Territory identifier.
    - **year** (int, Query): Specified year when value was modeled.
    - **value_type** (ValueType, Query): Value type (real, forecast or target).

    ### Returns:
    - **OkResponse**: A confirmation message of the deletion.

    ### Errors:
    - **404 Not Found**: If the social group's indicator value with such parameters does not exist.
    """
    soc_groups_service: SocGroupsService = request.state.soc_groups_service

    await soc_groups_service.delete_social_group_indicator_value_from_db(
        soc_group_id, soc_value_id, territory_id, year, value_type
    )

    return OkResponse()
