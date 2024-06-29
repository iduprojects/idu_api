# pylint: disable=unused-argument
"""Normatives territories-related handlers are defined here."""

from fastapi import Path, Request
from starlette import status

from urban_api.schemas.normatives import Normative, NormativeDelete, NormativePatch, NormativePost, NormativeType
from urban_api.schemas.service_types import ServiceTypeBasic, UrbanFunctionBasic

from .routers import territories_router

get_mock = [
    Normative(
        service_type=ServiceTypeBasic(id=22, name="Школа"),
        radius_availability_meters=600,
        services_per_1000_normative=2,
        is_regulated=True,
        normative_type=NormativeType.SELF,
    ),
    Normative(
        service_type=ServiceTypeBasic(id=21, name="Детский сад"),
        radius_availability_meters=300,
        services_capacity_per_1000_normative=100,
        is_regulated=True,
        normative_type=NormativeType.PARENT,
    ),
    Normative(
        urban_function=UrbanFunctionBasic(id=2, name="Зеленые зоны"),
        time_availability_minutes=30,
        services_capacity_per_1000_normative=20,
        is_regulated=False,
        normative_type=NormativeType.GLOBAL,
    ),
]


@territories_router.get(
    "/territory/{territory_id}/normatives",
    response_model=list[Normative],
    status_code=status.HTTP_200_OK,
)
def get_territory_normatives(
    request: Request, territory_id: int = Path(description="territory id", gt=0)
) -> list[Normative]:
    """This is MOCK endpoint, it always returns the same data.
    Get territory normatives.
    """
    return get_mock


@territories_router.post(
    "/territory/{territory_id}/normatives",
    response_model=bool,
    status_code=status.HTTP_200_OK,
)
def post_territory_normatives(
    request: Request,
    normatives: list[NormativePost],
    territory_id: int = Path(description="territory id", gt=0),
) -> bool:
    """This is MOCK endpoint, it always returns the same data.
    Post batch of territory normatives. If at least one normative already exist,
    400 error is returned and none are added.
    """
    return True


@territories_router.put(
    "/territory/{territory_id}/normatives",
    response_model=bool,
    status_code=status.HTTP_200_OK,
)
def put_territory_normatives(
    request: Request,
    normatives: list[NormativePost],
    territory_id: int = Path(description="territory id", gt=0),
) -> bool:
    """This is MOCK endpoint, it always returns the same data.
    Post batch of territory normatives. If at least one of normatives does not exist, 400 error is returned and no
    normatives are updated.
    """
    return True


@territories_router.patch(
    "/territory/{territory_id}/normatives",
    response_model=bool,
    status_code=status.HTTP_200_OK,
)
def patch_territory_normatives(
    request: Request,
    normatives: list[NormativePatch],
    territory_id: int = Path(description="territory id", gt=0),
) -> bool:
    """This is MOCK endpoint, it always returns the same data.
    Patch batch of territory normatives. If at least one of normatives does not exist, 400 error is returned and no
    normatives are updated.
    """
    return True


@territories_router.delete(
    "/territory/{territory_id}/normatives",
    response_model=bool,
    status_code=status.HTTP_200_OK,
)
def delete_territory_normatives(
    request: Request,
    normatives: list[NormativeDelete],
    territory_id: int = Path(description="territory id", gt=0),
) -> bool:
    """This is MOCK endpoint, it always returns the same data.
    Delete batch of territory normatives. If at least one of normatives does not exist, 400 error is returned and no
    normatives are deleted.
    """
    return True
