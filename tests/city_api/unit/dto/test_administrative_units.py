import pytest

from idu_api.city_api.dto.administrative_units import AdministrativeUnitsDTO
from idu_api.urban_api.dto import TerritoryDTO
from shapely import Polygon, Point
from datetime import datetime
import copy as cp

territory_dto_init = TerritoryDTO(
    1,
    1,
    "type1",
    1,
    1,
    "name1",
    Polygon([(0, 0), (1, 1), (1, 0)]),
    3,
    {"a": "b"},
    Point(0, 2),
    0,
    "123",
    datetime.now(),
    datetime.now()
)


@pytest.mark.asyncio
async def test_init_empty_administrative_unit():
    administrative_unit_dto: AdministrativeUnitsDTO = AdministrativeUnitsDTO()
    assert hasattr(administrative_unit_dto, "id") and getattr(administrative_unit_dto, "id") is None
    assert hasattr(administrative_unit_dto, "name") and getattr(administrative_unit_dto, "name") is None
    assert hasattr(administrative_unit_dto, "geometry") and getattr(administrative_unit_dto, "geometry") is None
    assert hasattr(administrative_unit_dto, "center") and getattr(administrative_unit_dto, "center") is None
    assert hasattr(administrative_unit_dto, "type") and getattr(administrative_unit_dto, "type") is None
    # TODO: better approach


@pytest.mark.asyncio
async def test_init_administrative_unit_with_data():
    administrative_unit_dto: AdministrativeUnitsDTO = AdministrativeUnitsDTO(
        id=1,
        name="name1",
        geometry=Polygon([(0, 0), (1, 1), (1, 0)]),
        center=Point(0, 2),
        type="type1",
    )
    assert hasattr(administrative_unit_dto, "id") and getattr(administrative_unit_dto, "id") == 1
    assert hasattr(administrative_unit_dto, "name") and getattr(administrative_unit_dto, "name") == "name1"
    assert hasattr(administrative_unit_dto, "geometry") \
           and getattr(administrative_unit_dto, "geometry") == Polygon([(0, 0), (1, 1), (1, 0)])
    assert hasattr(administrative_unit_dto, "center") and getattr(administrative_unit_dto, "center") == Point(0, 2)
    assert hasattr(administrative_unit_dto, "type") and getattr(administrative_unit_dto, "type") == "type1"


@pytest.mark.asyncio
async def test_map_administrative_unit_from_territory_dto_with_empty_exclude():
    """
    testing class mapper between DTOs
    """

    territory_dto = cp.deepcopy(territory_dto_init)
    assert territory_dto_init == territory_dto

    administrative_unit_dto: AdministrativeUnitsDTO = AdministrativeUnitsDTO()
    await administrative_unit_dto.map_from_territory_dto(
        territory_dto.__dict__,
        {
            "territory_id": "id",
            "territory_type_name": "type",
            "centre_point": "center"
        },
        []
    )
    administrative_unit_dto1: AdministrativeUnitsDTO = AdministrativeUnitsDTO(
        id=1,
        name="name1",
        geometry=Polygon([(0, 0), (1, 1), (1, 0)]),
        center=Point(0, 2),
        type="type1",
    )
    assert administrative_unit_dto == administrative_unit_dto1


@pytest.mark.asyncio
async def test_map_administrative_unit_with_none_exclude():
    """
    testing class mapper between DTOs with none exclude
    """

    territory_dto = cp.deepcopy(territory_dto_init)
    assert territory_dto_init == territory_dto

    administrative_unit_dto: AdministrativeUnitsDTO = AdministrativeUnitsDTO()
    await administrative_unit_dto.map_from_territory_dto(
        territory_dto.__dict__,
        {
            "territory_id": "id",
            "territory_type_name": "type",
            "centre_point": "center"
        }
    )
    administrative_unit_dto1: AdministrativeUnitsDTO = AdministrativeUnitsDTO(
        id=1,
        name="name1",
        geometry=Polygon([(0, 0), (1, 1), (1, 0)]),
        center=Point(0, 2),
        type="type1",
    )
    assert administrative_unit_dto == administrative_unit_dto1


@pytest.mark.asyncio
async def test_map_administrative_units_with_none_argument_mapper():
    """
    testing class mapper between DTOs with none argument mapper
    """

    territory_dto = cp.deepcopy(territory_dto_init)
    assert territory_dto_init == territory_dto

    administrative_unit_dto: AdministrativeUnitsDTO = AdministrativeUnitsDTO()
    await administrative_unit_dto.map_from_territory_dto(
        territory_dto.__dict__,
    )
    administrative_unit_dto1: AdministrativeUnitsDTO = AdministrativeUnitsDTO(
        id=1,
        name="name1",
        geometry=Polygon([(0, 0), (1, 1), (1, 0)]),
        center=Point(0, 2),
        type="type1",
    )
    administrative_unit_dto2: AdministrativeUnitsDTO = AdministrativeUnitsDTO(
        id=None,
        name="name1",
        center=None,
        geometry=Polygon([(0, 0), (1, 1), (1, 0)]),
        type=None
    )
    assert administrative_unit_dto != administrative_unit_dto1
    assert administrative_unit_dto == administrative_unit_dto2


@pytest.mark.asyncio
async def test_map_administrative_units_with_exclude():
    """
    testing class mapper between DTOs with exclude
    """

    territory_dto = cp.deepcopy(territory_dto_init)
    assert territory_dto_init == territory_dto

    administrative_unit_dto: AdministrativeUnitsDTO = AdministrativeUnitsDTO()
    await administrative_unit_dto.map_from_territory_dto(
        territory_dto.__dict__,
        exclude=["name"]
    )
    administrative_unit_dto1: AdministrativeUnitsDTO = AdministrativeUnitsDTO(
        id=1,
        name="name1",
        geometry=Polygon([(0, 0), (1, 1), (1, 0)]),
        center=Point(0, 2),
        type="type1",
    )
    administrative_unit_dto2: AdministrativeUnitsDTO = AdministrativeUnitsDTO(
        id=None,
        name=None,
        center=None,
        geometry=Polygon([(0, 0), (1, 1), (1, 0)]),
        type=None
    )
    assert administrative_unit_dto != administrative_unit_dto1
    assert administrative_unit_dto == administrative_unit_dto2


@pytest.mark.asyncio
async def test_map_administrative_units_with_argument_mapper_and_exclude():
    """
    testing class mapper between DTOs with argument mapper and exclude
    """

    territory_dto = cp.deepcopy(territory_dto_init)
    assert territory_dto_init == territory_dto

    administrative_unit_dto: AdministrativeUnitsDTO = AdministrativeUnitsDTO()
    await administrative_unit_dto.map_from_territory_dto(
        territory_dto.__dict__,
        {
            "territory_id": "id",
            "territory_type_name": "type",
            "centre_point": "center"
        },
        ["name"]
    )
    administrative_unit_dto1: AdministrativeUnitsDTO = AdministrativeUnitsDTO(
        id=1,
        name="name1",
        geometry=Polygon([(0, 0), (1, 1), (1, 0)]),
        center=Point(0, 2),
        type="type1",
    )
    administrative_unit_dto2: AdministrativeUnitsDTO = AdministrativeUnitsDTO(
        id=1,
        name=None,
        center=Point(0, 2),
        geometry=Polygon([(0, 0), (1, 1), (1, 0)]),
        type="type1"
    )
    assert administrative_unit_dto != administrative_unit_dto1
    assert administrative_unit_dto == administrative_unit_dto2
