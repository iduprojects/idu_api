import pytest
from idu_api.city_api import app
from httpx import AsyncClient

from idu_api.city_api.schemas.adminstrative_units import AdministrativeUnitsData


@pytest.mark.asyncio
async def test_get_administrative_units_by_city_id():
    async with AsyncClient(app=app, base_url="http://localhost:8000/api") as client:
        response = await client.get("/city/4443/administrative_units")
    assert response.status_code == 200
    pre_result = response.json()
    assert type(pre_result) == list
    assert len(pre_result) > 0 and type(pre_result[0]) == dict\
           and all(elem in pre_result[0].keys() for elem in ["id", "name", "center"])
    result: list[AdministrativeUnitsData] = [AdministrativeUnitsData(**elem) for elem in pre_result]
    assert len(pre_result) == len(result)
    assert pre_result[0]["id"] == result[0].id
