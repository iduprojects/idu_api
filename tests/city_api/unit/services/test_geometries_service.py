import pytest
from httpx import AsyncClient

from idu_api.city_api import app


@pytest.mark.asyncio
async def test_get_city_geometry():
    async with AsyncClient(app=app, base_url="http://localhost:8000/api") as client:
        response = await client.get("/city/3138/geometry")
    assert response.status_code == 200
    pre_result = response.json()
    assert type(pre_result) is dict
    assert all(elem in pre_result.keys() for elem in ["type", "coordinates"])
    assert pre_result["type"] == "MultiPolygon"
