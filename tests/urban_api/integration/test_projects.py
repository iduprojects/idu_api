import aiohttp
import pytest


@pytest.mark.asyncio
async def test_get_all_projects(urban_api_host: str, expired_auth_token: str):
    """Test GET to return list and status code 200."""

    # Arrange
    headers = {"Authorization": f"Bearer {expired_auth_token}"}

    # Act
    async with aiohttp.ClientSession(base_url=f"{urban_api_host}/api/v1/") as session:
        response = await session.get("projects", headers=headers)
        result = await response.json()

    # Assert
    assert response.status == 200
    assert isinstance(result, dict)
    assert "count" in result
    assert "prev" in result
    assert "next" in result
    assert "results" in result
