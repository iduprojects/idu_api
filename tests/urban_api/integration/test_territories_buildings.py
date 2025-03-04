"""Integration tests for territory-related buildings are defined here."""

from typing import Any

import httpx
import pytest
from pydantic import ValidationError

from idu_api.urban_api.schemas import BuildingPost, BuildingWithGeometry, Page, PhysicalObjectWithGeometryPost
from tests.urban_api.helpers.utils import assert_response

####################################################################################
#                           Default use-case tests                                 #
####################################################################################


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, territory_id_param, version",
    [
        (200, None, None, "v1"),
        (200, None, None, "v2"),
        (400, "You can use cities_only parameter only with including child territories", None, "v1"),
        (404, "not found", 1e9, "v1"),
    ],
    ids=["success_v1", "success_v2", "bad_request", "not_found"],
)
async def test_get_buildings_with_geometry_by_territory_id(
    urban_api_host: str,
    physical_object_with_geometry_post_req: PhysicalObjectWithGeometryPost,
    building_post_req: BuildingPost,
    physical_object_type: dict[str, Any],
    region: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    territory_id_param: int | None,
    version: str,
):
    """Test GET /territory/{territory_id}/living_buildings_with_geometry method."""

    # Arrange
    new_object = physical_object_with_geometry_post_req.model_dump()
    new_object["physical_object_type_id"] = physical_object_type["physical_object_type_id"]
    new_object["territory_id"] = region["territory_id"]
    new_building = building_post_req.model_dump()
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.post("/physical_objects", json=new_object)
        new_building["physical_object_id"] = response.json()["physical_object"]["physical_object_id"]
        await client.post("/buildings", json=new_building)
    territory_id = territory_id_param or region["territory_id"]
    if expected_status == 400:
        params = {"include_child_territories": False, "cities_only": True, "page_size": 1}
    else:
        params = {"include_child_territories": True, "cities_only": False, "page_size": 1}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/{version}") as client:
        response = await client.get(f"/territory/{territory_id}/living_buildings_with_geometry", params=params)
        result = response.json()

    # Assert
    assert_response(response, expected_status, Page, error_message)
    if response.status_code == 200:
        assert len(result["results"]) > 0, "Response should contain at least one item."
        assert (
            len(result["results"]) <= params["page_size"]
        ), f"Response should contain no more than {params['page_size']} items."
        try:
            BuildingWithGeometry(**result["results"][0])
        except ValidationError as e:
            pytest.fail(f"Pydantic validation error: {str(e)}")
