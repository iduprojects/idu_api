"""All fixtures for projects tests are defined here."""

import io

import pytest
from PIL import Image

from idu_api.urban_api.schemas import ProjectPatch, ProjectPost, ProjectPut, ProjectTerritoryPost
from idu_api.urban_api.schemas.geometries import Geometry

__all__ = [
    "project_image",
    "project_patch_req",
    "project_post_req",
    "project_put_req",
]


@pytest.fixture
def project_post_req() -> ProjectPost:
    """POST request template for user projects data."""

    return ProjectPost(
        name="Test Project Name",
        territory_id=1,
        description="Test Project Description",
        public=True,
        is_city=False,
        territory=ProjectTerritoryPost(
            geometry=Geometry(
                type="Polygon",
                coordinates=[[[30.22, 59.86], [30.22, 59.85], [30.25, 59.85], [30.25, 59.86], [30.22, 59.86]]],
            ),
        ),
    )


@pytest.fixture
def project_put_req() -> ProjectPut:
    """PUT request template for user projects data."""

    return ProjectPut(
        name="Updated Test Project Name",
        description="Updated Test Project Description",
        public=True,
        properties={},
    )


@pytest.fixture
def project_patch_req() -> ProjectPatch:
    """PATCH request template for user projects data."""

    return ProjectPatch(name="New Patched Project Name")


@pytest.fixture
def project_image() -> io.BytesIO:
    """Get simple project image bytes array."""

    img = Image.new("RGB", (60, 30), color="red")
    img_byte_arr = io.BytesIO()
    img.save(img_byte_arr, format="PNG")

    return img_byte_arr.getvalue()
