"""Unit tests for internal logic helper functions are defined here."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, Mock, patch

import pytest
from geoalchemy2.functions import ST_GeomFromWKB, ST_Union
from sqlalchemy import select, text
from sqlalchemy.sql.selectable import CTE, ScalarSelect, Select

from idu_api.common.db.entities import projects_data, territories_data
from idu_api.urban_api.dto import UserDTO
from idu_api.urban_api.exceptions.logic.projects import NotAllowedInRegionalScenario
from idu_api.urban_api.logic.impl.helpers.utils import (
    SRID,
    build_hierarchy,
    build_recursive_query,
    check_existence,
    extract_values_from_model,
    get_context_territories_geometry,
    include_child_territories_cte,
)
from idu_api.urban_api.schemas import TerritoryPatch, TerritoryPost, TerritoryPut
from tests.urban_api.helpers.connection import MockConnection, MockResult, MockRow


@pytest.mark.asyncio
async def test_check_existence(mock_conn: MockConnection):
    """Test the check_existence function."""

    # Arrange
    table = territories_data
    conditions = {"parent_id": 1}
    not_conditions = {"territory_id": 1}
    statement = select(table).where(territories_data.c.parent_id == 1, territories_data.c.territory_id != 1).limit(1)

    # Act
    result = await check_existence(mock_conn, table, conditions, not_conditions)

    # Assert
    assert isinstance(result, bool), "Result should be a boolean."
    mock_conn.execute_mock.assert_any_call(str(statement))


def test_build_recursive_query():
    """Test the build_recursive_query function."""

    # Arrange
    table = territories_data
    parent_id = 1
    cte_name = "recursive_cte"
    primary_column = "territory_id"
    parent_column = "parent_id"
    base_statement = select(table)

    base_cte = select(table).where(table.c.parent_id == parent_id)
    cte_statement = base_cte.cte(name=cte_name, recursive=True)
    recursive_part = base_statement.join(cte_statement, table.c.parent_id == cte_statement.c.territory_id)
    final_query = select(cte_statement.union_all(recursive_part))

    # Act
    result = build_recursive_query(base_statement, table, parent_id, cte_name, primary_column, parent_column)

    # Assert
    assert isinstance(result, Select), "Result should be a SQLAlchemy Select object."
    assert str(result) == str(final_query), "Expected result not found."


def test_include_child_territories_cte():
    """Test the include_child_territories_cte function."""

    # Arrange
    territory_id = 1
    cities_only = True
    base_cte = (
        select(territories_data.c.territory_id, territories_data.c.is_city)
        .where(territories_data.c.territory_id == territory_id)
        .cte(name="recursive_cte", recursive=True)
    )
    recursive_query = select(territories_data.c.territory_id, territories_data.c.is_city).where(
        territories_data.c.parent_id == base_cte.c.territory_id
    )
    recursive_cte = base_cte.union_all(recursive_query)
    filtered_cte = (
        select(recursive_cte.c.territory_id).where(recursive_cte.c.is_city.is_(True)).cte(name="filtered_cte")
    )

    # Act
    result = include_child_territories_cte(territory_id, cities_only)

    # Assert
    assert isinstance(result, CTE), "Result should be a SQLAlchemy CTE object."
    assert str(result) == str(filtered_cte), "Expected result not found."


def test_extract_values_from_model(
    territory_post_req: TerritoryPost,
    territory_put_req: TerritoryPut,
    territory_patch_req: TerritoryPatch,
):
    """Test the extract_values_from_model function."""

    # Arrange
    expected_post_result = territory_post_req.model_dump()
    expected_put_result = territory_put_req.model_dump()
    expected_patch_result = territory_patch_req.model_dump(exclude_unset=True)
    for field in ("geometry", "centre_point"):
        expected_post_result[field] = ST_GeomFromWKB(
            getattr(territory_post_req, field).as_shapely_geometry().wkb, text(str(SRID))
        )
        expected_put_result[field] = ST_GeomFromWKB(
            getattr(territory_put_req, field).as_shapely_geometry().wkb, text(str(SRID))
        )
    expected_put_result["updated_at"] = datetime.now(timezone.utc)
    expected_patch_result["updated_at"] = datetime.now(timezone.utc)

    # Act
    post_result = extract_values_from_model(territory_post_req, False, False)
    put_result = extract_values_from_model(territory_put_req, False, True)
    patch_result = extract_values_from_model(territory_patch_req, True, True)

    # Assert
    assert isinstance(post_result, dict), "Result should be a dictionary."
    assert isinstance(put_result, dict), "Result should be a dictionary."
    assert isinstance(patch_result, dict), "Result should be a dictionary."
    assert post_result.keys() == expected_post_result.keys(), "Expected set of keys not found."
    assert put_result.keys() == expected_put_result.keys(), "Expected set of keys not found."
    assert patch_result.keys() == expected_patch_result.keys(), "Expected set of keys not found."
    for field in ("geometry", "centre_point"):
        if field in post_result:
            assert str(post_result[field]) == str(expected_post_result[field]), f"Post {field} mismatch."
        if field in put_result:
            assert str(put_result[field]) == str(expected_put_result[field]), f"Put {field} mismatch."
    for key in expected_post_result:
        if key not in ("geometry", "centre_point", "updated_at"):
            assert post_result[key] == expected_post_result[key], f"Post {key} mismatch."
    for key in expected_put_result:
        if key not in ("geometry", "centre_point", "updated_at"):
            assert put_result[key] == expected_put_result[key], f"Put {key} mismatch."
    for key in expected_patch_result:
        if key != "updated_at":
            assert patch_result[key] == expected_patch_result[key], f"Patch {key} mismatch."


@pytest.mark.asyncio
async def test_get_all_context_territories(mock_conn: MockConnection):
    """Test the get_all_context_territories function."""

    # Arrange
    project_id = 1
    user = UserDTO("mock_string", is_superuser=False)
    statement = select(projects_data).where(projects_data.c.project_id == project_id)
    unified_geometry = (
        select(ST_Union(territories_data.c.geometry).label("geometry"))
        .where(territories_data.c.territory_id.in_([1]))
        .scalar_subquery()
    )

    # Act
    with pytest.raises(NotAllowedInRegionalScenario):
        await get_context_territories_geometry(mock_conn, project_id, user)
    with patch("tests.urban_api.helpers.connection.mock_conn") as new_mock_conn:
        new_mock_conn.execute = AsyncMock(
            return_value=MockResult(
                [
                    MockRow(
                        **{
                            "user_id": "mock_string",
                            "public": True,
                            "is_regional": False,
                            "properties": {"context": [1]},
                        }
                    )
                ]
            )
        )
        result = await get_context_territories_geometry(new_mock_conn, project_id, user)

    # Assert
    assert isinstance(result, tuple), "Result should be a dictionary."
    assert isinstance(
        result[0], ScalarSelect
    ), "The first item in result should be a ScalarSelect object (unified geometry)."
    assert str(result[0]) == str(unified_geometry), "The ScalarSelect should be a unified geometry."
    assert isinstance(result[1], list), "The second item in result should be a list of context identifiers."
    assert all(isinstance(idx, int) for idx in result[1]), "Each item in list of identifiers should be an integer."
    mock_conn.execute_mock.assert_any_call(str(statement))


def test_build_hierarchy(sample_dtos, expected_hierarchy):
    """Test the build_hierarchy function."""

    # Arrange
    input_dtos = sample_dtos
    output_model = expected_hierarchy["output_model"]
    expected_result = expected_hierarchy["expected_result"]

    # Act
    result = build_hierarchy(input_dtos, output_model)

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(isinstance(item, output_model) for item in result), "All items should be instances of the output model."

    def serialize(obj):
        """Helper to recursively serialize hierarchy to dicts for easy comparison."""
        data = obj.__dict__.copy()
        data["children"] = [serialize(child) for child in getattr(obj, "children", [])]
        return data

    actual_serialized = [serialize(item) for item in result]
    expected_serialized = [serialize(item) for item in expected_result]

    assert actual_serialized == expected_serialized, "Hierarchy structure does not match expected result."
