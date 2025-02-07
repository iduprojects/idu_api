"""Unit tests for internal logic helper functions are defined here."""

from datetime import datetime, timezone

import pytest
from geoalchemy2.functions import ST_GeomFromText, ST_Union
from sqlalchemy import select, text
from sqlalchemy.sql.selectable import CTE, Select

from idu_api.common.db.entities import projects_data, territories_data
from idu_api.urban_api.logic.impl.helpers.utils import (
    build_recursive_query,
    check_existence,
    extract_values_from_model,
    get_all_context_territories,
    include_child_territories_cte,
)
from idu_api.urban_api.schemas import TerritoryPatch, TerritoryPost, TerritoryPut
from tests.urban_api.helpers.connection import MockConnection


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
    srid = "4326"
    expected_post_result = territory_post_req.model_dump()
    expected_put_result = territory_put_req.model_dump()
    expected_patch_result = territory_patch_req.model_dump(exclude_unset=True)
    for field in ("geometry", "centre_point"):
        expected_post_result[field] = ST_GeomFromText(
            getattr(territory_post_req, field).as_shapely_geometry().wkt, text(srid)
        )
        expected_put_result[field] = ST_GeomFromText(
            getattr(territory_put_req, field).as_shapely_geometry().wkt, text(srid)
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
    user_id = "mock_string"
    statement = select(projects_data.c.user_id, projects_data.c.public, projects_data.c.properties).where(
        projects_data.c.project_id == project_id
    )
    context_territories = (
        select(
            territories_data.c.territory_id,
            territories_data.c.geometry,
        )
        .where(territories_data.c.territory_id.in_([1]))
        .subquery()
    )
    unified_geometry = select(ST_Union(context_territories.c.geometry)).scalar_subquery()
    all_descendants = (
        select(
            territories_data.c.territory_id,
            territories_data.c.parent_id,
        )
        .where(territories_data.c.territory_id.in_(select(context_territories.c.territory_id)))
        .cte(name="all_descendants", recursive=True)
    )
    all_descendants = all_descendants.union_all(
        select(
            territories_data.c.territory_id,
            territories_data.c.parent_id,
        ).select_from(
            territories_data.join(
                all_descendants,
                territories_data.c.parent_id == all_descendants.c.territory_id,
            )
        )
    )
    all_ancestors = (
        select(
            territories_data.c.territory_id,
            territories_data.c.parent_id,
        )
        .where(territories_data.c.territory_id.in_(select(context_territories.c.territory_id)))
        .cte(name="all_ancestors", recursive=True)
    )
    all_ancestors = all_ancestors.union_all(
        select(
            territories_data.c.territory_id,
            territories_data.c.parent_id,
        ).select_from(
            territories_data.join(
                all_ancestors,
                territories_data.c.territory_id == all_ancestors.c.parent_id,
            )
        )
    )
    all_related_territories = (
        select(all_descendants.c.territory_id).union(select(all_ancestors.c.territory_id)).subquery()
    )

    # Act
    result = await get_all_context_territories(mock_conn, project_id, user_id)

    # Assert
    assert isinstance(result, dict), "Result should be a dictionary."
    assert "geometry" in result, "Unified geometry should be included."
    assert str(result["geometry"]) == str(unified_geometry), "Expected unified geometry not found."
    assert "territories" in result, "All related territories should be included."
    assert str(result["territories"]) == str(all_related_territories), "Expected territories subquery not found."
    mock_conn.execute_mock.assert_called_once_with(str(statement))
