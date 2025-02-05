"""Unit tests for scenarios objects are defined here."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import delete, insert, literal, select, update

from idu_api.common.db.entities import (
    functional_zone_types_dict,
    projects_data,
    projects_functional_zones,
    projects_indicators_data,
    projects_object_geometries_data,
    projects_physical_objects_data,
    projects_services_data,
    projects_urban_objects_data,
    scenarios_data,
    territories_data,
)
from idu_api.urban_api.dto import ScenarioDTO
from idu_api.urban_api.exceptions.logic.common import EntityNotFoundById
from idu_api.urban_api.logic.impl.helpers.projects_scenarios import (
    copy_geometries,
    copy_physical_objects,
    copy_scenario_to_db,
    copy_services,
    delete_scenario_from_db,
    get_scenario_by_id_from_db,
    get_scenarios_by_project_id_from_db,
    patch_scenario_to_db,
    put_scenario_to_db,
)
from idu_api.urban_api.schemas import Scenario, ScenarioPatch, ScenarioPost, ScenarioPut
from tests.urban_api.helpers.connection import MockConnection

####################################################################################
#                           Default use-case tests                                 #
####################################################################################


@pytest.mark.asyncio
@patch("idu_api.urban_api.logic.impl.helpers.projects_scenarios.check_project")
async def test_get_scenarios_by_project_id_from_db(mock_check: AsyncMock, mock_conn: MockConnection):
    """Test the get_scenarios_by_project_id_from_db function."""

    # Arrange
    project_id = 1
    user_id = "mock_string"
    scenarios_data_parents = scenarios_data.alias("scenarios_data_parents")
    statement = (
        select(
            scenarios_data,
            scenarios_data_parents.c.name.label("parent_name"),
            projects_data.c.name.label("project_name"),
            projects_data.c.user_id.label("project_user_id"),
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            functional_zone_types_dict.c.name.label("functional_zone_type_name"),
            functional_zone_types_dict.c.zone_nickname.label("functional_zone_type_nickname"),
        )
        .select_from(
            scenarios_data.join(projects_data, projects_data.c.project_id == scenarios_data.c.project_id)
            .join(territories_data, territories_data.c.territory_id == projects_data.c.territory_id)
            .outerjoin(
                functional_zone_types_dict,
                functional_zone_types_dict.c.functional_zone_type_id == scenarios_data.c.functional_zone_type_id,
            )
            .outerjoin(
                scenarios_data_parents,
                scenarios_data.c.parent_id == scenarios_data_parents.c.scenario_id,
            )
        )
        .where(scenarios_data.c.project_id == project_id)
    )

    # Act
    result = await get_scenarios_by_project_id_from_db(mock_conn, project_id, user_id)

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(isinstance(item, ScenarioDTO) for item in result), "Each item should be a ScenarioDTO."
    assert isinstance(Scenario.from_dto(result[0]), Scenario), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))
    mock_check.assert_called_once_with(mock_conn, project_id, user_id)


@pytest.mark.asyncio
async def test_get_scenario_by_id_from_db(mock_conn: MockConnection):
    """Test the get_scenario_by_id_from_db function."""

    # Arrange
    scenario_id = 1
    user_id = "mock_string"
    scenarios_data_parents = scenarios_data.alias("scenarios_data_parents")
    statement = (
        select(
            scenarios_data,
            scenarios_data_parents.c.name.label("parent_name"),
            projects_data.c.name.label("project_name"),
            projects_data.c.user_id.label("project_user_id"),
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            functional_zone_types_dict.c.name.label("functional_zone_type_name"),
            functional_zone_types_dict.c.zone_nickname.label("functional_zone_type_nickname"),
        )
        .select_from(
            scenarios_data.join(projects_data, projects_data.c.project_id == scenarios_data.c.project_id)
            .join(territories_data, territories_data.c.territory_id == projects_data.c.territory_id)
            .outerjoin(
                functional_zone_types_dict,
                functional_zone_types_dict.c.functional_zone_type_id == scenarios_data.c.functional_zone_type_id,
            )
            .outerjoin(
                scenarios_data_parents,
                scenarios_data.c.parent_id == scenarios_data_parents.c.scenario_id,
            )
        )
        .where(scenarios_data.c.scenario_id == scenario_id)
    )

    # Act
    result = await get_scenario_by_id_from_db(mock_conn, scenario_id, user_id)

    # Assert
    assert isinstance(result, ScenarioDTO), "Result should be a ScenarioDTO."
    assert isinstance(Scenario.from_dto(result), Scenario), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))


@pytest.mark.asyncio
async def test_copy_scenario_to_db(mock_conn: MockConnection, scenario_post_req: ScenarioPost):
    """Test the copy_scenario_to_db function."""

    # Arrange
    async def check_project(conn, table, conditions=None):
        if table == projects_data:
            return False
        return True

    async def check_functional_zone_type(conn, table, conditions=None):
        if table == functional_zone_types_dict:
            return False
        return True

    scenario_id = 1
    user_id = "mock_string"
    project = {"project_id": 1, "territory_id": 1}
    parent_scenario_statement = (
        select(scenarios_data.c.scenario_id)
        .select_from(scenarios_data.join(projects_data, projects_data.c.project_id == scenarios_data.c.project_id))
        .where(
            projects_data.c.territory_id == project["territory_id"],
            projects_data.c.is_regional.is_(True),
            scenarios_data.c.is_based.is_(True),
        )
    )
    insert_scenario_statement = (
        insert(scenarios_data)
        .values(**scenario_post_req.model_dump(), parent_id=1, is_based=False)
        .returning(scenarios_data.c.scenario_id)
    )
    urban_objects_statement = select(
        projects_urban_objects_data.c.public_urban_object_id,
        projects_urban_objects_data.c.object_geometry_id,
        projects_urban_objects_data.c.physical_object_id,
        projects_urban_objects_data.c.service_id,
        projects_urban_objects_data.c.public_object_geometry_id,
        projects_urban_objects_data.c.public_physical_object_id,
        projects_urban_objects_data.c.public_service_id,
    ).where(projects_urban_objects_data.c.scenario_id == scenario_id)
    insert_urban_objects_statement = insert(projects_urban_objects_data).values(
        [
            {
                "scenario_id": 1,
                "object_geometry_id": None,
                "physical_object_id": None,
                "service_id": None,
                "public_object_geometry_id": None,
                "public_physical_object_id": None,
                "public_service_id": None,
                "public_urban_object_id": 1,
            }
        ]
    )
    insert_functional_zones_statement = insert(projects_functional_zones).from_select(
        [
            projects_functional_zones.c.scenario_id,
            projects_functional_zones.c.name,
            projects_functional_zones.c.functional_zone_type_id,
            projects_functional_zones.c.geometry,
            projects_functional_zones.c.year,
            projects_functional_zones.c.source,
            projects_functional_zones.c.properties,
        ],
        select(
            literal(1).label("scenario_id"),
            projects_functional_zones.c.name,
            projects_functional_zones.c.functional_zone_type_id,
            projects_functional_zones.c.geometry,
            projects_functional_zones.c.year,
            projects_functional_zones.c.source,
            projects_functional_zones.c.properties,
        ).where(projects_functional_zones.c.scenario_id == scenario_id),
    )
    insert_indicators_statement = insert(projects_indicators_data).from_select(
        [
            projects_indicators_data.c.scenario_id,
            projects_indicators_data.c.indicator_id,
            projects_indicators_data.c.territory_id,
            projects_indicators_data.c.hexagon_id,
            projects_indicators_data.c.value,
            projects_indicators_data.c.comment,
            projects_indicators_data.c.information_source,
            projects_indicators_data.c.properties,
        ],
        select(
            literal(1).label("scenario_id"),
            projects_indicators_data.c.indicator_id,
            projects_indicators_data.c.territory_id,
            projects_indicators_data.c.hexagon_id,
            projects_indicators_data.c.value,
            projects_indicators_data.c.comment,
            projects_indicators_data.c.information_source,
            projects_indicators_data.c.properties,
        ).where(projects_indicators_data.c.scenario_id == scenario_id),
    )

    # Act
    with patch(
        "idu_api.urban_api.logic.impl.helpers.projects_scenarios.check_existence",
        new=AsyncMock(side_effect=check_project),
    ):
        with pytest.raises(EntityNotFoundById):
            await copy_scenario_to_db(mock_conn, scenario_post_req, scenario_id, user_id)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.projects_scenarios.check_existence",
        new=AsyncMock(side_effect=check_functional_zone_type),
    ):
        with pytest.raises(EntityNotFoundById):
            await copy_scenario_to_db(mock_conn, scenario_post_req, scenario_id, user_id)
    result = await copy_scenario_to_db(mock_conn, scenario_post_req, scenario_id, user_id)

    # Assert
    assert isinstance(result, ScenarioDTO), "Result should be a ScenarioDTO."
    assert isinstance(Scenario.from_dto(result), Scenario), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(parent_scenario_statement))
    mock_conn.execute_mock.assert_any_call(str(insert_scenario_statement))
    mock_conn.execute_mock.assert_any_call(str(urban_objects_statement))
    mock_conn.execute_mock.assert_any_call(str(insert_urban_objects_statement))
    mock_conn.execute_mock.assert_any_call(str(insert_functional_zones_statement))
    mock_conn.execute_mock.assert_any_call(str(insert_indicators_statement))
    mock_conn.commit_mock.assert_called_once()


@pytest.mark.asyncio
async def test_put_scenario_to_db(mock_conn: MockConnection, scenario_put_req: ScenarioPut):
    """Test the put_scenario_to_db function."""

    # Arrange
    scenario_id = 1
    user_id = "mock_string"
    check_statement = (
        select(scenarios_data, projects_data.c.project_id, projects_data.c.user_id)
        .select_from(scenarios_data.join(projects_data, projects_data.c.project_id == scenarios_data.c.project_id))
        .where(scenarios_data.c.scenario_id == scenario_id)
    )
    update_statement = (
        update(scenarios_data)
        .where(scenarios_data.c.scenario_id == scenario_id)
        .values(**scenario_put_req.model_dump(), updated_at=datetime.now(timezone.utc))
    )
    not_based_scenario = ScenarioPut(**scenario_put_req.model_dump(exclude={"is_based"}), is_based=False)

    # Act
    with pytest.raises(ValueError):
        await put_scenario_to_db(mock_conn, not_based_scenario, scenario_id, user_id)
    result = await put_scenario_to_db(mock_conn, scenario_put_req, scenario_id, user_id)

    # Assert
    assert isinstance(result, ScenarioDTO), "Result should be a ScenarioDTO."
    assert isinstance(Scenario.from_dto(result), Scenario), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(check_statement))
    mock_conn.execute_mock.assert_any_call(str(update_statement))


@pytest.mark.asyncio
async def test_patch_scenario_to_db(mock_conn: MockConnection, scenario_patch_req: ScenarioPatch):
    """Test the patch_scenario_to_db function."""

    # Arrange
    scenario_id = 1
    user_id = "mock_string"
    check_statement = (
        select(scenarios_data, projects_data.c.project_id, projects_data.c.user_id)
        .select_from(scenarios_data.join(projects_data, projects_data.c.project_id == scenarios_data.c.project_id))
        .where(scenarios_data.c.scenario_id == scenario_id)
    )
    update_statement = (
        update(scenarios_data)
        .where(scenarios_data.c.scenario_id == scenario_id)
        .values(**scenario_patch_req.model_dump(exclude_unset=True), updated_at=datetime.now(timezone.utc))
    )
    not_based_scenario = ScenarioPatch(
        **scenario_patch_req.model_dump(exclude={"is_based"}, exclude_unset=True), is_based=False
    )

    # Act
    with pytest.raises(ValueError):
        await patch_scenario_to_db(mock_conn, not_based_scenario, scenario_id, user_id)
    result = await patch_scenario_to_db(mock_conn, scenario_patch_req, scenario_id, user_id)

    # Assert
    assert isinstance(result, ScenarioDTO), "Result should be a ScenarioDTO."
    assert isinstance(Scenario.from_dto(result), Scenario), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(check_statement))
    mock_conn.execute_mock.assert_any_call(str(update_statement))


@pytest.mark.asyncio
@patch("idu_api.urban_api.logic.impl.helpers.projects_scenarios.check_scenario")
async def test_delete_scenario_from_db(mock_check: AsyncMock, mock_conn: MockConnection):
    """Test the delete_scenario_from_db function."""

    # Arrange
    scenario_id = 1
    user_id = "mock_string"
    delete_geometry_statement = delete(projects_object_geometries_data).where(
        projects_object_geometries_data.c.object_geometry_id.in_([1])
    )
    delete_physical_statement = delete(projects_physical_objects_data).where(
        projects_physical_objects_data.c.physical_object_id.in_([1])
    )
    delete_service_statement = delete(projects_services_data).where(projects_services_data.c.service_id.in_([1]))
    delete_statement = delete(scenarios_data).where(scenarios_data.c.scenario_id == scenario_id)

    # Act
    result = await delete_scenario_from_db(mock_conn, scenario_id, user_id)

    # Assert
    assert result == {"status": "ok"}, "Result should be {'status': 'ok'}."
    mock_conn.execute_mock.assert_any_call(str(delete_geometry_statement))
    mock_conn.execute_mock.assert_any_call(str(delete_physical_statement))
    mock_conn.execute_mock.assert_any_call(str(delete_service_statement))
    mock_conn.execute_mock.assert_any_call(str(delete_statement))
    mock_conn.commit_mock.assert_called_once()
    mock_check.assert_called_once_with(mock_conn, scenario_id, user_id, to_edit=True)


@pytest.mark.asyncio
async def test_copy_geometries(mock_conn: MockConnection):
    """Test the copy_geometries function."""

    # Arrange
    geometry_ids = [1]
    select_statement = (
        select(
            projects_object_geometries_data.c.public_object_geometry_id,
            projects_object_geometries_data.c.territory_id,
            projects_object_geometries_data.c.address,
            projects_object_geometries_data.c.osm_id,
            projects_object_geometries_data.c.geometry,
            projects_object_geometries_data.c.centre_point,
        )
        .where(projects_object_geometries_data.c.object_geometry_id.in_(geometry_ids))
        .order_by(projects_object_geometries_data.c.object_geometry_id)
    )
    result = (await mock_conn.execute(select_statement)).mappings().all()
    insert_statement = (
        insert(projects_object_geometries_data)
        .values([dict(row) for row in result])
        .returning(projects_object_geometries_data.c.object_geometry_id)
    )

    # Act
    result = await copy_geometries(mock_conn, geometry_ids)

    # Assert
    assert isinstance(result, dict), "Result should be dictionary."
    assert result == {1: 1}, "Expected result not found."
    mock_conn.execute_mock.assert_any_call(str(select_statement))
    mock_conn.execute_mock.assert_any_call(str(insert_statement))


@pytest.mark.asyncio
async def test_copy_physical_objects(mock_conn: MockConnection):
    """Test the copy_physical_objects function."""

    # Arrange
    physical_ids = [1]
    select_statement = (
        select(
            projects_physical_objects_data.c.public_physical_object_id,
            projects_physical_objects_data.c.physical_object_type_id,
            projects_physical_objects_data.c.name,
            projects_physical_objects_data.c.properties,
        )
        .where(projects_physical_objects_data.c.physical_object_id.in_(physical_ids))
        .order_by(projects_physical_objects_data.c.physical_object_id)
    )
    result = (await mock_conn.execute(select_statement)).mappings().all()
    insert_statement = (
        insert(projects_physical_objects_data)
        .values([dict(row) for row in result])
        .returning(projects_physical_objects_data.c.physical_object_id)
    )

    # Act
    result = await copy_physical_objects(mock_conn, physical_ids)

    # Assert
    assert isinstance(result, dict), "Result should be dictionary."
    assert result == {1: 1}, "Expected result not found."
    mock_conn.execute_mock.assert_any_call(str(select_statement))
    mock_conn.execute_mock.assert_any_call(str(insert_statement))


@pytest.mark.asyncio
async def test_copy_services(mock_conn: MockConnection):
    """Test the copy_services function."""

    # Arrange
    service_ids = [1]
    select_statement = (
        select(
            projects_services_data.c.public_service_id,
            projects_services_data.c.service_type_id,
            projects_services_data.c.name,
            projects_services_data.c.capacity_real,
            projects_services_data.c.properties,
        )
        .where(projects_services_data.c.service_id.in_(service_ids))
        .order_by(projects_services_data.c.service_id)
    )
    result = (await mock_conn.execute(select_statement)).mappings().all()
    insert_statement = (
        insert(projects_services_data)
        .values([dict(row) for row in result])
        .returning(projects_services_data.c.service_id)
    )

    # Act
    result = await copy_services(mock_conn, service_ids)

    # Assert
    assert isinstance(result, dict), "Result should be dictionary."
    assert result == {1: 1}, "Expected result not found."
    mock_conn.execute_mock.assert_any_call(str(select_statement))
    mock_conn.execute_mock.assert_any_call(str(insert_statement))
