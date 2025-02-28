"""Unit tests for scenario functional zone objects are defined here."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest
from geoalchemy2.functions import ST_AsEWKB, ST_GeomFromWKB, ST_Intersection, ST_Intersects, ST_Within
from sqlalchemy import case, delete, insert, select, text, update

from idu_api.common.db.entities import (
    functional_zone_types_dict,
    functional_zones_data,
    projects_functional_zones,
    scenarios_data,
    territories_data,
)
from idu_api.urban_api.dto import (
    FunctionalZoneDTO,
    FunctionalZoneSourceDTO,
    ScenarioFunctionalZoneDTO,
)
from idu_api.urban_api.exceptions.logic.common import EntitiesNotFoundByIds, EntityNotFoundById, TooManyObjectsError
from idu_api.urban_api.logic.impl.helpers.projects_functional_zones import (
    add_scenario_functional_zones_to_db,
    delete_functional_zones_by_scenario_id_from_db,
    get_context_functional_zones_from_db,
    get_context_functional_zones_sources_from_db,
    get_functional_zone_by_ids,
    get_functional_zones_by_scenario_id_from_db,
    get_functional_zones_sources_by_scenario_id_from_db,
    patch_scenario_functional_zone_to_db,
    put_scenario_functional_zone_to_db,
)
from idu_api.urban_api.logic.impl.helpers.utils import (
    OBJECTS_NUMBER_LIMIT,
    SRID,
    extract_values_from_model,
    get_context_territories_geometry,
)
from idu_api.urban_api.schemas import (
    FunctionalZone,
    FunctionalZoneSource,
    ScenarioFunctionalZone,
    ScenarioFunctionalZonePatch,
    ScenarioFunctionalZonePut,
)
from tests.urban_api.helpers.connection import MockConnection

####################################################################################
#                           Default use-case tests                                 #
####################################################################################


@pytest.mark.asyncio
@patch("idu_api.urban_api.logic.impl.helpers.projects_functional_zones.check_scenario")
async def test_get_functional_zones_sources_by_scenario_id_from_db(mock_check: AsyncMock, mock_conn: MockConnection):
    """Test the get_functional_zones_sources_by_scenario_id_from_db function."""

    # Arrange
    scenario_id = 1
    user_id = "mock_string"
    statement = (
        select(projects_functional_zones.c.year, projects_functional_zones.c.source)
        .where(projects_functional_zones.c.scenario_id == scenario_id)
        .distinct()
    )

    # Act
    result = await get_functional_zones_sources_by_scenario_id_from_db(mock_conn, scenario_id, user_id)

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(
        isinstance(item, FunctionalZoneSourceDTO) for item in result
    ), "Each item should be a FunctionalZoneSourceDTO."
    assert isinstance(
        FunctionalZoneSource.from_dto(result[0]), FunctionalZoneSource
    ), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))
    mock_check.assert_called_once_with(mock_conn, scenario_id, user_id)


@pytest.mark.asyncio
@patch("idu_api.urban_api.logic.impl.helpers.projects_functional_zones.check_scenario")
async def test_get_functional_zones_by_scenario_id_from_db(mock_check: AsyncMock, mock_conn: MockConnection):
    """Test the get_functional_zones_by_scenario_id_from_db function."""

    # Arrange
    scenario_id = 1
    year = datetime.today().year
    source = "mock_sting"
    functional_zone_type_id = 1
    user_id = "mock_sting"
    statement = (
        select(
            projects_functional_zones.c.functional_zone_id,
            projects_functional_zones.c.scenario_id,
            scenarios_data.c.name.label("scenario_name"),
            projects_functional_zones.c.functional_zone_type_id,
            functional_zone_types_dict.c.name.label("functional_zone_type_name"),
            functional_zone_types_dict.c.zone_nickname.label("functional_zone_type_nickname"),
            projects_functional_zones.c.name,
            ST_AsEWKB(projects_functional_zones.c.geometry).label("geometry"),
            projects_functional_zones.c.year,
            projects_functional_zones.c.source,
            projects_functional_zones.c.properties,
            projects_functional_zones.c.created_at,
            projects_functional_zones.c.updated_at,
        )
        .select_from(
            projects_functional_zones.join(
                scenarios_data,
                scenarios_data.c.scenario_id == projects_functional_zones.c.scenario_id,
            ).join(
                functional_zone_types_dict,
                functional_zone_types_dict.c.functional_zone_type_id
                == projects_functional_zones.c.functional_zone_type_id,
            )
        )
        .where(
            projects_functional_zones.c.scenario_id == scenario_id,
            projects_functional_zones.c.year == year,
            projects_functional_zones.c.source == source,
            projects_functional_zones.c.functional_zone_type_id == functional_zone_type_id,
        )
    )

    # Act
    result = await get_functional_zones_by_scenario_id_from_db(
        mock_conn, scenario_id, year, source, functional_zone_type_id, user_id
    )

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(
        isinstance(item, ScenarioFunctionalZoneDTO) for item in result
    ), "Each item should be a ScenarioFunctionalZoneDTO."
    assert isinstance(
        ScenarioFunctionalZone.from_dto(result[0]), ScenarioFunctionalZone
    ), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))
    mock_check.assert_called_once_with(mock_conn, scenario_id, user_id)


@pytest.mark.asyncio
async def test_get_context_functional_zones_sources_from_db(mock_conn: MockConnection):
    """Test the get_context_functional_zones_sources_from_db function."""

    # Arrange
    project_id = 1
    user_id = "mock_string"
    context_geom, context_ids = await get_context_territories_geometry(mock_conn, project_id, user_id)
    statement = (
        select(functional_zones_data.c.year, functional_zones_data.c.source)
        .where(
            (functional_zones_data.c.territory_id.in_(context_ids))
            | (ST_Intersects(functional_zones_data.c.geometry, context_geom))
        )
        .distinct()
    )

    # Act
    result = await get_context_functional_zones_sources_from_db(mock_conn, project_id, user_id)

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(
        isinstance(item, FunctionalZoneSourceDTO) for item in result
    ), "Each item should be a FunctionalZoneSourceDTO."
    assert isinstance(
        FunctionalZoneSource.from_dto(result[0]), FunctionalZoneSource
    ), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))


@pytest.mark.asyncio
async def test_get_context_functional_zones_from_db(mock_conn: MockConnection):
    """Test the get_context_functional_zones_from_db function."""

    # Arrange
    project_id = 1
    year = datetime.today().year
    source = "mock_string"
    functional_zone_type_id = 1
    user_id = "mock_string"
    context_geom, context_ids = await get_context_territories_geometry(mock_conn, project_id, user_id)
    statement = (
        select(
            functional_zones_data.c.functional_zone_id,
            functional_zones_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            functional_zones_data.c.functional_zone_type_id,
            functional_zone_types_dict.c.name.label("functional_zone_type_name"),
            functional_zone_types_dict.c.zone_nickname.label("functional_zone_type_nickname"),
            functional_zones_data.c.name,
            ST_AsEWKB(
                case(
                    (
                        ~ST_Within(functional_zones_data.c.geometry, context_geom),
                        ST_Intersection(functional_zones_data.c.geometry, context_geom),
                    ),
                    else_=functional_zones_data.c.geometry,
                )
            ).label("geometry"),
            functional_zones_data.c.year,
            functional_zones_data.c.source,
            functional_zones_data.c.properties,
            functional_zones_data.c.created_at,
            functional_zones_data.c.updated_at,
        )
        .select_from(
            functional_zones_data.join(
                territories_data,
                territories_data.c.territory_id == functional_zones_data.c.territory_id,
            ).join(
                functional_zone_types_dict,
                functional_zone_types_dict.c.functional_zone_type_id == functional_zones_data.c.functional_zone_type_id,
            )
        )
        .where(
            functional_zones_data.c.year == year,
            functional_zones_data.c.source == source,
            (
                functional_zones_data.c.territory_id.in_(context_ids)
                | ST_Intersects(functional_zones_data.c.geometry, context_geom)
            ),
            functional_zones_data.c.functional_zone_type_id == functional_zone_type_id,
        )
    )

    # Act
    result = await get_context_functional_zones_from_db(
        mock_conn, project_id, year, source, functional_zone_type_id, user_id
    )

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(isinstance(item, FunctionalZoneDTO) for item in result), "Each item should be a FunctionalZoneDTO."
    assert isinstance(FunctionalZone.from_dto(result[0]), FunctionalZone), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))


@pytest.mark.asyncio
async def test_get_functional_zone_by_ids(mock_conn: MockConnection):
    """Test the get_functional_zone_by_ids function."""

    # Arrange
    ids = [1]
    not_found_ids = [1, 2]
    too_many_ids = list(range(OBJECTS_NUMBER_LIMIT + 1))
    statement = (
        select(
            projects_functional_zones.c.functional_zone_id,
            projects_functional_zones.c.scenario_id,
            scenarios_data.c.name.label("scenario_name"),
            projects_functional_zones.c.functional_zone_type_id,
            functional_zone_types_dict.c.name.label("functional_zone_type_name"),
            functional_zone_types_dict.c.zone_nickname.label("functional_zone_type_nickname"),
            projects_functional_zones.c.name,
            ST_AsEWKB(projects_functional_zones.c.geometry).label("geometry"),
            projects_functional_zones.c.year,
            projects_functional_zones.c.source,
            projects_functional_zones.c.properties,
            projects_functional_zones.c.created_at,
            projects_functional_zones.c.updated_at,
        )
        .select_from(
            projects_functional_zones.join(
                scenarios_data,
                scenarios_data.c.scenario_id == projects_functional_zones.c.scenario_id,
            ).join(
                functional_zone_types_dict,
                functional_zone_types_dict.c.functional_zone_type_id
                == projects_functional_zones.c.functional_zone_type_id,
            )
        )
        .where(projects_functional_zones.c.functional_zone_id.in_(ids))
    )

    # Act
    with pytest.raises(EntitiesNotFoundByIds):
        await get_functional_zone_by_ids(mock_conn, not_found_ids)
    with pytest.raises(TooManyObjectsError):
        await get_functional_zone_by_ids(mock_conn, too_many_ids)
    result = await get_functional_zone_by_ids(mock_conn, ids)

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(
        isinstance(item, ScenarioFunctionalZoneDTO) for item in result
    ), "Each item should be a ScenarioFunctionalZoneDTO."
    assert isinstance(
        ScenarioFunctionalZone.from_dto(result[0]), ScenarioFunctionalZone
    ), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))


@pytest.mark.asyncio
@patch("idu_api.urban_api.logic.impl.helpers.projects_functional_zones.check_scenario")
async def test_add_scenario_functional_zones_to_db(
    mock_check: AsyncMock, mock_conn: MockConnection, scenario_functional_zone_post_req: ScenarioFunctionalZone
):
    """Test the add_scenario_functional_zones_to_db function."""

    # Arrange
    scenario_id = 1
    user_id = "mock_string"
    delete_statement = delete(projects_functional_zones).where(projects_functional_zones.c.scenario_id == scenario_id)
    insert_statement = (
        insert(projects_functional_zones)
        .values([{"scenario_id": scenario_id, **extract_values_from_model(scenario_functional_zone_post_req)}])
        .returning(projects_functional_zones.c.functional_zone_id)
    )

    # Act
    result = await add_scenario_functional_zones_to_db(
        mock_conn, [scenario_functional_zone_post_req], scenario_id, user_id
    )

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(
        isinstance(item, ScenarioFunctionalZoneDTO) for item in result
    ), "Each item should be a ScenarioFunctionalZoneDTO."
    assert isinstance(
        ScenarioFunctionalZone.from_dto(result[0]), ScenarioFunctionalZone
    ), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(delete_statement))
    mock_conn.execute_mock.assert_any_call(str(insert_statement))
    mock_conn.commit_mock.assert_called_once()
    mock_check.assert_called_once_with(mock_conn, scenario_id, user_id, to_edit=True)


@pytest.mark.asyncio
@patch("idu_api.urban_api.logic.impl.helpers.projects_functional_zones.check_scenario")
async def test_put_scenario_functional_zone_to_db(
    mock_check: AsyncMock, mock_conn: MockConnection, scenario_functional_zone_put_req: ScenarioFunctionalZonePut
):
    """Test the put_scenario_functional_zone_to_db function."""

    # Arrange
    async def check_functional_zone(conn, table, conditions):
        if table == projects_functional_zones:
            return False
        return True

    async def check_functional_zone_type(conn, table, conditions):
        if table == functional_zone_types_dict:
            return False
        return True

    scenario_id = 1
    functional_zone_id = 1
    user_id = "mock_string"
    update_statement = (
        update(projects_functional_zones)
        .where(projects_functional_zones.c.functional_zone_id == functional_zone_id)
        .values(
            name=scenario_functional_zone_put_req.name,
            functional_zone_type_id=scenario_functional_zone_put_req.functional_zone_type_id,
            year=scenario_functional_zone_put_req.year,
            source=scenario_functional_zone_put_req.source,
            geometry=ST_GeomFromWKB(
                scenario_functional_zone_put_req.geometry.as_shapely_geometry().wkb, text(str(SRID))
            ),
            properties=scenario_functional_zone_put_req.properties,
            updated_at=datetime.now(timezone.utc),
        )
    )

    # Act
    with patch(
        "idu_api.urban_api.logic.impl.helpers.projects_functional_zones.check_existence",
        new=AsyncMock(side_effect=check_functional_zone),
    ):
        with pytest.raises(EntityNotFoundById):
            await put_scenario_functional_zone_to_db(
                mock_conn, scenario_functional_zone_put_req, scenario_id, functional_zone_id, user_id
            )
    with patch(
        "idu_api.urban_api.logic.impl.helpers.projects_functional_zones.check_existence",
        new=AsyncMock(side_effect=check_functional_zone_type),
    ):
        with pytest.raises(EntityNotFoundById):
            await put_scenario_functional_zone_to_db(
                mock_conn, scenario_functional_zone_put_req, scenario_id, functional_zone_id, user_id
            )
    result = await put_scenario_functional_zone_to_db(
        mock_conn, scenario_functional_zone_put_req, scenario_id, functional_zone_id, user_id
    )

    # Assert
    assert isinstance(result, ScenarioFunctionalZoneDTO), "Result should be a ScenarioFunctionalZoneDTO."
    assert isinstance(
        ScenarioFunctionalZone.from_dto(result), ScenarioFunctionalZone
    ), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(update_statement))
    mock_conn.commit_mock.assert_called_once()
    mock_check.assert_any_call(mock_conn, scenario_id, user_id, to_edit=True)


@pytest.mark.asyncio
@patch("idu_api.urban_api.logic.impl.helpers.projects_functional_zones.check_scenario")
async def test_patch_scenario_functional_zone_to_db(
    mock_check: AsyncMock, mock_conn: MockConnection, scenario_functional_zone_patch_req: ScenarioFunctionalZonePatch
):
    """Test the patch_scenario_functional_zone_to_db function."""

    # Arrange
    async def check_functional_zone(conn, table, conditions):
        if table == projects_functional_zones:
            return False
        return True

    async def check_functional_zone_type(conn, table, conditions):
        if table == functional_zone_types_dict:
            return False
        return True

    scenario_id = 1
    functional_zone_id = 1
    user_id = "test_user"
    update_statement = (
        update(projects_functional_zones)
        .where(projects_functional_zones.c.functional_zone_id == functional_zone_id)
        .values(
            **scenario_functional_zone_patch_req.model_dump(exclude_unset=True), updated_at=datetime.now(timezone.utc)
        )
    )

    # Act
    with patch(
        "idu_api.urban_api.logic.impl.helpers.projects_functional_zones.check_existence",
        new=AsyncMock(side_effect=check_functional_zone),
    ):
        with pytest.raises(EntityNotFoundById):
            await patch_scenario_functional_zone_to_db(
                mock_conn, scenario_functional_zone_patch_req, scenario_id, functional_zone_id, user_id
            )
    with patch(
        "idu_api.urban_api.logic.impl.helpers.projects_functional_zones.check_existence",
        new=AsyncMock(side_effect=check_functional_zone_type),
    ):
        with pytest.raises(EntityNotFoundById):
            await patch_scenario_functional_zone_to_db(
                mock_conn, scenario_functional_zone_patch_req, scenario_id, functional_zone_id, user_id
            )
    result = await patch_scenario_functional_zone_to_db(
        mock_conn, scenario_functional_zone_patch_req, scenario_id, functional_zone_id, user_id
    )

    # Assert
    assert isinstance(result, ScenarioFunctionalZoneDTO), "Result should be a ScenarioFunctionalZoneDTO."
    assert isinstance(
        ScenarioFunctionalZone.from_dto(result), ScenarioFunctionalZone
    ), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(update_statement))
    mock_conn.commit_mock.assert_called_once()
    mock_check.assert_any_call(mock_conn, scenario_id, user_id, to_edit=True)


@pytest.mark.asyncio
@patch("idu_api.urban_api.logic.impl.helpers.projects_functional_zones.check_scenario")
async def test_delete_functional_zones_by_scenario_id_from_db(mock_check: AsyncMock, mock_conn: MockConnection):
    """Test the delete_functional_zones_by_scenario_id_from_db function."""

    # Arrange
    scenario_id = 1
    user_id = "test_user"
    delete_statement = delete(projects_functional_zones).where(projects_functional_zones.c.scenario_id == scenario_id)

    # Act
    result = await delete_functional_zones_by_scenario_id_from_db(mock_conn, scenario_id, user_id)

    # Assert
    assert result == {"status": "ok"}, "Result should be {'status': 'ok'}."
    mock_conn.execute_mock.assert_called_once_with(str(delete_statement))
    mock_conn.commit_mock.assert_called_once()
    mock_check.assert_called_once_with(mock_conn, scenario_id, user_id, to_edit=True)
