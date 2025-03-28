"""Unit tests for scenario functional zone objects are defined here."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import aiohttp
import pytest
import structlog
from aioresponses import aioresponses
from aioresponses.core import merge_params, normalize_url
from geoalchemy2.functions import ST_AsEWKB
from sqlalchemy import delete, insert, select, update

from idu_api.common.db.entities import (
    hexagons_data,
    indicators_dict,
    indicators_groups_data,
    measurement_units_dict,
    projects_indicators_data,
    scenarios_data,
    territories_data,
)
from idu_api.urban_api.config import UrbanAPIConfig
from idu_api.urban_api.dto import HexagonWithIndicatorsDTO, ScenarioIndicatorValueDTO, UserDTO
from idu_api.urban_api.exceptions.logic.common import EntityAlreadyExists, EntityNotFoundById
from idu_api.urban_api.logic.impl.helpers.projects_indicators import (
    add_scenario_indicator_value_to_db,
    delete_scenario_indicator_value_by_id_from_db,
    delete_scenario_indicators_values_by_scenario_id_from_db,
    get_hexagons_with_indicators_by_scenario_id_from_db,
    get_scenario_indicator_value_by_id_from_db,
    get_scenario_indicators_values_by_scenario_id_from_db,
    patch_scenario_indicator_value_to_db,
    put_scenario_indicator_value_to_db,
    update_all_indicators_values_by_scenario_id_to_db,
)
from idu_api.urban_api.schemas import (
    ScenarioIndicatorValue,
    ScenarioIndicatorValuePatch,
    ScenarioIndicatorValuePost,
    ScenarioIndicatorValuePut,
)
from tests.urban_api.helpers.connection import MockConnection

####################################################################################
#                           Default use-case tests                                 #
####################################################################################


@pytest.mark.asyncio
async def test_get_scenario_indicator_value_by_id_from_db(mock_conn: MockConnection):
    """Test the get_scenario_indicator_value_by_id_from_db function."""

    # Arrange
    indicator_value_id = 1
    statement = (
        select(
            projects_indicators_data,
            indicators_dict.c.parent_id,
            indicators_dict.c.name_full,
            indicators_dict.c.measurement_unit_id,
            measurement_units_dict.c.name.label("measurement_unit_name"),
            indicators_dict.c.level,
            indicators_dict.c.list_label,
            scenarios_data.c.name.label("scenario_name"),
            territories_data.c.name.label("territory_name"),
        )
        .select_from(
            projects_indicators_data.join(
                scenarios_data, scenarios_data.c.scenario_id == projects_indicators_data.c.scenario_id
            )
            .join(indicators_dict, indicators_dict.c.indicator_id == projects_indicators_data.c.indicator_id)
            .outerjoin(
                measurement_units_dict,
                measurement_units_dict.c.measurement_unit_id == indicators_dict.c.measurement_unit_id,
            )
            .outerjoin(territories_data, territories_data.c.territory_id == projects_indicators_data.c.territory_id)
        )
        .where(projects_indicators_data.c.indicator_value_id == indicator_value_id)
    )

    # Act
    result = await get_scenario_indicator_value_by_id_from_db(mock_conn, indicator_value_id)

    # Assert
    assert isinstance(result, ScenarioIndicatorValueDTO), "Result should be a ScenarioIndicatorValueDTO."
    assert isinstance(
        ScenarioIndicatorValue.from_dto(result), ScenarioIndicatorValue
    ), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_called_once_with(str(statement))


@pytest.mark.asyncio
@patch("idu_api.urban_api.logic.impl.helpers.projects_indicators.check_scenario")
async def test_get_scenario_indicators_values_by_scenario_id_from_db(mock_check: AsyncMock, mock_conn: MockConnection):
    """Test the get_scenario_indicators_values_by_scenario_id_from_db function."""

    # Arrange
    scenario_id = 1
    indicator_ids = {1}
    indicators_group_id = 1
    territory_id = 1
    hexagon_id = 1
    user = UserDTO(id="mock_string", is_superuser=False)
    statement = (
        select(
            projects_indicators_data,
            indicators_dict.c.parent_id,
            indicators_dict.c.name_full,
            indicators_dict.c.measurement_unit_id,
            measurement_units_dict.c.name.label("measurement_unit_name"),
            indicators_dict.c.level,
            indicators_dict.c.list_label,
            scenarios_data.c.name.label("scenario_name"),
            territories_data.c.name.label("territory_name"),
        )
        .select_from(
            projects_indicators_data.join(
                scenarios_data, scenarios_data.c.scenario_id == projects_indicators_data.c.scenario_id
            )
            .join(indicators_dict, indicators_dict.c.indicator_id == projects_indicators_data.c.indicator_id)
            .outerjoin(
                measurement_units_dict,
                measurement_units_dict.c.measurement_unit_id == indicators_dict.c.measurement_unit_id,
            )
            .outerjoin(territories_data, territories_data.c.territory_id == projects_indicators_data.c.territory_id)
            .outerjoin(
                indicators_groups_data,
                indicators_groups_data.c.indicator_id == indicators_dict.c.indicator_id,
            )
        )
        .where(
            projects_indicators_data.c.scenario_id == scenario_id,
            indicators_groups_data.c.indicators_group_id == indicators_group_id,
            projects_indicators_data.c.indicator_id.in_(indicator_ids),
            projects_indicators_data.c.territory_id == territory_id,
            projects_indicators_data.c.hexagon_id == hexagon_id,
        )
        .distinct()
        .order_by(projects_indicators_data.c.indicator_value_id)
    )

    # Act
    result = await get_scenario_indicators_values_by_scenario_id_from_db(
        mock_conn, scenario_id, "1", indicators_group_id, territory_id, hexagon_id, user
    )

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(
        isinstance(item, ScenarioIndicatorValueDTO) for item in result
    ), "Each item should be a ScenarioIndicatorValueDTO."
    assert isinstance(
        ScenarioIndicatorValue.from_dto(result[0]), ScenarioIndicatorValue
    ), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))
    mock_check.assert_called_once_with(mock_conn, scenario_id, user)


@pytest.mark.asyncio
@patch("idu_api.urban_api.logic.impl.helpers.projects_indicators.check_scenario")
async def test_add_scenario_indicator_value_to_db(
    mock_check: AsyncMock, mock_conn: MockConnection, scenario_indicator_value_post_req: ScenarioIndicatorValuePost
):
    """Test the add_scenario_indicator_value_to_db function."""

    # Arrange
    async def check_indicator(conn, table, conditions):
        if table == indicators_dict:
            return False
        return True

    async def check_territory(conn, table, conditions):
        if table == territories_data:
            return False
        return True

    async def check_hexagon(conn, table, conditions):
        if table == hexagons_data:
            return False
        return True

    async def check_indicator_value(conn, table, conditions):
        if table == projects_indicators_data:
            return False
        return True

    scenario_id = 1
    user = UserDTO(id="mock_string", is_superuser=False)
    insert_statement = (
        insert(projects_indicators_data)
        .values(**scenario_indicator_value_post_req.model_dump())
        .returning(projects_indicators_data.c.indicator_value_id)
    )

    # Act
    with pytest.raises(EntityAlreadyExists):
        await add_scenario_indicator_value_to_db(mock_conn, scenario_indicator_value_post_req, scenario_id, user)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.projects_indicators.check_existence",
        new=AsyncMock(side_effect=check_indicator),
    ):
        with pytest.raises(EntityNotFoundById):
            await add_scenario_indicator_value_to_db(mock_conn, scenario_indicator_value_post_req, scenario_id, user)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.projects_indicators.check_existence",
        new=AsyncMock(side_effect=check_territory),
    ):
        with pytest.raises(EntityNotFoundById):
            await add_scenario_indicator_value_to_db(mock_conn, scenario_indicator_value_post_req, scenario_id, user)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.projects_indicators.check_existence",
        new=AsyncMock(side_effect=check_hexagon),
    ):
        with pytest.raises(EntityNotFoundById):
            await add_scenario_indicator_value_to_db(mock_conn, scenario_indicator_value_post_req, scenario_id, user)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.projects_indicators.check_existence",
        new=AsyncMock(side_effect=check_indicator_value),
    ):
        result = await add_scenario_indicator_value_to_db(
            mock_conn, scenario_indicator_value_post_req, scenario_id, user
        )

    # Assert
    assert isinstance(result, ScenarioIndicatorValueDTO), "Result should be a ScenarioIndicatorValueDTO."
    assert isinstance(
        ScenarioIndicatorValue.from_dto(result), ScenarioIndicatorValue
    ), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(insert_statement))
    mock_conn.commit_mock.assert_called_once()
    mock_check.assert_any_call(mock_conn, scenario_id, user, to_edit=True)


@pytest.mark.asyncio
@patch("idu_api.urban_api.logic.impl.helpers.projects_indicators.check_scenario")
async def test_put_scenario_indicator_value_to_db(
    mock_check: AsyncMock, mock_conn: MockConnection, scenario_indicator_value_put_req: ScenarioIndicatorValuePut
):
    """Test the put_scenario_indicator_value_to_db function."""

    # Arrange
    async def check_indicator(conn, table, conditions):
        if table == indicators_dict:
            return False
        return True

    async def check_territory(conn, table, conditions):
        if table == territories_data:
            return False
        return True

    async def check_hexagon(conn, table, conditions):
        if table == hexagons_data:
            return False
        return True

    async def check_indicator_value(conn, table, conditions):
        if table == projects_indicators_data:
            return False
        return True

    scenario_id = 1
    user = UserDTO(id="mock_string", is_superuser=False)
    update_statement = (
        update(projects_indicators_data)
        .where(
            projects_indicators_data.c.indicator_id == scenario_indicator_value_put_req.indicator_id,
            projects_indicators_data.c.scenario_id == scenario_id,
            (
                projects_indicators_data.c.territory_id == scenario_indicator_value_put_req.territory_id
                if scenario_indicator_value_put_req.territory_id is not None
                else projects_indicators_data.c.territory_id.is_(None)
            ),
            (
                projects_indicators_data.c.hexagon_id == scenario_indicator_value_put_req.hexagon_id
                if scenario_indicator_value_put_req.hexagon_id is not None
                else projects_indicators_data.c.hexagon_id.is_(None)
            ),
        )
        .values(**scenario_indicator_value_put_req.model_dump(), updated_at=datetime.now(timezone.utc))
        .returning(projects_indicators_data.c.indicator_value_id)
    )
    insert_statement = (
        insert(projects_indicators_data)
        .values(**scenario_indicator_value_put_req.model_dump())
        .returning(projects_indicators_data.c.indicator_value_id)
    )

    # Act
    with patch(
        "idu_api.urban_api.logic.impl.helpers.projects_indicators.check_existence",
        new=AsyncMock(side_effect=check_indicator),
    ):
        with pytest.raises(EntityNotFoundById):
            await add_scenario_indicator_value_to_db(mock_conn, scenario_indicator_value_put_req, scenario_id, user)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.projects_indicators.check_existence",
        new=AsyncMock(side_effect=check_territory),
    ):
        with pytest.raises(EntityNotFoundById):
            await add_scenario_indicator_value_to_db(mock_conn, scenario_indicator_value_put_req, scenario_id, user)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.projects_indicators.check_existence",
        new=AsyncMock(side_effect=check_hexagon),
    ):
        with pytest.raises(EntityNotFoundById):
            await add_scenario_indicator_value_to_db(mock_conn, scenario_indicator_value_put_req, scenario_id, user)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.projects_indicators.check_existence",
        new=AsyncMock(side_effect=check_indicator_value),
    ):
        await put_scenario_indicator_value_to_db(mock_conn, scenario_indicator_value_put_req, scenario_id, user)
    result = await put_scenario_indicator_value_to_db(mock_conn, scenario_indicator_value_put_req, scenario_id, user)

    # Assert
    assert isinstance(result, ScenarioIndicatorValueDTO), "Result should be a ScenarioIndicatorValueDTO."
    assert isinstance(
        ScenarioIndicatorValue.from_dto(result), ScenarioIndicatorValue
    ), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(update_statement))
    mock_conn.execute_mock.assert_any_call(str(insert_statement))
    assert mock_conn.commit_mock.call_count == 2, "Commit mock count should be one for one method."
    mock_check.assert_any_call(mock_conn, scenario_id, user, to_edit=True)


@pytest.mark.asyncio
@patch("idu_api.urban_api.logic.impl.helpers.projects_indicators.check_scenario")
async def test_patch_scenario_indicator_value_to_db(
    mock_check: AsyncMock, mock_conn: MockConnection, scenario_indicator_value_patch_req: ScenarioIndicatorValuePatch
):
    """Test the patch_scenario_indicator_value_to_db function."""

    # Arrange
    indicator_value_id = 1
    scenario_id = 1
    user = UserDTO(id="mock_string", is_superuser=False)
    update_statement = (
        update(projects_indicators_data)
        .where(projects_indicators_data.c.indicator_value_id == indicator_value_id)
        .values(
            **scenario_indicator_value_patch_req.model_dump(exclude_unset=True), updated_at=datetime.now(timezone.utc)
        )
    )

    # Act
    with patch("idu_api.urban_api.logic.impl.helpers.projects_indicators.check_existence") as mock_check_existence:
        mock_check_existence.return_value = False
        with pytest.raises(EntityNotFoundById):
            await patch_scenario_indicator_value_to_db(
                mock_conn, scenario_indicator_value_patch_req, scenario_id, indicator_value_id, user
            )
    result = await patch_scenario_indicator_value_to_db(
        mock_conn, scenario_indicator_value_patch_req, scenario_id, indicator_value_id, user
    )

    # Assert
    assert isinstance(result, ScenarioIndicatorValueDTO), "Result should be a ScenarioIndicatorValueDTO."
    assert isinstance(
        ScenarioIndicatorValue.from_dto(result), ScenarioIndicatorValue
    ), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(update_statement))
    mock_conn.commit_mock.assert_called_once()
    mock_check.assert_any_call(mock_conn, scenario_id, user, to_edit=True)


@pytest.mark.asyncio
@patch("idu_api.urban_api.logic.impl.helpers.projects_indicators.check_scenario")
async def test_delete_scenario_indicators_values_by_scenario_id_from_db(
    mock_check: AsyncMock, mock_conn: MockConnection
):
    """Test the delete_scenario_indicators_values_by_scenario_id_from_db function."""

    # Arrange
    scenario_id = 1
    user = UserDTO(id="mock_string", is_superuser=False)
    delete_statement = delete(projects_indicators_data).where(projects_indicators_data.c.scenario_id == scenario_id)

    # Act
    result = await delete_scenario_indicators_values_by_scenario_id_from_db(mock_conn, scenario_id, user)

    # Assert
    assert result == {"status": "ok"}, "Result should be {'status': 'ok'}."
    mock_conn.execute_mock.assert_any_call(str(delete_statement))
    mock_conn.commit_mock.assert_called_once()
    mock_check.assert_called_once_with(mock_conn, scenario_id, user, to_edit=True)


@pytest.mark.asyncio
@patch("idu_api.urban_api.logic.impl.helpers.projects_indicators.check_scenario")
async def test_delete_scenario_indicator_value_by_id_from_db(mock_check: AsyncMock, mock_conn: MockConnection):
    """Test the delete_scenario_indicator_value_by_id_from_db function."""

    # Arrange
    indicator_value_id = 1
    scenario_id = 1
    user = UserDTO(id="mock_string", is_superuser=False)
    delete_statement = delete(projects_indicators_data).where(
        projects_indicators_data.c.indicator_value_id == indicator_value_id
    )

    # Act
    with patch("idu_api.urban_api.logic.impl.helpers.projects_indicators.check_existence") as mock_check_existence:
        mock_check_existence.return_value = False
        with pytest.raises(EntityNotFoundById):
            await delete_scenario_indicator_value_by_id_from_db(mock_conn, scenario_id, indicator_value_id, user)
    result = await delete_scenario_indicator_value_by_id_from_db(mock_conn, scenario_id, indicator_value_id, user)

    # Assert
    assert result == {"status": "ok"}, "Result should be {'status': 'ok'}."
    mock_conn.execute_mock.assert_any_call(str(delete_statement))
    mock_conn.commit_mock.assert_called_once()
    mock_check.assert_any_call(mock_conn, scenario_id, user, to_edit=True)


@pytest.mark.asyncio
@patch("idu_api.urban_api.logic.impl.helpers.projects_indicators.check_scenario")
async def test_get_hexagons_with_indicators_by_scenario_id_from_db(mock_check: AsyncMock, mock_conn: MockConnection):
    """Test the get_hexagons_with_indicators_by_scenario_id_from_db function."""

    # Arrange
    scenario_id = 1
    indicator_ids = [1]
    indicators_group_id = 1
    user = UserDTO(id="mock_string", is_superuser=False)
    statement = (
        select(
            projects_indicators_data.c.value,
            projects_indicators_data.c.comment,
            indicators_dict.c.indicator_id,
            indicators_dict.c.name_full,
            measurement_units_dict.c.name.label("measurement_unit_name"),
            hexagons_data.c.hexagon_id,
            ST_AsEWKB(hexagons_data.c.geometry).label("geometry"),
            ST_AsEWKB(hexagons_data.c.centre_point).label("centre_point"),
        )
        .select_from(
            projects_indicators_data.join(
                hexagons_data,
                hexagons_data.c.hexagon_id == projects_indicators_data.c.hexagon_id,
            )
            .join(indicators_dict, indicators_dict.c.indicator_id == projects_indicators_data.c.indicator_id)
            .outerjoin(
                measurement_units_dict,
                measurement_units_dict.c.measurement_unit_id == indicators_dict.c.measurement_unit_id,
            )
            .outerjoin(
                indicators_groups_data,
                indicators_groups_data.c.indicator_id == indicators_dict.c.indicator_id,
            )
        )
        .where(
            projects_indicators_data.c.scenario_id == scenario_id,
            indicators_groups_data.c.indicators_group_id == indicators_group_id,
            projects_indicators_data.c.indicator_id.in_(indicator_ids),
        )
        .order_by(projects_indicators_data.c.indicator_id.asc())
    )

    # Act
    result = await get_hexagons_with_indicators_by_scenario_id_from_db(
        mock_conn, scenario_id, "1", indicators_group_id, user
    )

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(
        isinstance(item, HexagonWithIndicatorsDTO) for item in result
    ), "Each item should be a HexagonWithIndicatorsDTO."
    mock_conn.execute_mock.assert_any_call(str(statement))
    mock_check.assert_called_once_with(mock_conn, scenario_id, user)


@pytest.mark.asyncio
async def test_update_all_indicators_values_by_scenario_id_to_db(config: UrbanAPIConfig, mock_conn: MockConnection):
    """Test the update_all_indicators_values_by_scenario_id_to_db function."""

    # Arrange
    scenario_id = 1
    project_id = 1
    user = UserDTO(id="mock_string", is_superuser=False)
    logger: structlog.stdlib.BoundLogger = structlog.get_logger()
    api_url = f"{config.external.hextech_api}/hextech/indicators_saving/save_all"
    params = {"scenario_id": scenario_id, "project_id": project_id, "background": "false"}
    normal_api_url = normalize_url(merge_params(api_url, params))

    # Act
    with aioresponses() as mocked:
        mocked.put(normal_api_url, status=200)
        mocked.put(normal_api_url, status=400)
        result = await update_all_indicators_values_by_scenario_id_to_db(mock_conn, scenario_id, user, logger)
        with pytest.raises(aiohttp.ClientResponseError):
            await update_all_indicators_values_by_scenario_id_to_db(mock_conn, scenario_id, user, logger)

    # Assert
    assert result == {"status": "ok"}, "Result should be {'status': 'ok'}."
    mocked.assert_any_call(
        url=api_url,
        method="PUT",
        params=params,
    )
    assert mocked.requests[("PUT", normal_api_url)][0].kwargs["params"] == params, "Request params do not match."
