"""Unit tests for indicator objects are defined here."""

from collections.abc import Callable
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy.sql import delete, insert, select, update

from idu_api.common.db.entities import (
    indicators_dict,
    indicators_groups_data,
    indicators_groups_dict,
    measurement_units_dict,
    territories_data,
    territory_indicators_data,
)
from idu_api.urban_api.dto import IndicatorDTO, IndicatorsGroupDTO, IndicatorValueDTO, MeasurementUnitDTO
from idu_api.urban_api.exceptions.logic.common import EntityAlreadyExists, EntityNotFoundById, EntityNotFoundByParams
from idu_api.urban_api.logic.impl.helpers.indicators import (
    add_indicator_to_db,
    add_indicator_value_to_db,
    add_indicators_group_to_db,
    add_measurement_unit_to_db,
    delete_indicator_from_db,
    delete_indicator_value_from_db,
    get_indicator_by_id_from_db,
    get_indicator_value_by_id_from_db,
    get_indicator_values_by_id_from_db,
    get_indicators_by_group_id_from_db,
    get_indicators_by_parent_from_db,
    get_indicators_groups_from_db,
    get_measurement_units_from_db,
    patch_indicator_to_db,
    put_indicator_to_db,
    put_indicator_value_to_db,
    update_indicators_group_from_db,
)
from idu_api.urban_api.schemas import (
    Indicator,
    IndicatorsGroup,
    IndicatorsGroupPost,
    IndicatorsPatch,
    IndicatorsPost,
    IndicatorsPut,
    IndicatorValue,
    IndicatorValuePost,
    IndicatorValuePut,
    MeasurementUnit,
    MeasurementUnitPost,
)
from tests.urban_api.helpers.connection import MockConnection

####################################################################################
#                           Default use-case tests                                 #
####################################################################################


@pytest.mark.asyncio
async def test_get_measurement_units_from_db(mock_conn: MockConnection):
    """Test the get_measurement_units_from_db function."""

    # Arrange
    statement = select(measurement_units_dict).order_by(measurement_units_dict.c.measurement_unit_id)

    # Act
    result = await get_measurement_units_from_db(mock_conn)

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(isinstance(item, MeasurementUnitDTO) for item in result), "Each item should be a MeasurementUnitDTO."
    assert isinstance(MeasurementUnit.from_dto(result[0]), MeasurementUnit), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_called_once_with(str(statement))


@pytest.mark.asyncio
async def test_add_measurement_unit_to_db(mock_conn: MockConnection, measurement_unit_post_req: MeasurementUnitPost):
    """Test the add_measurement_unit_to_db function."""

    # Arrange
    statement_insert = (
        insert(measurement_units_dict)
        .values(**measurement_unit_post_req.model_dump())
        .returning(measurement_units_dict)
    )

    # Act
    with patch("idu_api.urban_api.logic.impl.helpers.indicators.check_existence") as mock_check_existence:
        with pytest.raises(EntityAlreadyExists):
            await add_measurement_unit_to_db(mock_conn, measurement_unit_post_req)
        mock_check_existence.return_value = False
        result = await add_measurement_unit_to_db(mock_conn, measurement_unit_post_req)

    # Assert
    assert isinstance(result, MeasurementUnitDTO), "Result should be a MeasurementUnitDTO."
    assert isinstance(MeasurementUnit.from_dto(result), MeasurementUnit), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_called_once_with(str(statement_insert))
    mock_conn.commit_mock.assert_called_once()


@pytest.mark.asyncio
async def test_get_indicators_groups_from_db(mock_conn: MockConnection):
    """Test the get_indicators_groups_from_db function."""

    # Arrange
    statement = (
        select(
            indicators_groups_dict.c.indicators_group_id,
            indicators_groups_dict.c.name.label("group_name"),
            indicators_dict,
            measurement_units_dict.c.name.label("measurement_unit_name"),
        )
        .select_from(
            indicators_groups_dict.outerjoin(
                indicators_groups_data,
                indicators_groups_dict.c.indicators_group_id == indicators_groups_data.c.indicators_group_id,
            )
            .outerjoin(indicators_dict, indicators_groups_data.c.indicator_id == indicators_dict.c.indicator_id)
            .outerjoin(
                measurement_units_dict,
                indicators_dict.c.measurement_unit_id == measurement_units_dict.c.measurement_unit_id,
            )
        )
        .order_by(indicators_groups_dict.c.indicators_group_id, indicators_dict.c.indicator_id)
    )

    # Act
    result = await get_indicators_groups_from_db(mock_conn)

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(isinstance(group, IndicatorsGroupDTO) for group in result), "Each item should be an IndicatorsGroupDTO."
    assert isinstance(IndicatorsGroup.from_dto(result[0]), IndicatorsGroup), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_called_once_with(str(statement))


@pytest.mark.asyncio
async def test_add_indicators_group_to_db(mock_conn: MockConnection, indicators_group_post_req: IndicatorsGroupPost):
    """Test the add_indicators_group_to_db function."""

    # Arrange
    indicators_group_id = 1
    statement_check_indicators = (
        select(
            indicators_dict,
            measurement_units_dict.c.name.label("measurement_unit_name"),
        ).select_from(
            indicators_dict.outerjoin(
                measurement_units_dict,
                indicators_dict.c.measurement_unit_id == measurement_units_dict.c.measurement_unit_id,
            )
        )
    ).where(indicators_dict.c.indicator_id.in_(indicators_group_post_req.indicators_ids))
    statement_insert_group = (
        insert(indicators_groups_dict)
        .values(name=indicators_group_post_req.name)
        .returning(indicators_groups_dict.c.indicators_group_id)
    )
    statement_insert_indicators = insert(indicators_groups_data).values(
        [
            {"indicators_group_id": indicators_group_id, "indicator_id": indicator_id}
            for indicator_id in indicators_group_post_req.indicators_ids
        ]
    )

    # Act
    with patch("idu_api.urban_api.logic.impl.helpers.indicators.check_existence") as mock_check_existence:
        with pytest.raises(EntityAlreadyExists):
            await add_indicators_group_to_db(mock_conn, indicators_group_post_req)
        mock_check_existence.return_value = False
        result = await add_indicators_group_to_db(mock_conn, indicators_group_post_req)

    # Assert
    assert isinstance(result, IndicatorsGroupDTO), "Result should be an IndicatorsGroupDTO."
    assert isinstance(IndicatorsGroup.from_dto(result), IndicatorsGroup), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement_check_indicators))
    mock_conn.execute_mock.assert_any_call(str(statement_insert_group))
    mock_conn.execute_mock.assert_any_call(str(statement_insert_indicators))
    mock_conn.commit_mock.assert_called_once()


@pytest.mark.asyncio
async def test_update_indicators_group_from_db(
    mock_conn: MockConnection, indicators_group_post_req: IndicatorsGroupPost
):
    """Test the update_indicators_group_from_db function."""

    # Arrange
    indicators_group_id = 1
    statement_check_indicators = (
        select(
            indicators_dict,
            measurement_units_dict.c.name.label("measurement_unit_name"),
        ).select_from(
            indicators_dict.outerjoin(
                measurement_units_dict,
                indicators_dict.c.measurement_unit_id == measurement_units_dict.c.measurement_unit_id,
            )
        )
    ).where(indicators_dict.c.indicator_id.in_(indicators_group_post_req.indicators_ids))
    statement_delete = delete(indicators_groups_data).where(
        indicators_groups_data.c.indicators_group_id == indicators_group_id
    )
    statement_insert_indicators = insert(indicators_groups_data).values(
        [
            {"indicators_group_id": indicators_group_id, "indicator_id": indicator_id}
            for indicator_id in indicators_group_post_req.indicators_ids
        ]
    )

    # Act
    result = await update_indicators_group_from_db(mock_conn, indicators_group_post_req)

    # Assert
    assert isinstance(result, IndicatorsGroupDTO), "Result should be an IndicatorsGroupDTO."
    assert isinstance(IndicatorsGroup.from_dto(result), IndicatorsGroup), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement_check_indicators))
    mock_conn.execute_mock.assert_any_call(str(statement_delete))
    mock_conn.execute_mock.assert_any_call(str(statement_insert_indicators))
    mock_conn.commit_mock.assert_called_once()


@pytest.mark.asyncio
async def test_get_indicators_by_group_id_from_db(mock_conn: MockConnection):
    """Test the get_indicators_by_group_id_from_db function."""

    # Arrange
    indicators_group_id = 1
    statement = (
        select(indicators_dict, measurement_units_dict.c.name.label("measurement_unit_name"))
        .select_from(
            indicators_groups_data.join(
                indicators_dict,
                indicators_dict.c.indicator_id == indicators_groups_data.c.indicator_id,
            ).outerjoin(
                measurement_units_dict,
                indicators_dict.c.measurement_unit_id == measurement_units_dict.c.measurement_unit_id,
            )
        )
        .where(indicators_groups_data.c.indicators_group_id == indicators_group_id)
    )

    # Act
    with patch("idu_api.urban_api.logic.impl.helpers.indicators.check_existence") as mock_check_existence:
        result = await get_indicators_by_group_id_from_db(mock_conn, indicators_group_id)
        mock_check_existence.return_value = False
        with pytest.raises(EntityNotFoundById):
            await get_indicators_by_group_id_from_db(mock_conn, indicators_group_id)

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(isinstance(indicator, IndicatorDTO) for indicator in result), "Each item should be an IndicatorDTO."
    assert isinstance(Indicator.from_dto(result[0]), Indicator), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_called_once_with(str(statement))


@pytest.mark.asyncio
async def test_get_indicators_by_parent_from_db(mock_conn: MockConnection):
    """Test the get_indicator_by_id_from_db function."""

    # Arrange
    async def check_parent_indicator(conn, table, conditions=None, not_conditions=None):
        if table == indicators_dict:
            return False
        return True

    parent_id = 1
    statement = select(
        indicators_dict,
        measurement_units_dict.c.name.label("measurement_unit_name"),
    ).select_from(
        indicators_dict.outerjoin(
            measurement_units_dict,
            indicators_dict.c.measurement_unit_id == measurement_units_dict.c.measurement_unit_id,
        )
    )
    cte_statement = statement.where(
        indicators_dict.c.parent_id == parent_id if parent_id is not None else indicators_dict.c.parent_id.is_(None)
    )
    cte_statement = cte_statement.cte(name="indicators_recursive", recursive=True)
    recursive_part = statement.join(cte_statement, indicators_dict.c.parent_id == cte_statement.c.indicator_id)
    recursive_statement = select(cte_statement.union_all(recursive_part))
    statement = statement.where(indicators_dict.c.parent_id == parent_id)
    requested_indicators = statement.cte("requested_indicators")
    statement = select(requested_indicators)
    recursive_statement = select(recursive_statement.cte("requested_indicators"))
    name, territory_id = "mock_string", 1
    territory_filter = (
        select(territory_indicators_data.c.indicator_id.distinct().label("indicator_id"))
        .where(territory_indicators_data.c.territory_id == territory_id)
        .cte("territory_filter")
    )
    statement_with_filters = statement.where(
        requested_indicators.c.indicator_id.in_(select(territory_filter.c.indicator_id)),
        requested_indicators.c.name_full.ilike(f"%{name}%") | requested_indicators.c.name_short.ilike(f"%{name}%"),
    )

    # Act
    with patch(
        "idu_api.urban_api.logic.impl.helpers.indicators.check_existence",
        new=AsyncMock(side_effect=check_parent_indicator),
    ):
        with pytest.raises(EntityNotFoundById):
            await get_indicators_by_parent_from_db(mock_conn, 2, None, None, None, True)
    await get_indicators_by_parent_from_db(mock_conn, None, "mock_string", None, None, True)
    await get_indicators_by_parent_from_db(mock_conn, parent_id, None, "mock_string", 1, False)
    result = await get_indicators_by_parent_from_db(mock_conn, parent_id, None, None, None, False)

    # Assert
    assert isinstance(result, list), "Result should be an IndicatorDTO."
    assert all(isinstance(item, IndicatorDTO) for item in result), "Each item should be an IndicatorDTO."
    assert isinstance(Indicator.from_dto(result[0]), Indicator), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))
    mock_conn.execute_mock.assert_any_call(str(recursive_statement))
    mock_conn.execute_mock.assert_any_call(str(statement_with_filters))


@pytest.mark.asyncio
async def test_get_indicator_by_id_from_db(mock_conn: MockConnection):
    """Test the get_indicator_by_id_from_db function."""

    # Arrange
    indicator_id = 1
    statement = (
        select(
            indicators_dict,
            measurement_units_dict.c.name.label("measurement_unit_name"),
        )
        .select_from(
            indicators_dict.outerjoin(
                measurement_units_dict,
                measurement_units_dict.c.measurement_unit_id == indicators_dict.c.measurement_unit_id,
            )
        )
        .where(indicators_dict.c.indicator_id == indicator_id)
    )

    # Act
    result = await get_indicator_by_id_from_db(mock_conn, indicator_id)

    # Assert
    assert isinstance(result, IndicatorDTO), "Result should be an IndicatorDTO."
    assert isinstance(Indicator.from_dto(result), Indicator), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_called_once_with(str(statement))


@pytest.mark.asyncio
async def test_add_indicator_to_db(mock_conn: MockConnection, indicators_post_req: IndicatorsPost):
    """Test the add_indicator_to_db function."""

    # Arrange
    async def check_parent_indicator(conn, table, conditions):
        if table == indicators_dict and conditions == {"indicator_id": indicators_post_req.parent_id}:
            return False
        return True

    async def check_indicator_name(conn, table, conditions):
        if table == indicators_dict and conditions == {"name_full": indicators_post_req.name_full}:
            return False
        return True

    async def check_measurement_unit(conn, table, conditions):
        if table == measurement_units_dict:
            return False
        return True

    statement = (
        insert(indicators_dict).values(**indicators_post_req.model_dump()).returning(indicators_dict.c.indicator_id)
    )

    # Act
    with pytest.raises(EntityAlreadyExists):
        await add_indicator_to_db(mock_conn, indicators_post_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.indicators.check_existence",
        new=AsyncMock(side_effect=check_parent_indicator),
    ):
        with pytest.raises(EntityNotFoundById):
            await add_indicator_to_db(mock_conn, indicators_post_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.indicators.check_existence",
        new=AsyncMock(side_effect=check_measurement_unit),
    ):
        with pytest.raises(EntityNotFoundById):
            await add_indicator_to_db(mock_conn, indicators_post_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.indicators.check_existence",
        new=AsyncMock(side_effect=check_indicator_name),
    ):
        result = await add_indicator_to_db(mock_conn, indicators_post_req)

    # Assert
    assert isinstance(result, IndicatorDTO), "Result should be an IndicatorDTO."
    assert isinstance(Indicator.from_dto(result), Indicator), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))
    mock_conn.commit_mock.assert_called_once()


@pytest.mark.asyncio
async def test_put_indicator_to_db(mock_conn: MockConnection, indicators_put_req: IndicatorsPut):
    """Test the put_indicator_to_db function."""

    # Arrange
    async def check_parent_indicator(conn, table, conditions):
        if table == indicators_dict and conditions == {"indicator_id": indicators_put_req.parent_id}:
            return False
        return True

    async def check_indicator_name(conn, table, conditions):
        if table == indicators_dict and conditions == {"name_full": indicators_put_req.name_full}:
            return False
        return True

    async def check_measurement_unit(conn, table, conditions):
        if table == measurement_units_dict:
            return False
        return True

    statement_insert = (
        insert(indicators_dict).values(**indicators_put_req.model_dump()).returning(indicators_dict.c.indicator_id)
    )
    statement_update = (
        update(indicators_dict)
        .where(indicators_dict.c.name_full == indicators_put_req.name_full)
        .values(**indicators_put_req.model_dump(), updated_at=datetime.now(timezone.utc))
        .returning(indicators_dict.c.indicator_id)
    )

    # Act
    with patch(
        "idu_api.urban_api.logic.impl.helpers.indicators.check_existence",
        new=AsyncMock(side_effect=check_parent_indicator),
    ):
        with pytest.raises(EntityNotFoundById):
            await put_indicator_to_db(mock_conn, indicators_put_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.indicators.check_existence",
        new=AsyncMock(side_effect=check_measurement_unit),
    ):
        with pytest.raises(EntityNotFoundById):
            await put_indicator_to_db(mock_conn, indicators_put_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.indicators.check_existence",
        new=AsyncMock(side_effect=check_indicator_name),
    ):
        await put_indicator_to_db(mock_conn, indicators_put_req)
    result = await put_indicator_to_db(mock_conn, indicators_put_req)

    # Assert
    assert isinstance(result, IndicatorDTO), "Result should be an IndicatorDTO."
    assert isinstance(Indicator.from_dto(result), Indicator), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement_insert))
    mock_conn.execute_mock.assert_any_call(str(statement_update))
    assert mock_conn.commit_mock.call_count == 2, "Commit mock count should be one for one method."


@pytest.mark.asyncio
async def test_patch_indicator_to_db(mock_conn: MockConnection, indicators_patch_req: IndicatorsPatch):
    """Test the patch_indicator_to_db function."""

    # Arrange
    indicator_id = 1

    async def check_indicator(conn, table, conditions, not_conditions=None):
        if table == indicators_dict and conditions == {"indicator_id": indicator_id}:
            return False
        return True

    async def check_parent_indicator(conn, table, conditions, not_conditions=None):
        if table == indicators_dict and conditions == {"indicator_id": indicators_patch_req.parent_id}:
            return False
        return True

    async def check_indicator_name(conn, table, conditions, not_conditions=None):
        if table == indicators_dict and conditions == {"name_full": indicators_patch_req.name_full}:
            return False
        return True

    async def check_measurement_unit(conn, table, conditions, not_conditions=None):
        if table == measurement_units_dict:
            return False
        return True

    statement = (
        update(indicators_dict)
        .where(indicators_dict.c.indicator_id == indicator_id)
        .values(**indicators_patch_req.model_dump(exclude_unset=True), updated_at=datetime.now(timezone.utc))
    )

    # Act
    with pytest.raises(EntityAlreadyExists):
        await patch_indicator_to_db(mock_conn, indicator_id, indicators_patch_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.indicators.check_existence",
        new=AsyncMock(side_effect=check_indicator),
    ):
        with pytest.raises(EntityNotFoundById):
            await patch_indicator_to_db(mock_conn, indicator_id, indicators_patch_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.indicators.check_existence",
        new=AsyncMock(side_effect=check_parent_indicator),
    ):
        with pytest.raises(EntityNotFoundById):
            await patch_indicator_to_db(mock_conn, indicator_id, indicators_patch_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.indicators.check_existence",
        new=AsyncMock(side_effect=check_measurement_unit),
    ):
        with pytest.raises(EntityNotFoundById):
            await patch_indicator_to_db(mock_conn, indicator_id, indicators_patch_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.indicators.check_existence",
        new=AsyncMock(side_effect=check_indicator_name),
    ):
        result = await patch_indicator_to_db(mock_conn, indicator_id, indicators_patch_req)

    # Assert
    assert isinstance(result, IndicatorDTO), "Result should be an IndicatorDTO."
    assert isinstance(Indicator.from_dto(result), Indicator), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))
    mock_conn.commit_mock.assert_called_once()


@pytest.mark.asyncio
async def test_delete_indicator_from_db(mock_conn: MockConnection):
    """Test the delete_indicator_from_db function."""

    # Arrange
    indicator_id = 1
    statement = delete(indicators_dict).where(indicators_dict.c.indicator_id == indicator_id)

    # Act
    with patch("idu_api.urban_api.logic.impl.helpers.indicators.check_existence") as mock_check_existence:
        result = await delete_indicator_from_db(mock_conn, indicator_id)
        mock_check_existence.return_value = False
        with pytest.raises(EntityNotFoundById):
            await delete_indicator_from_db(mock_conn, indicator_id)

    # Assert
    assert result == {"status": "ok"}, "Result should be {'status': 'ok'}."
    mock_conn.execute_mock.assert_called_once_with(str(statement))
    mock_conn.commit_mock.assert_called_once()


@pytest.mark.asyncio
async def test_get_indicator_values_by_id_from_db(
    mock_conn: MockConnection, indicator_value_post_req: IndicatorValuePost
):
    """Test the get_indicator_values_by_id_from_db function."""

    # Arrange
    statement = (
        select(
            territory_indicators_data,
            indicators_dict.c.parent_id,
            indicators_dict.c.name_full,
            indicators_dict.c.level,
            indicators_dict.c.list_label,
            measurement_units_dict.c.measurement_unit_id,
            measurement_units_dict.c.name.label("measurement_unit_name"),
            territories_data.c.name.label("territory_name"),
        )
        .select_from(
            territory_indicators_data.join(
                indicators_dict,
                indicators_dict.c.indicator_id == territory_indicators_data.c.indicator_id,
            )
            .outerjoin(
                measurement_units_dict,
                measurement_units_dict.c.measurement_unit_id == indicators_dict.c.measurement_unit_id,
            )
            .join(territories_data, territories_data.c.territory_id == territory_indicators_data.c.territory_id)
        )
        .where(
            territory_indicators_data.c.indicator_id == indicator_value_post_req.indicator_id,
            territory_indicators_data.c.territory_id == indicator_value_post_req.territory_id,
            territory_indicators_data.c.date_type == indicator_value_post_req.date_type,
            territory_indicators_data.c.date_value == indicator_value_post_req.date_value,
            territory_indicators_data.c.value_type == indicator_value_post_req.value_type,
            territory_indicators_data.c.information_source.ilike(f"%{indicator_value_post_req.information_source}%"),
        )
    )

    # Act
    with patch("idu_api.urban_api.logic.impl.helpers.indicators.check_existence") as mock_check_existence:
        result = await get_indicator_values_by_id_from_db(
            mock_conn,
            indicator_value_post_req.indicator_id,
            indicator_value_post_req.territory_id,
            indicator_value_post_req.date_type,
            indicator_value_post_req.date_value,
            indicator_value_post_req.value_type,
            indicator_value_post_req.information_source,
        )
        mock_check_existence.return_value = False
        with pytest.raises(EntityNotFoundById):
            await get_indicator_values_by_id_from_db(
                mock_conn,
                indicator_value_post_req.indicator_id,
                indicator_value_post_req.territory_id,
                indicator_value_post_req.date_type,
                indicator_value_post_req.date_value,
                indicator_value_post_req.value_type,
                indicator_value_post_req.information_source,
            )

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(isinstance(item, IndicatorValueDTO) for item in result), "Each item should be an IndicatorValueDTO."
    assert isinstance(IndicatorValue.from_dto(result[0]), IndicatorValue), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_called_once_with(str(statement))


@pytest.mark.asyncio
async def test_get_indicator_value_by_id_from_db(
    mock_conn: MockConnection, indicator_value_post_req: IndicatorValuePost
):
    """Test the get_indicator_value_by_id_from_db function."""

    # Arrange
    statement = (
        select(
            territory_indicators_data,
            indicators_dict.c.parent_id,
            indicators_dict.c.name_full,
            indicators_dict.c.level,
            indicators_dict.c.list_label,
            measurement_units_dict.c.measurement_unit_id,
            measurement_units_dict.c.name.label("measurement_unit_name"),
            territories_data.c.name.label("territory_name"),
        )
        .select_from(
            territory_indicators_data.join(
                indicators_dict,
                indicators_dict.c.indicator_id == territory_indicators_data.c.indicator_id,
            )
            .outerjoin(
                measurement_units_dict,
                measurement_units_dict.c.measurement_unit_id == indicators_dict.c.measurement_unit_id,
            )
            .join(territories_data, territories_data.c.territory_id == territory_indicators_data.c.territory_id)
        )
        .where(
            territory_indicators_data.c.indicator_id == indicator_value_post_req.indicator_id,
            territory_indicators_data.c.territory_id == indicator_value_post_req.territory_id,
            territory_indicators_data.c.date_type == indicator_value_post_req.date_type,
            territory_indicators_data.c.date_value == indicator_value_post_req.date_value,
            territory_indicators_data.c.value_type == indicator_value_post_req.value_type,
            territory_indicators_data.c.information_source == indicator_value_post_req.information_source,
        )
    )

    # Act
    result = await get_indicator_value_by_id_from_db(
        mock_conn,
        indicator_value_post_req.indicator_id,
        indicator_value_post_req.territory_id,
        indicator_value_post_req.date_type,
        indicator_value_post_req.date_value,
        indicator_value_post_req.value_type,
        indicator_value_post_req.information_source,
    )

    # Assert
    assert isinstance(result, IndicatorValueDTO), "Result should be an IndicatorValueDTO."
    assert isinstance(IndicatorValue.from_dto(result), IndicatorValue), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_called_once_with(str(statement))


@pytest.mark.asyncio
async def test_add_indicator_value_to_db(mock_conn: MockConnection, indicator_value_post_req: IndicatorValuePost):
    """Test the add_indicator_value_to_db function."""

    # Arrange
    async def check_indicator(conn, table, conditions):
        if table == indicators_dict:
            return False
        return True

    async def check_territory(conn, table, conditions):
        if table == territories_data:
            return False
        return True

    async def check_indicator_value(conn, table, conditions):
        if table == territory_indicators_data:
            return False
        return True

    statement = (
        insert(territory_indicators_data)
        .values(**indicator_value_post_req.model_dump())
        .returning(territory_indicators_data)
    )

    # Act
    with pytest.raises(EntityAlreadyExists):
        await add_indicator_value_to_db(mock_conn, indicator_value_post_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.indicators.check_existence",
        new=AsyncMock(side_effect=check_indicator),
    ):
        with pytest.raises(EntityNotFoundById):
            await add_indicator_value_to_db(mock_conn, indicator_value_post_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.indicators.check_existence",
        new=AsyncMock(side_effect=check_territory),
    ):
        with pytest.raises(EntityNotFoundById):
            await add_indicator_value_to_db(mock_conn, indicator_value_post_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.indicators.check_existence",
        new=AsyncMock(side_effect=check_indicator_value),
    ):
        result = await add_indicator_value_to_db(mock_conn, indicator_value_post_req)

    # Assert
    assert isinstance(result, IndicatorValueDTO), "Result should be an IndicatorValueDTO."
    assert isinstance(IndicatorValue.from_dto(result), IndicatorValue), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))
    mock_conn.commit_mock.assert_called_once()


@pytest.mark.asyncio
async def test_put_indicator_value_to_db(mock_conn: MockConnection, indicator_value_put_req: IndicatorValuePut):
    """Test the put_indicator_value_to_db function."""

    # Arrange
    async def check_indicator(conn, table, conditions):
        if table == indicators_dict:
            return False
        return True

    async def check_territory(conn, table, conditions):
        if table == territories_data:
            return False
        return True

    async def check_indicator_value(conn, table, conditions):
        if table == territory_indicators_data:
            return False
        return True

    statement_insert = insert(territory_indicators_data).values(**indicator_value_put_req.model_dump())
    statement_update = (
        update(territory_indicators_data)
        .values(value=indicator_value_put_req.value, updated_at=datetime.now(timezone.utc))
        .where(
            territory_indicators_data.c.indicator_id == indicator_value_put_req.indicator_id,
            territory_indicators_data.c.territory_id == indicator_value_put_req.territory_id,
            territory_indicators_data.c.date_type == indicator_value_put_req.date_type,
            territory_indicators_data.c.date_value == indicator_value_put_req.date_value,
            territory_indicators_data.c.value_type == indicator_value_put_req.value_type,
            territory_indicators_data.c.information_source == indicator_value_put_req.information_source,
        )
    )

    # Act
    with patch(
        "idu_api.urban_api.logic.impl.helpers.indicators.check_existence",
        new=AsyncMock(side_effect=check_indicator),
    ):
        with pytest.raises(EntityNotFoundById):
            await add_indicator_value_to_db(mock_conn, indicator_value_put_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.indicators.check_existence",
        new=AsyncMock(side_effect=check_territory),
    ):
        with pytest.raises(EntityNotFoundById):
            await add_indicator_value_to_db(mock_conn, indicator_value_put_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.indicators.check_existence",
        new=AsyncMock(side_effect=check_indicator_value),
    ):
        await put_indicator_value_to_db(mock_conn, indicator_value_put_req)
    result = await put_indicator_value_to_db(mock_conn, indicator_value_put_req)

    # Assert
    assert isinstance(result, IndicatorValueDTO), "Result should be an IndicatorValueDTO."
    assert isinstance(IndicatorValue.from_dto(result), IndicatorValue), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement_insert))
    mock_conn.execute_mock.assert_any_call(str(statement_update))
    assert mock_conn.commit_mock.call_count == 2, "Commit mock count should be one for one method."


@pytest.mark.asyncio
async def test_delete_indicator_value_from_db(mock_conn: MockConnection, indicator_value_post_req: IndicatorValuePost):
    """Test the delete_indicator_value_from_db function."""

    # Arrange
    statement = delete(territory_indicators_data).where(
        territory_indicators_data.c.indicator_id == indicator_value_post_req.indicator_id,
        territory_indicators_data.c.territory_id == indicator_value_post_req.territory_id,
        territory_indicators_data.c.date_type == indicator_value_post_req.date_type,
        territory_indicators_data.c.date_value == indicator_value_post_req.date_value,
        territory_indicators_data.c.value_type == indicator_value_post_req.value_type,
        territory_indicators_data.c.information_source == indicator_value_post_req.information_source,
    )

    # Act
    with patch("idu_api.urban_api.logic.impl.helpers.indicators.check_existence") as mock_check_existence:
        result = await delete_indicator_value_from_db(
            mock_conn,
            indicator_value_post_req.indicator_id,
            indicator_value_post_req.territory_id,
            indicator_value_post_req.date_type,
            indicator_value_post_req.date_value,
            indicator_value_post_req.value_type,
            indicator_value_post_req.information_source,
        )
        mock_check_existence.return_value = False
        with pytest.raises(EntityNotFoundByParams):
            await delete_indicator_value_from_db(
                mock_conn,
                indicator_value_post_req.indicator_id,
                indicator_value_post_req.territory_id,
                indicator_value_post_req.date_type,
                indicator_value_post_req.date_value,
                indicator_value_post_req.value_type,
                indicator_value_post_req.information_source,
            )

    # Assert
    assert result == {"status": "ok"}, "Result should be {'status': 'ok'}."
    mock_conn.execute_mock.assert_called_once_with(str(statement))
    mock_conn.commit_mock.assert_called_once()
