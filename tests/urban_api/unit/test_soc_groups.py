"""Unit tests for social groups and values objects are defined here."""

from collections.abc import Callable
from datetime import date
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy.sql import delete, func, insert, select, update

from idu_api.common.db.entities import (
    service_types_dict,
    soc_value_indicators_data,
    soc_group_values_data,
    soc_groups_dict,
    soc_values_dict,
    soc_values_service_types_dict,
    territories_data,
    urban_functions_dict,
)
from idu_api.urban_api.dto import (
    ServiceTypeDTO,
    SocGroupDTO,
    SocValueIndicatorValueDTO,
    SocValueWithServiceTypesDTO,
    SocGroupWithServiceTypesDTO,
    SocValueDTO,
)
from idu_api.urban_api.exceptions.logic.common import EntityAlreadyExists, EntityNotFoundById, EntityNotFoundByParams
from idu_api.urban_api.logic.impl.helpers.soc_groups import (
    add_service_type_to_social_group_to_db,
    add_service_type_to_social_value_to_db,
    add_social_value_indicator_value_to_db,
    add_social_group_to_db,
    add_social_value_to_db,
    add_value_to_social_group_from_db,
    delete_social_group_from_db,
    delete_social_value_indicator_value_from_db,
    delete_social_value_from_db,
    get_service_types_by_social_value_id_from_db,
    get_social_group_by_id_from_db,
    get_social_value_indicator_values_from_db,
    get_social_groups_from_db,
    get_social_value_with_service_types_by_id_from_db,
    get_social_value_by_id_from_db,
    get_social_values_from_db,
    put_social_value_indicator_value_to_db,
)
from idu_api.urban_api.logic.impl.helpers.utils import extract_values_from_model
from idu_api.urban_api.schemas import (
    ServiceType,
    SocGroup,
    SocValueIndicatorValue,
    SocValueIndicatorValuePost,
    SocValueIndicatorValuePut,
    SocGroupPost,
    ServiceTypePost,
    SocGroupWithServiceTypes,
    SocValue,
    SocValuePost,
    SocValueWithServiceTypes,
)
from tests.urban_api.helpers.connection import MockConnection

func: Callable


####################################################################################
#                           Default use-case tests                                 #
####################################################################################


@pytest.mark.asyncio
async def test_get_social_groups_from_db(mock_conn: MockConnection):
    """Test the get_measurement_units_from_db function."""

    # Arrange
    statement = select(soc_groups_dict).order_by(soc_groups_dict.c.soc_group_id)

    # Act
    result = await get_social_groups_from_db(mock_conn)

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(isinstance(item, SocGroupDTO) for item in result), "Each item should be a SocGroupDTO."
    assert isinstance(SocGroup.from_dto(result[0]), SocGroup), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_called_once_with(str(statement))


@pytest.mark.asyncio
async def test_get_social_group_by_id_from_db(mock_conn: MockConnection):
    """Test the get_social_group_by_id_from_db function."""

    # Arrange
    soc_group_id = 1
    statement = (
        select(
            soc_groups_dict,
            service_types_dict,
            urban_functions_dict.c.name.label("urban_function_name")
        )
        .select_from(
            soc_groups_dict.outerjoin(
                soc_group_values_data,
                soc_group_values_data.c.soc_group_id == soc_groups_dict.c.soc_group_id,
            ).outerjoin(
                service_types_dict,
                service_types_dict.c.service_type_id == soc_group_values_data.c.service_type_id
            )
            .outerjoin(
                urban_functions_dict,
                urban_functions_dict.c.urban_function_id == service_types_dict.c.urban_function_id
            )
        )
        .where(soc_groups_dict.c.soc_group_id == soc_group_id)
        .order_by(service_types_dict.c.service_type_id)
        .distinct()
    )

    # Act
    result = await get_social_group_by_id_from_db(mock_conn, soc_group_id)

    # Assert
    assert isinstance(result, SocGroupWithServiceTypesDTO), "Result should be a SocGroupWithServiceTypesDTO."
    assert isinstance(
        SocGroupWithServiceTypes.from_dto(result), SocGroupWithServiceTypes
    ), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_called_once_with(str(statement))


@pytest.mark.asyncio
async def test_add_social_group_to_db(mock_conn: MockConnection, soc_group_post_req: SocGroupPost):
    """Test the add_social_group_to_db function."""

    # Arrange
    statement = (
        insert(soc_groups_dict)
        .values(**extract_values_from_model(soc_group_post_req))
        .returning(soc_groups_dict.c.soc_group_id)
    )

    # Act
    with patch("idu_api.urban_api.logic.impl.helpers.soc_groups.check_existence") as mock_check_existence:
        with pytest.raises(EntityAlreadyExists):
            await add_social_group_to_db(mock_conn, soc_group_post_req)
        mock_check_existence.return_value = False
        result = await add_social_group_to_db(mock_conn, soc_group_post_req)

    # Assert
    assert isinstance(result, SocGroupWithServiceTypesDTO), "Result should be a SocGroupWithServiceTypesDTO."
    assert isinstance(
        SocGroupWithServiceTypes.from_dto(result), SocGroupWithServiceTypes
    ), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))
    mock_conn.commit_mock.assert_called_once()


@pytest.mark.asyncio
async def add_service_type_to_social_group_to_db(
    mock_conn: MockConnection,
    soc_group_service_type_post_req: ServiceTypePost,
):
    """Test the add_service_type_to_social_group_from_db function."""

    # Arrange
    soc_group_id = 1

    async def check_soc_group(conn, table, conditions):
        if table == soc_groups_dict:
            return False
        return True

    async def check_service_type(conn, table, conditions):
        if table == service_types_dict:
            return False
        return True

    async def check_soc_group_value(conn, table, conditions):
        if table == soc_group_values_data:
            return False
        return True

    statement = (
        insert(soc_group_values_data)
        .values(soc_group_id=soc_group_id, **extract_values_from_model(soc_group_service_type_post_req))
        .returning(soc_group_values_data.c.soc_group_id)
    )

    # Act
    with pytest.raises(EntityAlreadyExists):
        await add_service_type_to_social_group_to_db(mock_conn, soc_group_id, soc_group_service_type_post_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.soc_groups.check_existence",
        new=AsyncMock(side_effect=check_soc_group),
    ):
        with pytest.raises(EntityNotFoundById):
            await add_service_type_to_social_group_to_db(mock_conn, soc_group_id, soc_group_service_type_post_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.soc_groups.check_existence",
        new=AsyncMock(side_effect=check_service_type),
    ):
        with pytest.raises(EntityNotFoundById):
            await add_service_type_to_social_group_to_db(mock_conn, soc_group_id, soc_group_service_type_post_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.soc_groups.check_existence",
        new=AsyncMock(side_effect=check_soc_group_value),
    ):
        result = await add_service_type_to_social_group_to_db(
            mock_conn, soc_group_id, soc_group_service_type_post_req
        )

    # Assert
    assert isinstance(result, SocGroupWithServiceTypesDTO), "Result should be a SocGroupWithServiceTypesDTO."
    assert isinstance(
        SocGroupWithServiceTypes.from_dto(result), SocGroupWithServiceTypes
    ), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))
    mock_conn.commit_mock.assert_called_once()

@pytest.mark.asyncio
async def test_add_service_type_to_social_value_to_db(mock_conn: MockConnection):
    """Test the add_service_type_to_social_value_to_db function."""

    # Arrange
    soc_value_id = 1
    service_type_id = 1

    async def check_soc_value(conn, table, conditions):
        if table == soc_values_dict:
            return False
        return True

    async def check_service(conn, table, conditions):
        if table == service_types_dict:
            return False
        return True

    async def check_conn(conn, table, conditions):
        if table == soc_values_service_types_dict:
            return False
        return True

    statement = (
        insert(soc_values_service_types_dict)
        .values(soc_value_id=soc_value_id, service_type_id=service_type_id)
        .returning(soc_values_service_types_dict.c.soc_value_id)
    )

    # Act

    with pytest.raises(EntityAlreadyExists):
        await add_service_type_to_social_value_to_db(mock_conn, soc_value_id, service_type_id)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.soc_groups.check_existence",
        new=AsyncMock(side_effect=check_soc_value),
    ):
        with pytest.raises(EntityNotFoundById):
            await add_service_type_to_social_value_to_db(mock_conn, soc_value_id, service_type_id)

    with patch(
        "idu_api.urban_api.logic.impl.helpers.soc_groups.check_existence",
        new=AsyncMock(side_effect=check_service),
    ):
        with pytest.raises(EntityNotFoundById):
            await add_service_type_to_social_value_to_db(mock_conn, soc_value_id, service_type_id)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.soc_groups.check_existence",
        new=AsyncMock(side_effect=check_conn),
    ):
        result = await add_service_type_to_social_value_to_db(mock_conn, soc_value_id, service_type_id)

    # Assert
    assert isinstance(result, SocValueWithServiceTypesDTO), "Result should be a SocValueWithServiceTypesDTO."
    assert isinstance(
        SocValueWithServiceTypes.from_dto(result), SocValueWithServiceTypes
    ), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))
    mock_conn.commit_mock.assert_called_once()

@pytest.mark.asyncio
async def test_delete_social_group_from_db(mock_conn: MockConnection):
    """Test the delete_social_group_from_db function."""

    # Arrange
    soc_group_id = 1
    statement = delete(soc_groups_dict).where(soc_groups_dict.c.soc_group_id == soc_group_id)

    # Act
    with patch("idu_api.urban_api.logic.impl.helpers.soc_groups.check_existence") as mock_check_existence:
        result = await delete_social_group_from_db(mock_conn, soc_group_id)
        mock_check_existence.return_value = False
        with pytest.raises(EntityNotFoundById):
            await delete_social_group_from_db(mock_conn, soc_group_id)

    # Assert
    assert result == {"status": "ok"}, "Result should be {'status': 'ok'}."
    mock_conn.execute_mock.assert_called_once_with(str(statement))
    mock_conn.commit_mock.assert_called_once()


@pytest.mark.asyncio
async def test_get_social_values_from_db(mock_conn: MockConnection):
    """Test the get_measurement_units_from_db function."""

    # Arrange
    statement = select(soc_values_dict).order_by(soc_values_dict.c.soc_value_id)

    # Act
    result = await get_social_values_from_db(mock_conn)

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(isinstance(item, SocValueDTO) for item in result), "Each item should be a SocValueDTO."
    assert isinstance(SocValue.from_dto(result[0]), SocValue), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_called_once_with(str(statement))


@pytest.mark.asyncio
async def test_get_social_value_by_id_from_db(mock_conn: MockConnection):
    """Test the get_social_value_by_id_from_db"""

    # Arrange
    soc_value_id = 1

    async def check_soc_value(conn, table, conditions):
        if table == soc_values_dict:
            return False
        return True

    statement = select(soc_values_dict).where(soc_values_dict.c.soc_value_id == soc_value_id)

    # Act
    with patch(
        "idu_api.urban_api.logic.impl.helpers.soc_groups.check_existence",
        new=AsyncMock(side_effect=check_soc_value)
    ):
        with pytest.raises(EntityNotFoundById):
            await get_social_value_by_id_from_db(mock_conn, soc_value_id)

    result = await get_social_value_by_id_from_db(mock_conn, soc_value_id)

    # Assert
    assert isinstance(result, SocValueDTO), "Result should be a SocValueDTO."
    assert isinstance(
        SocValue.from_dto(result), SocValue
    ), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))


@pytest.mark.asyncio
async def test_get_social_value_with_service_types_by_id_from_db(mock_conn: MockConnection):
    """Test the get_social_value_with_service_types_by_id_from_db function."""

    # Arrange
    soc_value_id = 1

    async def check_soc_value(conn, table, conditions):
        if table == soc_values_dict:
            return False
        return True

    select_from = (
        soc_values_dict
        .join(
            soc_values_service_types_dict,
            soc_values_service_types_dict.c.soc_value_id == soc_values_dict.c.soc_value_id
        )
        .join(
            service_types_dict,
            service_types_dict.c.service_type_id == soc_values_service_types_dict.c.service_type_id,
        )
        .join(
            urban_functions_dict,
            urban_functions_dict.c.urban_function_id == service_types_dict.c.urban_function_id
        )
    )

    statement = select(
        soc_values_dict,
        service_types_dict.c.service_type_id,
        service_types_dict.c.urban_function_id,
        service_types_dict.c.name.label("service_type_name"),
        service_types_dict.c.capacity_modeled,
        service_types_dict.c.code,
        service_types_dict.c.infrastructure_type,
        service_types_dict.c.properties,
        urban_functions_dict.c.name.label("urban_function_name")
    ).select_from(select_from).where(soc_values_dict.c.soc_value_id == soc_value_id).distinct()

    # Act
    with patch(
        "idu_api.urban_api.logic.impl.helpers.soc_groups.check_existence",
        new=AsyncMock(side_effect=check_soc_value)
    ):
        with pytest.raises(EntityNotFoundById):
            await get_social_value_with_service_types_by_id_from_db(mock_conn, soc_value_id)

    result = await get_social_value_with_service_types_by_id_from_db(mock_conn, soc_value_id)

    # Assert
    assert isinstance(result, SocValueWithServiceTypesDTO), "Result should be a SocValueWithServiceTypesDTO."
    assert isinstance(
        SocValueWithServiceTypes.from_dto(result), SocValueWithServiceTypes
    ), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))


@pytest.mark.asyncio
async def test_add_social_value_to_db(mock_conn: MockConnection, soc_value_post_req: SocValuePost):
    """Test the add_social_value_to_db function."""

    # Arrange
    statement = (
        insert(soc_values_dict)
        .values(**extract_values_from_model(soc_value_post_req))
        .returning(soc_values_dict.c.soc_value_id)
    )

    # Act
    with patch("idu_api.urban_api.logic.impl.helpers.soc_groups.check_existence") as mock_check_existence:
        mock_check_existence.side_effect = [True, False, True]
        with pytest.raises(EntityAlreadyExists):
            await add_social_value_to_db(mock_conn, soc_value_post_req)
        mock_check_existence.return_value = False
        result = await add_social_value_to_db(mock_conn, soc_value_post_req)

    # Assert
    assert isinstance(result, SocValueDTO), "Result should be a SocValueDTO."
    assert isinstance(
        SocValue.from_dto(result), SocValue
    ), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))
    mock_conn.commit_mock.assert_called_once()


@pytest.mark.asyncio
async def test_add_value_to_social_group_from_db(mock_conn: MockConnection):
    """Test the add_value_to_social_group_from_db function."""

    # Arrange
    soc_group_id = 1
    service_type_id = 1
    soc_value_id = 2

    async def check_soc_group(conn, table, conditions):
        if table == soc_groups_dict:
            return False
        return True

    async def check_service_type(conn, table, conditions):
        if table == service_types_dict:
            return False
        return True

    async def check_indicator_value(conn, table, conditions):
        if table == soc_group_values_data:
            return False
        return True

    statement = insert(soc_group_values_data).values(
        soc_group_id=soc_group_id,
        service_type_id=service_type_id,
        soc_value_id=soc_value_id,
        infrastructure_type="basic",
    )

    # Act
    with pytest.raises(EntityAlreadyExists):
        await add_value_to_social_group_from_db(mock_conn, soc_group_id, service_type_id, 1)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.soc_groups.check_existence",
        new=AsyncMock(side_effect=check_soc_group),
    ):
        with pytest.raises(EntityNotFoundById):
            await add_value_to_social_group_from_db(mock_conn, soc_group_id, service_type_id, soc_value_id)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.soc_groups.check_existence",
        new=AsyncMock(side_effect=check_service_type),
    ):
        with pytest.raises(EntityNotFoundById):
            await add_value_to_social_group_from_db(mock_conn, soc_group_id, service_type_id, soc_value_id)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.soc_groups.check_existence",
        new=AsyncMock(side_effect=check_indicator_value),
    ):
        result = await add_value_to_social_group_from_db(mock_conn, soc_group_id, service_type_id, soc_value_id)

    # Assert
    assert isinstance(result, SocValueDTO), "Result should be a SocValueDTO."
    assert isinstance(
        SocValue.from_dto(result), SocValue
    ), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))
    mock_conn.commit_mock.assert_called_once()


@pytest.mark.asyncio
async def test_delete_social_value_from_db(mock_conn: MockConnection):
    """Test the delete_social_value_from_db function."""

    # Arrange
    soc_value_id = 1
    statement = delete(soc_values_dict).where(soc_values_dict.c.soc_value_id == soc_value_id)

    # Act
    with patch("idu_api.urban_api.logic.impl.helpers.soc_groups.check_existence") as mock_check_existence:
        result = await delete_social_value_from_db(mock_conn, soc_value_id)
        mock_check_existence.return_value = False
        with pytest.raises(EntityNotFoundById):
            await delete_social_value_from_db(mock_conn, soc_value_id)

    # Assert
    assert result == {"status": "ok"}, "Result should be {'status': 'ok'}."
    mock_conn.execute_mock.assert_called_once_with(str(statement))
    mock_conn.commit_mock.assert_called_once()


@pytest.mark.asyncio
async def test_get_social_value_indicator_values_from_db(mock_conn: MockConnection):
    """Test the get_social_value_indicator_values_from_db function."""

    # Arrange
    soc_value_id = 1

    async def check_soc_value(conn, table, conditions):
        if table == soc_values_dict:
            return False
        return True

    async def check_territory(conn, table, conditions):
        if table == territories_data:
            return False
        return True

    select_from = (
        soc_value_indicators_data
        .join(
            soc_values_dict,
            soc_values_dict.c.soc_value_id == soc_value_indicators_data.c.soc_value_id,
        )
        .join(
            territories_data,
            territories_data.c.territory_id == soc_value_indicators_data.c.territory_id)
    )

    subquery = (
        select(
            soc_value_indicators_data.c.soc_value_id,
            soc_value_indicators_data.c.territory_id,
            func.max(soc_value_indicators_data.c.year).label("max_date"),
        )
        .group_by(
            soc_value_indicators_data.c.soc_value_id,
            soc_value_indicators_data.c.territory_id,
        )
        .subquery()
    )

    last_only_select_from = select_from.join(
        subquery,
        (soc_value_indicators_data.c.soc_value_id == subquery.c.soc_value_id)
        & (soc_value_indicators_data.c.territory_id == subquery.c.territory_id)
        & (soc_value_indicators_data.c.year == subquery.c.max_date),
    )

    statement = select(
        soc_value_indicators_data,
        soc_values_dict.c.name.label("soc_value_name"),
        territories_data.c.name.label("territory_name"),
    ).select_from(select_from).where(soc_value_indicators_data.c.soc_value_id == soc_value_id)

    last_only_statement = select(
        soc_value_indicators_data,
        soc_values_dict.c.name.label("soc_value_name"),
        territories_data.c.name.label("territory_name"),
    ).select_from(last_only_select_from).where(soc_value_indicators_data.c.soc_value_id == soc_value_id)

    params = {"territory_id": 1, "year": date.today().year}
    statement_with_filters = select(
        soc_value_indicators_data,
        soc_values_dict.c.name.label("soc_value_name"),
        territories_data.c.name.label("territory_name"),
    ).select_from(select_from).where(
        soc_value_indicators_data.c.soc_value_id == soc_value_id,
        soc_value_indicators_data.c.territory_id == params["territory_id"],
        soc_value_indicators_data.c.year == params["year"],
    )

    # Act
    with patch(
        "idu_api.urban_api.logic.impl.helpers.soc_groups.check_existence",
        new=AsyncMock(side_effect=check_soc_value),
    ):
        with pytest.raises(EntityNotFoundById):
            await get_social_value_indicator_values_from_db(mock_conn, soc_value_id, **params, last_only=False)

    with patch(
        "idu_api.urban_api.logic.impl.helpers.soc_groups.check_existence",
        new=AsyncMock(side_effect=check_territory),
    ):
        with pytest.raises(EntityNotFoundById):
            await get_social_value_indicator_values_from_db(mock_conn, soc_value_id, **params, last_only=False)
    mock_conn.execute_mock.reset_mock()

    await get_social_value_indicator_values_from_db(mock_conn, soc_value_id, None, None, last_only=False)
    await get_social_value_indicator_values_from_db(mock_conn, soc_value_id, None, None, last_only=True)
    result = await get_social_value_indicator_values_from_db(mock_conn, soc_value_id, **params, last_only=False)

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(
        isinstance(item, SocValueIndicatorValueDTO) for item in result
    ), "Each item should be a SocValueIndicatorValueDTO."
    assert isinstance(
        SocValueIndicatorValue.from_dto(result[0]), SocValueIndicatorValue
    ), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))
    mock_conn.execute_mock.assert_any_call(str(last_only_statement))
    mock_conn.execute_mock.assert_any_call(str(statement_with_filters))


@pytest.mark.asyncio
async def test_add_social_value_indicator_value_to_db(
    mock_conn: MockConnection, soc_value_indicator_post_req: SocValueIndicatorValuePost
):
    """Test the add_social_value_indicator_value_to_db function."""

    # Arrange

    async def check_soc_value(conn, table, conditions):
        if table == soc_values_dict:
            return False
        return True

    async def check_territory(conn, table, conditions):
        if table == territories_data:
            return False
        return True

    async def check_soc_value_indicator(conn, table, conditions):
        if table == soc_value_indicators_data:
            return False
        return True

    statement = (
        insert(soc_value_indicators_data)
        .values(**soc_value_indicator_post_req.model_dump())
        .returning(soc_value_indicators_data)
    )

    # Act
    with pytest.raises(EntityAlreadyExists):
        await add_social_value_indicator_value_to_db(mock_conn, soc_value_indicator_post_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.soc_groups.check_existence",
        new=AsyncMock(side_effect=check_soc_value),
    ):
        with pytest.raises(EntityNotFoundById):
            await add_social_value_indicator_value_to_db(mock_conn, soc_value_indicator_post_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.soc_groups.check_existence",
        new=AsyncMock(side_effect=check_territory),
    ):
        with pytest.raises(EntityNotFoundById):
            await add_social_value_indicator_value_to_db(mock_conn, soc_value_indicator_post_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.soc_groups.check_existence",
        new=AsyncMock(side_effect=check_soc_value_indicator),
    ):
        result = await add_social_value_indicator_value_to_db(mock_conn, soc_value_indicator_post_req)

    # Assert
    assert isinstance(result, SocValueIndicatorValueDTO), "Result should be an SocValueIndicatorValueDTO."
    assert isinstance(
        SocValueIndicatorValue.from_dto(result), SocValueIndicatorValue
    ), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))
    mock_conn.commit_mock.assert_called_once()


@pytest.mark.asyncio
async def test_put_social_value_indicator_value_to_db(
    mock_conn: MockConnection, soc_value_indicator_put_req: SocValueIndicatorValuePut
):
    """Test the put_social_value_indicator_value_to_db function."""

    # Arrange

    async def check_soc_value(conn, table, conditions):
        if table == soc_values_dict:
            return False
        return True

    async def check_territory(conn, table, conditions):
        if table == territories_data:
            return False
        return True

    async def check_soc_value_indicator(conn, table, conditions):
        if table == soc_value_indicators_data:
            return False
        return True

    statement_insert = (
        insert(soc_value_indicators_data)
        .values(**soc_value_indicator_put_req.model_dump())
        .returning(soc_value_indicators_data)
    )
    statement_update = (
        update(soc_value_indicators_data)
        .values(**extract_values_from_model(soc_value_indicator_put_req, to_update=True))
        .where(
            soc_value_indicators_data.c.soc_value_id == soc_value_indicator_put_req.soc_value_id,
            soc_value_indicators_data.c.territory_id == soc_value_indicator_put_req.territory_id,
            soc_value_indicators_data.c.year == soc_value_indicator_put_req.year,
        )
        .returning(soc_value_indicators_data)
    )

    # Act
    await put_social_value_indicator_value_to_db(mock_conn, soc_value_indicator_put_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.soc_groups.check_existence",
        new=AsyncMock(side_effect=check_soc_value),
    ):
        with pytest.raises(EntityNotFoundById):
            await put_social_value_indicator_value_to_db(mock_conn, soc_value_indicator_put_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.soc_groups.check_existence",
        new=AsyncMock(side_effect=check_territory),
    ):
        with pytest.raises(EntityNotFoundById):
            await put_social_value_indicator_value_to_db(mock_conn, soc_value_indicator_put_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.soc_groups.check_existence",
        new=AsyncMock(side_effect=check_soc_value_indicator),
    ):
        result = await put_social_value_indicator_value_to_db(mock_conn, soc_value_indicator_put_req)

    # Assert
    assert isinstance(result, SocValueIndicatorValueDTO), "Result should be an SocValueIndicatorValueDTO."
    assert isinstance(
        SocValueIndicatorValue.from_dto(result), SocValueIndicatorValue
    ), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement_insert))
    mock_conn.execute_mock.assert_any_call(str(statement_update))
    assert mock_conn.commit_mock.call_count == 2, "Commit mock count should be one for one method."


@pytest.mark.asyncio
async def test_delete_social_value_indicator_value_from_db(mock_conn: MockConnection):
    """Test the delete_social_value_indicator_value_from_db function."""

    # Arrange
    soc_value_id = 1
    territory_id = 1
    year = date.today().year

    async def check_soc_value(conn, table, conditions):
        if table == soc_values_dict:
            return False
        return True

    async def check_territory(conn, table, conditions):
        if table == territories_data:
            return False
        return True

    statement = (
        delete(soc_value_indicators_data)
        .where(
            soc_value_indicators_data.c.soc_value_id == soc_value_id,
            soc_value_indicators_data.c.territory_id == territory_id,
            soc_value_indicators_data.c.year == year,
        )
    )

    # Act
    with patch(
        "idu_api.urban_api.logic.impl.helpers.soc_groups.check_existence",
        new=AsyncMock(side_effect=check_soc_value),
    ):
        with pytest.raises(EntityNotFoundById):
            result = await delete_social_value_indicator_value_from_db(mock_conn, soc_value_id, territory_id, year)

    with patch(
        "idu_api.urban_api.logic.impl.helpers.soc_groups.check_existence",
        new=AsyncMock(side_effect=check_territory),
    ):
        with pytest.raises(EntityNotFoundById):
            result = await delete_social_value_indicator_value_from_db(mock_conn, soc_value_id, territory_id, year)

    with patch(
        "idu_api.urban_api.logic.impl.helpers.soc_groups.check_existence",
    ) as mock_check_existence:
        result = await delete_social_value_indicator_value_from_db(mock_conn, soc_value_id, territory_id, year)

        mock_check_existence.return_value = False
        mock_check_existence.side_effect = [True, True, False]
        with pytest.raises(EntityNotFoundByParams):
            await delete_social_value_indicator_value_from_db(mock_conn, soc_value_id, territory_id, year)

    # Assert
    assert result == {"status": "ok"}, "Result should be {'status': 'ok'}."
    mock_conn.execute_mock.assert_called_once_with(str(statement))
    mock_conn.commit_mock.assert_called_once()


@pytest.mark.asyncio
async def test_get_service_types_by_social_value_id_from_db(mock_conn: MockConnection):
    """Test the get_service_types_by_social_value_id_from_db function."""

    # Arrange
    social_value_id = 1
    statement = (
        select(service_types_dict, urban_functions_dict.c.name.label("urban_function_name"))
        .select_from(
            soc_values_service_types_dict.join(
                service_types_dict,
                service_types_dict.c.service_type_id == soc_values_service_types_dict.c.service_type_id,
            ).join(
                urban_functions_dict, urban_functions_dict.c.urban_function_id == service_types_dict.c.urban_function_id
            )
        )
        .where(soc_values_service_types_dict.c.soc_value_id == social_value_id)
        .order_by(service_types_dict.c.service_type_id)
    )

    # Act
    with patch("idu_api.urban_api.logic.impl.helpers.soc_groups.check_existence") as mock_check_existence:
        result = await get_service_types_by_social_value_id_from_db(mock_conn, social_value_id)
        mock_check_existence.return_value = False
        with pytest.raises(EntityNotFoundById):
            await get_service_types_by_social_value_id_from_db(mock_conn, social_value_id)

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(isinstance(item, ServiceTypeDTO) for item in result), "Each item should be a ServiceTypeDTO."
    assert isinstance(ServiceType.from_dto(result[0]), ServiceType), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_called_once_with(str(statement))
