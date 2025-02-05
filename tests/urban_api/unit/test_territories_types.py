"""Unit tests for territory types are defined here."""

from unittest.mock import patch

import pytest
from sqlalchemy.sql import insert, select

from idu_api.common.db.entities import territory_types_dict
from idu_api.urban_api.dto import TerritoryTypeDTO
from idu_api.urban_api.exceptions.logic.common import EntityAlreadyExists
from idu_api.urban_api.logic.impl.helpers.territories_types import add_territory_type_to_db, get_territory_types_from_db
from idu_api.urban_api.schemas import TerritoryType, TerritoryTypePost
from tests.urban_api.helpers.connection import MockConnection

####################################################################################
#                           Default use-case tests                                 #
####################################################################################


@pytest.mark.asyncio
async def test_get_territory_types_from_db(mock_conn: MockConnection):
    """Test the get_territory_types_from_db function."""

    # Arrange
    statement = select(territory_types_dict).order_by(territory_types_dict.c.territory_type_id)

    # Act
    result = await get_territory_types_from_db(mock_conn)

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(isinstance(item, TerritoryTypeDTO) for item in result), "Each item should be a TerritoryTypeDTO."
    assert isinstance(TerritoryType.from_dto(result[0]), TerritoryType), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_called_once_with(str(statement))


@pytest.mark.asyncio
async def test_add_territory_type_to_db(mock_conn: MockConnection, territory_type_post_req: TerritoryTypePost):
    """Test the add_territory_type_to_db function."""

    # Arrange
    statement = (
        insert(territory_types_dict).values(**territory_type_post_req.model_dump()).returning(territory_types_dict)
    )

    # Act
    with pytest.raises(EntityAlreadyExists):
        await add_territory_type_to_db(mock_conn, territory_type_post_req)
    with patch("idu_api.urban_api.logic.impl.helpers.territories_types.check_existence") as mock_check_existence:
        mock_check_existence.return_value = False
        result = await add_territory_type_to_db(mock_conn, territory_type_post_req)

    # Assert
    assert isinstance(result, TerritoryTypeDTO), "Result should be a TerritoryTypeDTO."
    assert isinstance(TerritoryType.from_dto(result), TerritoryType), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))
    mock_conn.commit_mock.assert_called_once()
