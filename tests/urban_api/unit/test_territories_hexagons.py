"""Unit tests for territory-related hexagons are defined here."""

from unittest.mock import AsyncMock, patch

import pytest
from geoalchemy2.functions import ST_AsEWKB, ST_GeomFromWKB
from sqlalchemy import delete, insert, select, text

from idu_api.common.db.entities import hexagons_data, territories_data
from idu_api.urban_api.dto import HexagonDTO
from idu_api.urban_api.exceptions.logic.common import (
    EntitiesNotFoundByIds,
    EntityAlreadyExists,
    EntityNotFoundById,
    TooManyObjectsError,
)
from idu_api.urban_api.logic.impl.helpers.territories_hexagons import (
    add_hexagons_by_territory_id_to_db,
    delete_hexagons_by_territory_id_from_db,
    get_hexagons_by_ids,
    get_hexagons_by_territory_id_from_db,
)
from idu_api.urban_api.logic.impl.helpers.utils import (
    OBJECTS_NUMBER_LIMIT,
    SRID,
)
from idu_api.urban_api.schemas import Hexagon, HexagonAttributes, HexagonPost
from idu_api.urban_api.schemas.geometries import GeoJSONResponse
from tests.urban_api.helpers.connection import MockConnection

####################################################################################
#                           Default use-case tests                                 #
####################################################################################


@pytest.mark.asyncio
async def test_get_hexagons_by_ids(mock_conn: MockConnection):
    """Test the get_hexagons_by_ids function."""

    # Arrange
    ids = [1]
    not_found_ids = [1, 2]
    too_many_ids = list(range(OBJECTS_NUMBER_LIMIT + 1))
    statement = (
        select(
            hexagons_data.c.hexagon_id,
            hexagons_data.c.territory_id,
            ST_AsEWKB(hexagons_data.c.geometry).label("geometry"),
            ST_AsEWKB(hexagons_data.c.centre_point).label("centre_point"),
            hexagons_data.c.properties,
            territories_data.c.name.label("territory_name"),
        )
        .select_from(
            hexagons_data.join(territories_data, hexagons_data.c.territory_id == territories_data.c.territory_id)
        )
        .where(hexagons_data.c.hexagon_id.in_(ids))
    )

    # Act
    with pytest.raises(EntitiesNotFoundByIds):
        await get_hexagons_by_ids(mock_conn, not_found_ids)
    with pytest.raises(TooManyObjectsError):
        await get_hexagons_by_ids(mock_conn, too_many_ids)
    result = await get_hexagons_by_ids(mock_conn, ids)

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(isinstance(item, HexagonDTO) for item in result), "Each item should be an HexagonDTO."
    assert isinstance(Hexagon.from_dto(result[0]), Hexagon), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))


@pytest.mark.asyncio
async def test_get_hexagons_by_territory_id_from_db(mock_conn: MockConnection):
    """Test the get_hexagons_by_territory_id_from_db function."""

    # Arrange
    territory_id = 1
    statement = (
        select(
            hexagons_data.c.hexagon_id,
            hexagons_data.c.territory_id,
            ST_AsEWKB(hexagons_data.c.geometry).label("geometry"),
            ST_AsEWKB(hexagons_data.c.centre_point).label("centre_point"),
            hexagons_data.c.properties,
            territories_data.c.name.label("territory_name"),
        )
        .select_from(
            hexagons_data.join(territories_data, hexagons_data.c.territory_id == territories_data.c.territory_id)
        )
        .where(hexagons_data.c.territory_id == territory_id)
    )

    # Act
    with patch("idu_api.urban_api.logic.impl.helpers.territories_hexagons.check_existence") as mock_check_existence:
        mock_check_existence.return_value = False
        with pytest.raises(EntityNotFoundById):
            await get_hexagons_by_territory_id_from_db(mock_conn, territory_id)
    result = await get_hexagons_by_territory_id_from_db(mock_conn, territory_id)
    geojson_result = await GeoJSONResponse.from_list([r.to_geojson_dict() for r in result])

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(isinstance(item, HexagonDTO) for item in result), "Each item should be a HexagonDTO."
    assert isinstance(Hexagon.from_dto(result[0]), Hexagon), "Couldn't create pydantic model from DTO."
    assert isinstance(
        HexagonAttributes(**geojson_result.features[0].properties), HexagonAttributes
    ), "Couldn't create pydantic model from geojson properties."
    mock_conn.execute_mock.assert_any_call(str(statement))


@pytest.mark.asyncio
async def test_add_hexagons_by_territory_id_to_db(mock_conn: MockConnection, hexagon_post_req: HexagonPost):
    """Test the add_hexagons_by_territory_id_to_db function."""

    # Arrange
    territory_id = 1

    async def check_territory_not_found(conn, table, conditions):
        if table == territories_data:
            return False
        return True

    async def check_hexagons_already_exist(conn, table, conditions):
        if table == hexagons_data:
            return False
        return True

    insert_values = [
        {
            "territory_id": territory_id,
            "geometry": ST_GeomFromWKB(hexagon_post_req.geometry.as_shapely_geometry().wkb, text(str(SRID))),
            "centre_point": ST_GeomFromWKB(hexagon_post_req.centre_point.as_shapely_geometry().wkb, text(str(SRID))),
            "properties": hexagon_post_req.properties,
        }
    ]
    statement = insert(hexagons_data).values(insert_values).returning(hexagons_data.c.hexagon_id)

    # Act
    with pytest.raises(EntityAlreadyExists):
        await add_hexagons_by_territory_id_to_db(mock_conn, territory_id, [hexagon_post_req])
    with patch(
        "idu_api.urban_api.logic.impl.helpers.territories_hexagons.check_existence",
        new=AsyncMock(side_effect=check_territory_not_found),
    ):
        with pytest.raises(EntityNotFoundById):
            await add_hexagons_by_territory_id_to_db(mock_conn, territory_id, [hexagon_post_req])

    with patch(
        "idu_api.urban_api.logic.impl.helpers.territories_hexagons.check_existence",
        new=AsyncMock(side_effect=check_hexagons_already_exist),
    ):
        result = await add_hexagons_by_territory_id_to_db(mock_conn, territory_id, [hexagon_post_req])

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(isinstance(item, HexagonDTO) for item in result), "Each item should be a HexagonDTO."
    assert isinstance(Hexagon.from_dto(result[0]), Hexagon), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))
    mock_conn.commit_mock.assert_called_once()


@pytest.mark.asyncio
async def test_delete_hexagons_by_territory_id_from_db(mock_conn: MockConnection):
    """Test the delete_hexagons_by_territory_id_from_db function."""

    # Arrange
    territory_id = 1
    statement = delete(hexagons_data).where(hexagons_data.c.territory_id == territory_id)

    # Act
    with patch("idu_api.urban_api.logic.impl.helpers.territories_hexagons.check_existence") as mock_check_existence:
        mock_check_existence.return_value = False
        with pytest.raises(EntityNotFoundById):
            await delete_hexagons_by_territory_id_from_db(mock_conn, territory_id)
    result = await delete_hexagons_by_territory_id_from_db(mock_conn, territory_id)

    # Assert
    assert result == {"status": "ok"}, "Result should be {'status': 'ok'}."
    mock_conn.execute_mock.assert_any_call(str(statement))
    mock_conn.commit_mock.assert_called_once()
