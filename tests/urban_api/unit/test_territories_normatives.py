"""Unit tests for territory-related normative objects are defined here."""

from dataclasses import asdict
from datetime import date, datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest
from geoalchemy2.functions import ST_AsGeoJSON
from sqlalchemy import and_, cast, delete, insert, literal, select, update
from sqlalchemy.dialects.postgresql import JSONB

from idu_api.common.db.entities import (
    service_types_dict,
    service_types_normatives_data,
    territories_data,
    urban_functions_dict,
)
from idu_api.urban_api.dto import NormativeDTO, TerritoryWithNormativesDTO
from idu_api.urban_api.exceptions.logic.common import (
    EntitiesNotFoundByIds,
    EntityAlreadyExists,
    EntityNotFoundById,
    TooManyObjectsError,
)
from idu_api.urban_api.logic.impl.helpers.territories_normatives import (
    add_normatives_to_territory_to_db,
    delete_normatives_by_territory_id_in_db,
    get_normatives_by_ids_from_db,
    get_normatives_by_territory_id_from_db,
    get_normatives_values_by_parent_id_from_db,
    patch_normatives_by_territory_id_in_db,
    put_normatives_by_territory_id_in_db,
)
from idu_api.urban_api.logic.impl.helpers.utils import DECIMAL_PLACES, OBJECTS_NUMBER_LIMIT
from idu_api.urban_api.schemas import Normative, NormativeDelete, NormativePatch, NormativePost, TerritoryWithNormatives
from idu_api.urban_api.schemas.geometries import GeoJSONResponse
from tests.urban_api.helpers.connection import MockConnection

####################################################################################
#                           Default use-case tests                                 #
####################################################################################


@pytest.mark.asyncio
async def test_get_normatives_by_territory_id_from_db(mock_conn: MockConnection):
    """Test the get_normatives_by_territory_id_from_db function."""

    # Arrange
    territory_id = 1
    year = date.today().year

    cte_statement = (
        select(
            territories_data.c.territory_id,
            territories_data.c.name,
            territories_data.c.parent_id,
            territories_data.c.is_city,
            territories_data.c.level,
        )
        .where(territories_data.c.territory_id == territory_id)
        .cte(name="territories_recursive", recursive=True)
    )
    recursive_part = select(
        territories_data.c.territory_id,
        territories_data.c.name,
        territories_data.c.parent_id,
        territories_data.c.is_city,
        territories_data.c.level,
    ).join(cte_statement, territories_data.c.territory_id == cte_statement.c.parent_id)
    cte_statement = cte_statement.union_all(recursive_part)
    ancestors_statement = (
        select(cte_statement).where(cte_statement.c.territory_id != territory_id).order_by(cte_statement.c.level.desc())
    )

    cte_statement = (
        select(
            territories_data.c.territory_id,
            territories_data.c.name,
            territories_data.c.parent_id,
            territories_data.c.is_city,
            territories_data.c.level,
        )
        .where(territories_data.c.territory_id == territory_id)
        .cte(name="territories_recursive", recursive=True)
    )
    recursive_part = select(
        territories_data.c.territory_id,
        territories_data.c.name,
        territories_data.c.parent_id,
        territories_data.c.is_city,
        territories_data.c.level,
    ).join(cte_statement, territories_data.c.parent_id == cte_statement.c.territory_id)
    cte_statement = cte_statement.union_all(recursive_part)

    descendants_statement = (
        select(cte_statement).where(cte_statement.c.territory_id != territory_id).order_by(cte_statement.c.level.desc())
    )

    normatives_statement = (
        select(
            service_types_normatives_data.c.territory_id,
            service_types_normatives_data.c.service_type_id,
            service_types_dict.c.name.label("service_type_name"),
            service_types_normatives_data.c.urban_function_id,
            urban_functions_dict.c.name.label("urban_function_name"),
            service_types_normatives_data.c.is_regulated,
            service_types_normatives_data.c.radius_availability_meters,
            service_types_normatives_data.c.time_availability_minutes,
            service_types_normatives_data.c.services_per_1000_normative,
            service_types_normatives_data.c.services_capacity_per_1000_normative,
            service_types_normatives_data.c.year,
            service_types_normatives_data.c.source,
            service_types_normatives_data.c.created_at,
            service_types_normatives_data.c.updated_at,
        )
        .select_from(
            service_types_normatives_data.outerjoin(
                service_types_dict,
                service_types_dict.c.service_type_id == service_types_normatives_data.c.service_type_id,
            ).outerjoin(
                urban_functions_dict,
                urban_functions_dict.c.urban_function_id == service_types_normatives_data.c.urban_function_id,
            )
        )
        .where(
            service_types_normatives_data.c.territory_id.in_([1, 1]),
            service_types_normatives_data.c.year == year,
        )
    )

    # Act
    with patch(
        "idu_api.urban_api.logic.impl.helpers.territories_normatives._get_territory_ancestors"
    ) as mock_get_territory_ancestors:
        mock_get_territory_ancestors.return_value = {"1": []}
        await get_normatives_by_territory_id_from_db(
            mock_conn, territory_id, year, last_only=False, include_child_territories=False, cities_only=False
        )
        result = await get_normatives_by_territory_id_from_db(
            mock_conn, territory_id, year, last_only=False, include_child_territories=True, cities_only=True
        )
        result = process_normatives(result)

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(isinstance(item, NormativeDTO) for item in result), "Each item should be a NormativeDTO."
    assert isinstance(Normative.from_dto(result[0]), Normative), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(ancestors_statement))
    mock_conn.execute_mock.assert_any_call(str(descendants_statement))
    mock_conn.execute_mock.assert_any_call(str(normatives_statement))


@pytest.mark.asyncio
async def test_get_normatives_by_ids_from_db(mock_conn: MockConnection):
    """Test the get_normatives_by_ids_from_db function."""

    # Arrange
    ids = [1]
    not_found_ids = [1, 2]
    too_many_ids = list(range(OBJECTS_NUMBER_LIMIT + 1))
    statement = (
        select(
            service_types_normatives_data.c.service_type_id,
            service_types_dict.c.name.label("service_type_name"),
            service_types_normatives_data.c.urban_function_id,
            urban_functions_dict.c.name.label("urban_function_name"),
            service_types_normatives_data.c.is_regulated,
            service_types_normatives_data.c.radius_availability_meters,
            service_types_normatives_data.c.time_availability_minutes,
            service_types_normatives_data.c.services_per_1000_normative,
            service_types_normatives_data.c.services_capacity_per_1000_normative,
            literal("self").label("normative_type"),
            service_types_normatives_data.c.year,
            service_types_normatives_data.c.source,
            service_types_normatives_data.c.created_at,
            service_types_normatives_data.c.updated_at,
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
        )
        .select_from(
            service_types_normatives_data.join(
                territories_data,
                territories_data.c.territory_id == service_types_normatives_data.c.territory_id,
            )
            .outerjoin(
                service_types_dict,
                service_types_dict.c.service_type_id == service_types_normatives_data.c.service_type_id,
            )
            .outerjoin(
                urban_functions_dict,
                urban_functions_dict.c.urban_function_id == service_types_normatives_data.c.urban_function_id,
            )
        )
        .where(service_types_normatives_data.c.normative_id.in_(ids))
    )

    # Act
    with pytest.raises(EntitiesNotFoundByIds):
        await get_normatives_by_ids_from_db(mock_conn, not_found_ids)
    with pytest.raises(TooManyObjectsError):
        await get_normatives_by_ids_from_db(mock_conn, too_many_ids)
    result = await get_normatives_by_ids_from_db(mock_conn, ids)
    result = process_normatives(result)

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(isinstance(item, NormativeDTO) for item in result), "Each item should be a NormativeDTO."
    assert isinstance(Normative.from_dto(result[0]), Normative), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))


@pytest.mark.asyncio
async def test_add_normatives_to_territory_to_db(mock_conn: MockConnection, normative_post_req: NormativePost):
    """Test the add_normatives_to_territory_to_db function."""

    # Arrange
    async def check_normative(conn, table, conditions=None, not_conditions=None):
        if table == service_types_normatives_data:
            return False
        return True

    async def check_territory(conn, table, conditions=None, not_conditions=None):
        if table == territories_data:
            return False
        return True

    territory_id = 1
    insert_statement = (
        insert(service_types_normatives_data)
        .values([{"territory_id": territory_id, **normative_post_req.model_dump()}])
        .returning(service_types_normatives_data.c.normative_id)
    )

    # Act
    with pytest.raises(EntityAlreadyExists):
        await add_normatives_to_territory_to_db(mock_conn, territory_id, [normative_post_req])
    with patch(
        "idu_api.urban_api.logic.impl.helpers.territories_normatives.check_existence",
        new=AsyncMock(side_effect=check_territory),
    ):
        with pytest.raises(EntityNotFoundById):
            await add_normatives_to_territory_to_db(mock_conn, territory_id, [normative_post_req])
    with patch(
        "idu_api.urban_api.logic.impl.helpers.territories_normatives.check_existence",
        new=AsyncMock(side_effect=check_normative),
    ):
        result = await add_normatives_to_territory_to_db(mock_conn, territory_id, [normative_post_req])
    result = process_normatives(result)

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(isinstance(item, NormativeDTO) for item in result), "Each item should be a NormativeDTO."
    assert isinstance(Normative.from_dto(result[0]), Normative), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(insert_statement))
    mock_conn.commit_mock.assert_called_once()


@pytest.mark.asyncio
async def test_put_normatives_by_territory_id_in_db(mock_conn: MockConnection, normative_post_req: NormativePost):
    """Test the put_normatives_by_territory_id_in_db function."""

    # Arrange
    async def check_normative(conn, table, conditions=None, not_conditions=None):
        if table == service_types_normatives_data:
            return False
        return True

    async def check_territory(conn, table, conditions=None, not_conditions=None):
        if table == territories_data:
            return False
        return True

    territory_id = 1
    update_statement = (
        update(service_types_normatives_data)
        .where(
            and_(
                service_types_normatives_data.c.service_type_id == 1,
                service_types_normatives_data.c.territory_id == territory_id,
                service_types_normatives_data.c.year == date.today().year,
            )
        )
        .values(**normative_post_req.model_dump(), updated_at=datetime.now(timezone.utc))
        .returning(service_types_normatives_data.c.normative_id)
    )
    insert_statement = (
        insert(service_types_normatives_data)
        .values(**normative_post_req.model_dump(), territory_id=territory_id)
        .returning(service_types_normatives_data.c.normative_id)
    )

    # Act
    with patch(
        "idu_api.urban_api.logic.impl.helpers.territories_normatives.check_existence",
        new=AsyncMock(side_effect=check_territory),
    ):
        with pytest.raises(EntityNotFoundById):
            await put_normatives_by_territory_id_in_db(mock_conn, territory_id, [normative_post_req])
    with patch(
        "idu_api.urban_api.logic.impl.helpers.territories_normatives.check_existence",
        new=AsyncMock(side_effect=check_normative),
    ):
        await put_normatives_by_territory_id_in_db(mock_conn, territory_id, [normative_post_req])
    result = await put_normatives_by_territory_id_in_db(mock_conn, territory_id, [normative_post_req])
    result = process_normatives(result)

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(isinstance(item, NormativeDTO) for item in result), "Each item should be a NormativeDTO."
    assert isinstance(Normative.from_dto(result[0]), Normative), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(insert_statement))
    mock_conn.execute_mock.assert_any_call(str(update_statement))
    assert mock_conn.commit_mock.call_count == 2, "Commit mock count should be one for one method."


@pytest.mark.asyncio
async def test_patch_normatives_by_territory_id_in_db(mock_conn: MockConnection, normative_patch_req: NormativePatch):
    """Test the patch_normatives_by_territory_id_in_db function."""

    # Arrange
    async def check_normative(conn, table, conditions=None, not_conditions=None):
        if table == service_types_normatives_data:
            return False
        return True

    async def check_territory(conn, table, conditions=None, not_conditions=None):
        if table == territories_data:
            return False
        return True

    territory_id = 1
    update_statement = (
        update(service_types_normatives_data)
        .where(
            and_(
                service_types_normatives_data.c.service_type_id == 1,
                service_types_normatives_data.c.territory_id == territory_id,
                service_types_normatives_data.c.year == date.today().year,
            )
        )
        .values(**normative_patch_req.model_dump(exclude_unset=True), updated_at=datetime.now(timezone.utc))
        .returning(service_types_normatives_data.c.normative_id)
    )

    # Act
    with pytest.raises(EntityAlreadyExists):
        await patch_normatives_by_territory_id_in_db(mock_conn, territory_id, [normative_patch_req])
    with patch(
        "idu_api.urban_api.logic.impl.helpers.territories_normatives.check_existence",
        new=AsyncMock(side_effect=check_territory),
    ):
        with pytest.raises(EntityNotFoundById):
            await patch_normatives_by_territory_id_in_db(mock_conn, territory_id, [normative_patch_req])
    with patch(
        "idu_api.urban_api.logic.impl.helpers.territories_normatives.check_existence",
        new=AsyncMock(side_effect=check_normative),
    ):
        result = await patch_normatives_by_territory_id_in_db(mock_conn, territory_id, [normative_patch_req])
    result = process_normatives(result)

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(isinstance(item, NormativeDTO) for item in result), "Each item should be a NormativeDTO."
    mock_conn.execute_mock.assert_any_call(str(update_statement))
    mock_conn.commit_mock.assert_called_once()


@pytest.mark.asyncio
async def test_delete_normatives_by_territory_id_in_db(
    mock_conn: MockConnection, normative_delete_req: NormativeDelete
):
    """Test the delete_normatives_by_territory_id_in_db function."""

    # Arrange
    territory_id = 1
    delete_statement = delete(service_types_normatives_data).where(
        service_types_normatives_data.c.normative_id.in_([1])
    )

    # Act
    with patch("idu_api.urban_api.logic.impl.helpers.territories_normatives.check_existence") as mock_check_existence:
        mock_check_existence.return_value = False
        with pytest.raises(EntityNotFoundById):
            await delete_normatives_by_territory_id_in_db(mock_conn, territory_id, [normative_delete_req])
    result = await delete_normatives_by_territory_id_in_db(mock_conn, territory_id, [normative_delete_req])

    # Assert
    assert result == {"status": "ok"}, "Result should be {'status': 'ok'}."
    mock_conn.execute_mock.assert_any_call(str(delete_statement))
    mock_conn.commit_mock.assert_called_once()


@pytest.mark.asyncio
async def test_get_normatives_values_by_parent_id_from_db(mock_conn: MockConnection):
    """Test the get_normatives_values_by_parent_id_from_db function."""

    # Arrange
    parent_id = 1
    year = date.today().year
    child_territories_statement = select(
        territories_data.c.territory_id,
        territories_data.c.name,
        territories_data.c.parent_id,
        cast(ST_AsGeoJSON(territories_data.c.geometry, DECIMAL_PLACES), JSONB).label("geometry"),
        cast(ST_AsGeoJSON(territories_data.c.centre_point, DECIMAL_PLACES), JSONB).label("centre_point"),
    ).where(territories_data.c.parent_id == parent_id)
    cte_statement = (
        select(
            territories_data.c.territory_id,
            territories_data.c.name,
            territories_data.c.parent_id,
            territories_data.c.is_city,
            territories_data.c.level,
        )
        .where(territories_data.c.territory_id == 1)
        .cte(name="territories_recursive", recursive=True)
    )
    recursive_part = select(
        territories_data.c.territory_id,
        territories_data.c.name,
        territories_data.c.parent_id,
        territories_data.c.is_city,
        territories_data.c.level,
    ).join(cte_statement, territories_data.c.territory_id == cte_statement.c.parent_id)
    cte_statement = cte_statement.union_all(recursive_part)
    ancestors_statement = (
        select(cte_statement).where(cte_statement.c.territory_id != 1).order_by(cte_statement.c.level.desc())
    )
    normatives_statement = (
        select(
            service_types_normatives_data.c.territory_id,
            service_types_normatives_data.c.service_type_id,
            service_types_dict.c.name.label("service_type_name"),
            service_types_normatives_data.c.urban_function_id,
            urban_functions_dict.c.name.label("urban_function_name"),
            service_types_normatives_data.c.is_regulated,
            service_types_normatives_data.c.radius_availability_meters,
            service_types_normatives_data.c.time_availability_minutes,
            service_types_normatives_data.c.services_per_1000_normative,
            service_types_normatives_data.c.services_capacity_per_1000_normative,
            service_types_normatives_data.c.year,
            service_types_normatives_data.c.source,
            service_types_normatives_data.c.created_at,
            service_types_normatives_data.c.updated_at,
        )
        .select_from(
            service_types_normatives_data.outerjoin(
                service_types_dict,
                service_types_dict.c.service_type_id == service_types_normatives_data.c.service_type_id,
            ).outerjoin(
                urban_functions_dict,
                urban_functions_dict.c.urban_function_id == service_types_normatives_data.c.urban_function_id,
            )
        )
        .where(
            service_types_normatives_data.c.territory_id.in_([1, 1]),
            service_types_normatives_data.c.year == year,
        )
    )

    # Act
    with patch("idu_api.urban_api.logic.impl.helpers.territories_normatives.check_existence") as mock_check_existence:
        mock_check_existence.return_value = False
        with pytest.raises(EntityNotFoundById):
            await get_normatives_values_by_parent_id_from_db(mock_conn, parent_id, year, last_only=False)
    await get_normatives_values_by_parent_id_from_db(mock_conn, parent_id, year, last_only=False)
    result = await get_normatives_values_by_parent_id_from_db(mock_conn, parent_id, year, last_only=False)
    for item in result:
        item.normatives = process_normatives(item.normatives)
    geojson_result = await GeoJSONResponse.from_list([r.to_geojson_dict() for r in result])

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(
        isinstance(item, TerritoryWithNormativesDTO) for item in result
    ), "Each item should be a TerritoryWithNormativesDTO."
    assert all(
        isinstance(TerritoryWithNormatives(**feature.properties), TerritoryWithNormatives)
        for feature in geojson_result.features
    ), "Couldn't create pydantic model from geojson properties."
    mock_conn.execute_mock.assert_any_call(str(child_territories_statement))
    mock_conn.execute_mock.assert_any_call(str(ancestors_statement))
    mock_conn.execute_mock.assert_any_call(str(normatives_statement))


def process_normatives(normatives: list[NormativeDTO]) -> list[NormativeDTO]:
    """Process normative to pass pydantic validation."""

    processed_result = []
    for item in normatives:
        normative = {
            k: v
            for k, v in asdict(item).items()
            if k
            not in {
                "urban_function_id",
                "urban_function_name",
                "services_per_1000_normative",
                "radius_availability_meters",
            }
        }
        normative.update(
            {
                "urban_function_id": None,
                "urban_function_name": None,
                "services_per_1000_normative": None,
                "radius_availability_meters": None,
            }
        )
        processed_result.append(NormativeDTO(**normative))

        return processed_result
