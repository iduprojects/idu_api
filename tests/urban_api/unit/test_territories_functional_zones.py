"""Unit tests for territory-related functional zones are defined here."""

from datetime import datetime
from unittest.mock import patch

import pytest
from geoalchemy2.functions import ST_AsEWKB
from sqlalchemy import delete, select

from idu_api.common.db.entities import functional_zone_types_dict, functional_zones_data, territories_data
from idu_api.urban_api.dto import FunctionalZoneDTO, FunctionalZoneSourceDTO
from idu_api.urban_api.exceptions.logic.common import EntityNotFoundById
from idu_api.urban_api.logic.impl.helpers.territories_functional_zones import (
    delete_all_functional_zones_for_territory_from_db,
    get_functional_zones_by_territory_id_from_db,
    get_functional_zones_sources_by_territory_id_from_db,
)
from idu_api.urban_api.logic.impl.helpers.utils import include_child_territories_cte
from idu_api.urban_api.schemas import FunctionalZone, FunctionalZoneSource, FunctionalZoneWithoutGeometry
from idu_api.urban_api.schemas.geometries import GeoJSONResponse
from tests.urban_api.helpers.connection import MockConnection

####################################################################################
#                           Default use-case tests                                 #
####################################################################################


@pytest.mark.asyncio
async def test_get_functional_zones_sources_by_territory_id_from_db(mock_conn: MockConnection):
    """Test the get_functional_zones_sources_by_territory_id_from_db function."""

    # Arrange
    territory_id = 1
    include_child_territories, cities_only = False, False
    statement = select(functional_zones_data.c.year, functional_zones_data.c.source).distinct()
    territories_cte = include_child_territories_cte(territory_id, True)
    recursive_statement = statement.where(
        functional_zones_data.c.territory_id.in_(select(territories_cte.c.territory_id))
    )
    statement = statement.where(functional_zones_data.c.territory_id == territory_id)

    # Act
    with patch(
        "idu_api.urban_api.logic.impl.helpers.territories_functional_zones.check_existence"
    ) as mock_check_existence:
        mock_check_existence.return_value = False
        with pytest.raises(EntityNotFoundById):
            await get_functional_zones_sources_by_territory_id_from_db(
                mock_conn, territory_id, include_child_territories, cities_only
            )
    await get_functional_zones_sources_by_territory_id_from_db(mock_conn, territory_id, True, True)
    result = await get_functional_zones_sources_by_territory_id_from_db(
        mock_conn, territory_id, include_child_territories, cities_only
    )

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(
        isinstance(item, FunctionalZoneSourceDTO) for item in result
    ), "Each item should be a FunctionalZoneSourceDTO."
    assert isinstance(
        FunctionalZoneSource.from_dto(result[0]), FunctionalZoneSource
    ), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))
    mock_conn.execute_mock.assert_any_call(str(recursive_statement))


@pytest.mark.asyncio
async def test_get_functional_zones_by_territory_id_from_db(mock_conn: MockConnection):
    """Test the get_functional_zones_by_territory_id_from_db function."""

    # Arrange
    territory_id = 1
    year = datetime.today().year
    source = "mock_string"
    functional_zone_type_id = 1
    include_child_territories, cities_only = False, False
    statement = (
        select(
            functional_zones_data.c.functional_zone_id,
            functional_zones_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            functional_zones_data.c.functional_zone_type_id,
            functional_zone_types_dict.c.name.label("functional_zone_type_name"),
            functional_zone_types_dict.c.zone_nickname.label("functional_zone_type_nickname"),
            functional_zone_types_dict.c.description.label("functional_zone_type_description"),
            functional_zones_data.c.name,
            ST_AsEWKB(functional_zones_data.c.geometry).label("geometry"),
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
        .where(functional_zones_data.c.year == year, functional_zones_data.c.source == source)
    )
    territories_cte = include_child_territories_cte(territory_id, True)
    recursive_statement = statement.where(
        functional_zones_data.c.territory_id.in_(select(territories_cte.c.territory_id))
    )
    statement = statement.where(functional_zones_data.c.territory_id == territory_id)
    recursive_statement = recursive_statement.where(
        functional_zones_data.c.functional_zone_type_id == functional_zone_type_id
    )
    statement = statement.where(functional_zones_data.c.functional_zone_type_id == functional_zone_type_id)

    # Act
    with patch(
        "idu_api.urban_api.logic.impl.helpers.territories_functional_zones.check_existence"
    ) as mock_check_existence:
        mock_check_existence.return_value = False
        with pytest.raises(EntityNotFoundById):
            await get_functional_zones_by_territory_id_from_db(
                mock_conn, territory_id, year, source, functional_zone_type_id, include_child_territories, cities_only
            )
    await get_functional_zones_by_territory_id_from_db(
        mock_conn, territory_id, year, source, functional_zone_type_id, True, True
    )
    result = await get_functional_zones_by_territory_id_from_db(
        mock_conn, territory_id, year, source, functional_zone_type_id, include_child_territories, cities_only
    )
    geojson_result = await GeoJSONResponse.from_list([r.to_geojson_dict() for r in result])

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(isinstance(item, FunctionalZoneDTO) for item in result), "Each item should be a FunctionalZoneDTO."
    assert isinstance(FunctionalZone.from_dto(result[0]), FunctionalZone), "Couldn't create pydantic model from DTO."
    assert isinstance(
        FunctionalZoneWithoutGeometry(**geojson_result.features[0].properties), FunctionalZoneWithoutGeometry
    ), "Couldn't create pydantic model from geojson properties."
    mock_conn.execute_mock.assert_any_call(str(statement))
    mock_conn.execute_mock.assert_any_call(str(recursive_statement))


@pytest.mark.asyncio
async def test_delete_all_functional_zones_for_territory_from_db(mock_conn: MockConnection):
    """Test the delete_all_functional_zones_for_territory_from_db function."""

    # Arrange
    territory_id = 1
    include_child_territories, cities_only = False, False
    statement = delete(functional_zones_data)
    territories_cte = include_child_territories_cte(territory_id, True)
    recursive_statement = statement.where(
        functional_zones_data.c.territory_id.in_(select(territories_cte.c.territory_id))
    )
    statement = statement.where(functional_zones_data.c.territory_id == territory_id)

    # Act
    with patch(
        "idu_api.urban_api.logic.impl.helpers.territories_functional_zones.check_existence"
    ) as mock_check_existence:
        mock_check_existence.return_value = False
        with pytest.raises(EntityNotFoundById):
            await delete_all_functional_zones_for_territory_from_db(
                mock_conn, territory_id, include_child_territories, cities_only
            )
    await delete_all_functional_zones_for_territory_from_db(mock_conn, territory_id, True, True)
    result = await delete_all_functional_zones_for_territory_from_db(
        mock_conn, territory_id, include_child_territories, cities_only
    )

    # Assert
    assert result == {"status": "ok"}, "Result should be {'status': 'ok'}."
    mock_conn.execute_mock.assert_any_call(str(statement))
    mock_conn.execute_mock.assert_any_call(str(recursive_statement))
    assert mock_conn.commit_mock.call_count == 2, "Commit mock count should be one for one method."
