"""Unit tests for territory-related indicators objects are defined here."""

from datetime import date
from unittest.mock import patch

import pytest
from geoalchemy2.functions import ST_AsGeoJSON
from sqlalchemy import cast, func, select, text
from sqlalchemy.dialects.postgresql import JSONB

from idu_api.common.db.entities import (
    indicators_dict,
    indicators_groups_data,
    measurement_units_dict,
    territories_data,
    territory_indicators_data,
)
from idu_api.urban_api.dto import IndicatorDTO, IndicatorValueDTO, TerritoryWithIndicatorsDTO
from idu_api.urban_api.exceptions.logic.common import EntityNotFoundById
from idu_api.urban_api.logic.impl.helpers.territories_indicators import (
    get_indicator_values_by_parent_id_from_db,
    get_indicator_values_by_territory_id_from_db,
    get_indicators_by_territory_id_from_db,
)
from idu_api.urban_api.logic.impl.helpers.utils import DECIMAL_PLACES, include_child_territories_cte
from idu_api.urban_api.schemas import Indicator, IndicatorValue, TerritoryWithIndicators
from idu_api.urban_api.schemas.geometries import GeoJSONResponse
from tests.urban_api.helpers.connection import MockConnection

####################################################################################
#                           Default use-case tests                                 #
####################################################################################


@pytest.mark.asyncio
async def test_get_indicators_by_territory_id_from_db(mock_conn: MockConnection):
    """Test the get_indicators_by_territory_id_from_db function."""

    # Arrange
    territory_id = 1
    statement = (
        select(indicators_dict, measurement_units_dict.c.name.label("measurement_unit_name"))
        .select_from(
            territory_indicators_data.join(
                indicators_dict, territory_indicators_data.c.indicator_id == indicators_dict.c.indicator_id
            ).outerjoin(
                measurement_units_dict,
                measurement_units_dict.c.measurement_unit_id == indicators_dict.c.measurement_unit_id,
            )
        )
        .where(territory_indicators_data.c.territory_id == territory_id)
    )

    # Act
    with patch("idu_api.urban_api.logic.impl.helpers.territories_indicators.check_existence") as mock_check_existence:
        mock_check_existence.return_value = False
        with pytest.raises(EntityNotFoundById):
            await get_indicators_by_territory_id_from_db(mock_conn, territory_id)
    result = await get_indicators_by_territory_id_from_db(mock_conn, territory_id)

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(isinstance(item, IndicatorDTO) for item in result), "Each item should be a IndicatorDTO."
    assert isinstance(Indicator.from_dto(result[0]), Indicator), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))


@pytest.mark.asyncio
async def test_get_indicator_values_by_territory_id_from_db(mock_conn: MockConnection):
    """Test the get_indicator_values_by_territory_id_from_db function."""

    # Arrange
    territory_id = 1
    filters = {
        "indicators_group_id": 1,
        "indicator_ids": "1",
        "start_date": date.today(),
        "end_date": date.today(),
        "value_type": "real",
        "information_source": "mock_string",
    }
    subquery = (
        select(
            territory_indicators_data.c.indicator_id,
            territory_indicators_data.c.value_type,
            func.max(func.date(territory_indicators_data.c.date_value)).label("max_date"),
        )
        .select_from(
            territory_indicators_data.join(
                territories_data, territories_data.c.territory_id == territory_indicators_data.c.territory_id
            )
        )
        .where(territory_indicators_data.c.territory_id == territory_id)
        .group_by(
            territory_indicators_data.c.indicator_id,
            territory_indicators_data.c.value_type,
        )
        .subquery()
    )
    base_statement = select(
        territory_indicators_data,
        indicators_dict.c.parent_id,
        indicators_dict.c.name_full,
        indicators_dict.c.level,
        indicators_dict.c.list_label,
        measurement_units_dict.c.measurement_unit_id,
        measurement_units_dict.c.name.label("measurement_unit_name"),
        territories_data.c.name.label("territory_name"),
    )
    base_select_from = (
        territory_indicators_data.join(
            indicators_dict,
            indicators_dict.c.indicator_id == territory_indicators_data.c.indicator_id,
        )
        .outerjoin(
            measurement_units_dict,
            measurement_units_dict.c.measurement_unit_id == indicators_dict.c.measurement_unit_id,
        )
        .outerjoin(
            indicators_groups_data,
            indicators_groups_data.c.indicator_id == indicators_dict.c.indicator_id,
        )
        .join(
            territories_data,
            territories_data.c.territory_id == territory_indicators_data.c.territory_id,
        )
    )
    last_only_select_from = (
        territory_indicators_data.join(
            subquery,
            (territory_indicators_data.c.indicator_id == subquery.c.indicator_id)
            & (territory_indicators_data.c.value_type == subquery.c.value_type)
            & (territory_indicators_data.c.date_value == subquery.c.max_date),
        )
        .join(
            indicators_dict,
            indicators_dict.c.indicator_id == territory_indicators_data.c.indicator_id,
        )
        .outerjoin(
            measurement_units_dict,
            measurement_units_dict.c.measurement_unit_id == indicators_dict.c.measurement_unit_id,
        )
        .outerjoin(
            indicators_groups_data,
            indicators_groups_data.c.indicator_id == indicators_dict.c.indicator_id,
        )
        .join(
            territories_data,
            territories_data.c.territory_id == territory_indicators_data.c.territory_id,
        )
    )
    statement = base_statement.select_from(base_select_from)
    last_only_statement = base_statement.select_from(last_only_select_from)
    statement_with_filters = statement.where(
        territory_indicators_data.c.indicator_id.in_([1]),
        indicators_groups_data.c.indicators_group_id == filters["indicators_group_id"],
        func.date(territory_indicators_data.c.date_value) >= filters["start_date"],
        func.date(territory_indicators_data.c.date_value) <= filters["end_date"],
        territory_indicators_data.c.value_type == filters["value_type"],
        territory_indicators_data.c.information_source.ilike(f"%{filters['information_source']}%"),
        territory_indicators_data.c.territory_id == territory_id,
    )
    territories_cte = include_child_territories_cte(territory_id, True)
    last_only_recursive_statement = last_only_statement.where(
        territory_indicators_data.c.territory_id.in_(select(territories_cte.c.territory_id))
    )
    statement = statement.where(territory_indicators_data.c.territory_id == territory_id)
    last_only_statement = last_only_statement.where(territory_indicators_data.c.territory_id == territory_id)

    # Act
    with patch("idu_api.urban_api.logic.impl.helpers.territories_indicators.check_existence") as mock_check_existence:
        mock_check_existence.return_value = False
        with pytest.raises(EntityNotFoundById):
            await get_indicator_values_by_territory_id_from_db(
                mock_conn, territory_id, **filters, last_only=False, include_child_territories=True, cities_only=True
            )
    await get_indicator_values_by_territory_id_from_db(
        mock_conn,
        territory_id,
        None,
        None,
        None,
        None,
        None,
        None,
        last_only=True,
        include_child_territories=True,
        cities_only=True,
    )
    await get_indicator_values_by_territory_id_from_db(
        mock_conn,
        territory_id,
        None,
        None,
        None,
        None,
        None,
        None,
        last_only=True,
        include_child_territories=False,
        cities_only=False,
    )
    await get_indicator_values_by_territory_id_from_db(
        mock_conn,
        territory_id,
        None,
        None,
        None,
        None,
        None,
        None,
        last_only=False,
        include_child_territories=False,
        cities_only=False,
    )
    result = await get_indicator_values_by_territory_id_from_db(
        mock_conn, territory_id, **filters, last_only=False, include_child_territories=False, cities_only=False
    )

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(isinstance(item, IndicatorValueDTO) for item in result), "Each item should be a IndicatorValueDTO."
    assert isinstance(IndicatorValue.from_dto(result[0]), IndicatorValue), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))
    mock_conn.execute_mock.assert_any_call(str(last_only_statement))
    mock_conn.execute_mock.assert_any_call(str(statement_with_filters))
    mock_conn.execute_mock.assert_any_call(str(last_only_recursive_statement))


@pytest.mark.asyncio
async def test_get_indicator_values_by_parent_id_from_db(mock_conn: MockConnection):
    """Test the get_indicator_values_by_parent_id_from_db function."""

    # Arrange
    parent_id = 1
    filters = {
        "indicators_group_id": 1,
        "indicator_ids": "1",
        "start_date": date.today(),
        "end_date": date.today(),
        "value_type": "real",
        "information_source": "mock_string",
    }
    statement = select(
        territories_data.c.territory_id,
        territories_data.c.name,
        territories_data.c.geometry,
        territories_data.c.centre_point,
    ).where(territories_data.c.parent_id == parent_id)
    territories_cte = statement.cte(name="territories_cte")
    statement = (
        select(
            territories_cte.c.name.label("territory_name"),
            cast(ST_AsGeoJSON(territories_cte.c.geometry, DECIMAL_PLACES), JSONB).label("geometry"),
            cast(ST_AsGeoJSON(territories_cte.c.centre_point, DECIMAL_PLACES), JSONB).label("centre_point"),
            territory_indicators_data,
            indicators_dict.c.parent_id,
            indicators_dict.c.name_full,
            indicators_dict.c.level,
            indicators_dict.c.list_label,
            measurement_units_dict.c.measurement_unit_id,
            measurement_units_dict.c.name.label("measurement_unit_name"),
        )
        .select_from(
            territories_cte.join(
                territory_indicators_data, territories_cte.c.territory_id == territory_indicators_data.c.territory_id
            )
            .join(indicators_dict, indicators_dict.c.indicator_id == territory_indicators_data.c.indicator_id)
            .outerjoin(
                measurement_units_dict,
                measurement_units_dict.c.measurement_unit_id == indicators_dict.c.measurement_unit_id,
            )
            .outerjoin(indicators_groups_data, indicators_groups_data.c.indicator_id == indicators_dict.c.indicator_id)
        )
        .where(
            territory_indicators_data.c.indicator_id.in_([1]),
            indicators_groups_data.c.indicators_group_id == filters["indicators_group_id"],
            func.date(territory_indicators_data.c.date_value) >= filters["start_date"],
            func.date(territory_indicators_data.c.date_value) <= filters["end_date"],
            territory_indicators_data.c.value_type == filters["value_type"],
            territory_indicators_data.c.information_source.ilike(f"%{filters['information_source']}%"),
        )
    )
    last_only_statement = (
        statement.add_columns(
            func.row_number()
            .over(
                partition_by=[
                    territory_indicators_data.c.territory_id,
                    territory_indicators_data.c.indicator_id,
                    territory_indicators_data.c.value_type,
                ],
                order_by=territory_indicators_data.c.date_value.desc(),
            )
            .label("row_num")
        )
        .alias("last_values")
        .select()
        .where(text("row_num = 1"))
    )

    # Act
    with patch("idu_api.urban_api.logic.impl.helpers.territories_indicators.check_existence") as mock_check_existence:
        mock_check_existence.return_value = False
        with pytest.raises(EntityNotFoundById):
            await get_indicator_values_by_parent_id_from_db(mock_conn, parent_id, **filters, last_only=False)
    await get_indicator_values_by_parent_id_from_db(mock_conn, parent_id, **filters, last_only=True)
    await get_indicator_values_by_parent_id_from_db(mock_conn, parent_id, **filters, last_only=False)
    await get_indicator_values_by_parent_id_from_db(mock_conn, parent_id, **filters, last_only=True)
    result = await get_indicator_values_by_parent_id_from_db(mock_conn, parent_id, **filters, last_only=False)
    geojson_result = await GeoJSONResponse.from_list([r.to_geojson_dict() for r in result])

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(
        isinstance(item, TerritoryWithIndicatorsDTO) for item in result
    ), "Each item should be a TerritoryWithIndicatorsDTO."
    assert all(
        isinstance(indicator, IndicatorValueDTO) for item in result for indicator in item.indicators
    ), "Each item in list indicators should be a IndicatorValueDTO."
    assert isinstance(
        TerritoryWithIndicators(**geojson_result.features[0].properties), TerritoryWithIndicators
    ), "Couldn't create pydantic model from geojson properties."
    mock_conn.execute_mock.assert_any_call(str(statement))
    mock_conn.execute_mock.assert_any_call(str(last_only_statement))
