from unittest.mock import patch

import pytest
from geoalchemy2.functions import ST_AsEWKB
from sqlalchemy import select

from idu_api.common.db.entities import (
    buffer_types_dict,
    buffers_data,
    object_geometries_data,
    physical_object_types_dict,
    physical_objects_data,
    service_types_dict,
    services_data,
    territories_data,
    urban_objects_data,
)
from idu_api.urban_api.dto import BufferDTO
from idu_api.urban_api.exceptions.logic.common import EntityNotFoundById
from idu_api.urban_api.logic.impl.helpers.territories_buffers import get_buffers_by_territory_id_from_db
from idu_api.urban_api.logic.impl.helpers.utils import include_child_territories_cte
from idu_api.urban_api.schemas import Buffer, BufferAttributes
from idu_api.urban_api.schemas.geometries import GeoJSONResponse
from tests.urban_api.helpers import MockConnection


@pytest.mark.asyncio
async def test_get_buffers_by_territory_id_from_db(mock_conn: MockConnection):
    """Test the get_buffers_by_territory_id_from_db function."""

    # Arrange
    territory_id = 1
    include_child_territories, cities_only = False, False
    buffer_type_id = 1
    physical_object_type_id, service_type_id = 1, 1

    statement = (
        select(
            buffer_types_dict.c.buffer_type_id,
            buffer_types_dict.c.name.label("buffer_type_name"),
            urban_objects_data.c.urban_object_id,
            physical_objects_data.c.physical_object_id,
            physical_objects_data.c.name.label("physical_object_name"),
            physical_object_types_dict.c.physical_object_type_id,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            object_geometries_data.c.object_geometry_id,
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            services_data.c.service_id,
            services_data.c.name.label("service_name"),
            service_types_dict.c.service_type_id,
            service_types_dict.c.name.label("service_type_name"),
            ST_AsEWKB(buffers_data.c.geometry).label("geometry"),
            buffers_data.c.is_custom,
        )
        .select_from(
            buffers_data.join(buffer_types_dict, buffer_types_dict.c.buffer_type_id == buffers_data.c.buffer_type_id)
            .join(urban_objects_data, urban_objects_data.c.urban_object_id == buffers_data.c.urban_object_id)
            .join(
                physical_objects_data,
                physical_objects_data.c.physical_object_id == urban_objects_data.c.physical_object_id,
            )
            .join(
                physical_object_types_dict,
                physical_object_types_dict.c.physical_object_type_id == physical_objects_data.c.physical_object_type_id,
            )
            .join(
                object_geometries_data,
                object_geometries_data.c.object_geometry_id == urban_objects_data.c.object_geometry_id,
            )
            .join(territories_data, territories_data.c.territory_id == object_geometries_data.c.territory_id)
            .outerjoin(services_data, services_data.c.service_id == urban_objects_data.c.service_id)
            .outerjoin(service_types_dict, service_types_dict.c.service_type_id == services_data.c.service_type_id)
        )
        .distinct()
    )
    territories_cte = include_child_territories_cte(territory_id, True)
    recursive_statement = statement.where(territories_data.c.territory_id.in_(select(territories_cte.c.territory_id)))
    statement = statement.where(territories_data.c.territory_id == territory_id)
    statement_with_filters = statement.where(
        buffer_types_dict.c.buffer_type_id == buffer_type_id,
        physical_object_types_dict.c.physical_object_type_id == physical_object_type_id,
        service_types_dict.c.service_type_id == service_type_id,
    )

    # Act
    with patch("idu_api.urban_api.logic.impl.helpers.territories_buffers.check_existence") as mock_check_existence:
        mock_check_existence.return_value = False
        with pytest.raises(EntityNotFoundById):
            await get_buffers_by_territory_id_from_db(
                mock_conn,
                territory_id,
                include_child_territories,
                cities_only,
                buffer_type_id,
                physical_object_type_id,
                service_type_id,
            )
    await get_buffers_by_territory_id_from_db(
        mock_conn,
        territory_id,
        include_child_territories,
        cities_only,
        None,
        None,
        None,
    )
    await get_buffers_by_territory_id_from_db(
        mock_conn,
        territory_id,
        True,
        True,
        None,
        None,
        None,
    )
    list_result = await get_buffers_by_territory_id_from_db(
        mock_conn,
        territory_id,
        include_child_territories,
        cities_only,
        buffer_type_id,
        physical_object_type_id,
        service_type_id,
    )
    geojson_result = await GeoJSONResponse.from_list([r.to_geojson_dict() for r in list_result])

    # Assert
    assert isinstance(list_result, list), "Result should be a list."
    assert all(isinstance(item, BufferDTO) for item in list_result), "Each item should be a BufferDTO."
    assert isinstance(
        BufferAttributes(**geojson_result.features[0].properties), BufferAttributes
    ), "Couldn't create pydantic model from geojson properties."
    mock_conn.execute_mock.assert_any_call(str(statement))
    mock_conn.execute_mock.assert_any_call(str(recursive_statement))
    mock_conn.execute_mock.assert_any_call(str(statement_with_filters))
