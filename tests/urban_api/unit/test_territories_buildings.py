"""Unit tests for territory-related living buildings are defined here."""

from unittest.mock import AsyncMock, patch

import pytest
from fastapi_pagination.bases import CursorRawParams, RawParams
from geoalchemy2.functions import ST_AsGeoJSON
from sqlalchemy import cast, select
from sqlalchemy.dialects.postgresql import JSONB

from idu_api.common.db.entities import (
    living_buildings_data,
    object_geometries_data,
    physical_object_types_dict,
    physical_objects_data,
    urban_objects_data,
)
from idu_api.urban_api.dto import LivingBuildingWithGeometryDTO, PageDTO
from idu_api.urban_api.exceptions.logic.common import EntityNotFoundById
from idu_api.urban_api.logic.impl.helpers.territories_buildings import (
    get_living_buildings_with_geometry_by_territory_id_from_db,
)
from idu_api.urban_api.logic.impl.helpers.utils import DECIMAL_PLACES, include_child_territories_cte
from idu_api.urban_api.schemas import LivingBuildingWithGeometry
from tests.urban_api.helpers.connection import MockConnection

####################################################################################
#                           Default use-case tests                                 #
####################################################################################


@pytest.mark.asyncio
async def test_get_living_buildings_with_geometry_by_territory_id_from_db(mock_conn: MockConnection):
    """Test the get_living_buildings_with_geometry_by_territory_id_from_db function."""

    # Arrange
    territory_id = 1
    include_child_territories, cities_only = False, False
    limit, offset = 10, 0
    statement = (
        select(
            living_buildings_data.c.living_building_id,
            living_buildings_data.c.living_area,
            living_buildings_data.c.properties,
            physical_objects_data.c.physical_object_id,
            physical_objects_data.c.name.label("physical_object_name"),
            physical_objects_data.c.properties.label("physical_object_properties"),
            physical_object_types_dict.c.physical_object_type_id,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            object_geometries_data.c.object_geometry_id,
            object_geometries_data.c.address,
            object_geometries_data.c.osm_id,
            cast(ST_AsGeoJSON(object_geometries_data.c.geometry, DECIMAL_PLACES), JSONB).label("geometry"),
            cast(ST_AsGeoJSON(object_geometries_data.c.centre_point, DECIMAL_PLACES), JSONB).label("centre_point"),
        )
        .select_from(
            living_buildings_data.join(
                physical_objects_data,
                physical_objects_data.c.physical_object_id == living_buildings_data.c.physical_object_id,
            )
            .join(
                physical_object_types_dict,
                physical_objects_data.c.physical_object_type_id == physical_object_types_dict.c.physical_object_type_id,
            )
            .join(
                urban_objects_data,
                physical_objects_data.c.physical_object_id == urban_objects_data.c.physical_object_id,
            )
            .join(
                object_geometries_data,
                urban_objects_data.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
            )
        )
        .distinct()
        .order_by(living_buildings_data.c.living_building_id)
    )
    territories_cte = include_child_territories_cte(territory_id, True)
    recursive_statement = statement.where(
        object_geometries_data.c.territory_id.in_(select(territories_cte.c.territory_id))
    )
    statement = statement.where(object_geometries_data.c.territory_id == territory_id)

    # Act
    with patch("idu_api.urban_api.logic.impl.helpers.territories_buildings.check_existence") as mock_check_existence:
        mock_check_existence.return_value = False
        with pytest.raises(EntityNotFoundById):
            await get_living_buildings_with_geometry_by_territory_id_from_db(
                mock_conn, territory_id, include_child_territories, cities_only
            )
    with patch("idu_api.urban_api.utils.pagination.verify_params") as mock_verify_params:
        mock_verify_params.return_value = (None, RawParams(limit=limit, offset=offset))
        statement = statement.offset(offset).limit(limit)
        recursive_statement = recursive_statement.limit(limit).offset(offset)
        await get_living_buildings_with_geometry_by_territory_id_from_db(mock_conn, territory_id, True, True)
        page_result = await get_living_buildings_with_geometry_by_territory_id_from_db(
            mock_conn, territory_id, include_child_territories, cities_only
        )
    with patch("idu_api.urban_api.utils.pagination.verify_params") as mock_verify_params:
        mock_verify_params.return_value = (None, CursorRawParams(cursor=None, size=limit))
        with patch("idu_api.urban_api.utils.pagination.greenlet_spawn", new=AsyncMock()) as mock_greenlet_spawn:
            mock_greenlet_spawn.return_value = await mock_conn.execute(
                statement.limit(limit),
                paging_data={
                    "previous": None,
                    "next_": None,
                    "has_previous": False,
                    "has_next": False,
                },
            )
            cursor_result = await get_living_buildings_with_geometry_by_territory_id_from_db(
                mock_conn, territory_id, include_child_territories, cities_only
            )

    # Assert
    assert isinstance(page_result, PageDTO), "Result should be a PageDTO."
    assert all(
        isinstance(item, LivingBuildingWithGeometryDTO) for item in page_result.items
    ), "Each item should be a LivingBuildingWithGeometryDTO."
    assert isinstance(
        LivingBuildingWithGeometry.from_dto(page_result.items[0]), LivingBuildingWithGeometry
    ), "Couldn't create pydantic model from DTO."
    assert isinstance(cursor_result, PageDTO), "Result should be a PageDTO."
    assert all(
        isinstance(item, LivingBuildingWithGeometryDTO) for item in cursor_result.items
    ), "Each item should be a LivingBuildingWithGeometryDTO."
    assert isinstance(
        LivingBuildingWithGeometry.from_dto(cursor_result.items[0]), LivingBuildingWithGeometry
    ), "Couldn't create pydantic model from DTO."
    assert hasattr(
        cursor_result, "cursor_data"
    ), "Expected cursor_result to have an additional data for cursor pagination."
    mock_conn.execute_mock.assert_any_call(str(statement))
    mock_conn.execute_mock.assert_any_call(str(recursive_statement))
