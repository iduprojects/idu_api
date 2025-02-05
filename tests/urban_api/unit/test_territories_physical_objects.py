"""Unit tests for territory-related physical_objects are defined here."""

from unittest.mock import AsyncMock, patch

import pytest
from fastapi_pagination.bases import CursorRawParams, RawParams
from geoalchemy2.functions import ST_AsGeoJSON
from sqlalchemy import cast, select
from sqlalchemy.dialects.postgresql import JSONB

from idu_api.common.db.entities import (
    living_buildings_data,
    object_geometries_data,
    physical_object_functions_dict,
    physical_object_types_dict,
    physical_objects_data,
    territories_data,
    urban_objects_data,
)
from idu_api.urban_api.dto import PageDTO, PhysicalObjectDTO, PhysicalObjectTypeDTO, PhysicalObjectWithGeometryDTO
from idu_api.urban_api.exceptions.logic.common import EntityNotFoundById
from idu_api.urban_api.logic.impl.helpers.territories_physical_objects import (
    get_physical_object_types_by_territory_id_from_db,
    get_physical_objects_by_territory_id_from_db,
    get_physical_objects_with_geometry_by_territory_id_from_db,
)
from idu_api.urban_api.logic.impl.helpers.utils import DECIMAL_PLACES, include_child_territories_cte
from idu_api.urban_api.schemas import PhysicalObject, PhysicalObjectType, PhysicalObjectWithGeometry
from idu_api.urban_api.schemas.geometries import GeoJSONResponse
from tests.urban_api.helpers.connection import MockConnection

####################################################################################
#                           Default use-case tests                                 #
####################################################################################


@pytest.mark.asyncio
async def test_get_physical_object_types_by_territory_id_from_db(mock_conn: MockConnection):
    """Test the get_physical_object_types_by_territory_id_from_db function."""

    # Arrange
    territory_id = 1
    include_child_territories, cities_only = False, False
    statement = (
        select(physical_object_types_dict, physical_object_functions_dict.c.name.label("physical_object_function_name"))
        .select_from(
            territories_data.join(
                object_geometries_data,
                object_geometries_data.c.territory_id == territories_data.c.territory_id,
            )
            .join(
                urban_objects_data,
                urban_objects_data.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
            )
            .join(
                physical_objects_data,
                physical_objects_data.c.physical_object_id == urban_objects_data.c.physical_object_id,
            )
            .join(
                physical_object_types_dict,
                physical_object_types_dict.c.physical_object_type_id == physical_objects_data.c.physical_object_type_id,
            )
            .outerjoin(
                physical_object_functions_dict,
                physical_object_functions_dict.c.physical_object_function_id
                == physical_object_types_dict.c.physical_object_function_id,
            )
        )
        .order_by(physical_object_types_dict.c.physical_object_type_id)
        .distinct()
    )
    territories_cte = include_child_territories_cte(territory_id, True)
    recursive_statement = statement.where(
        object_geometries_data.c.territory_id.in_(select(territories_cte.c.territory_id))
    )
    statement = statement.where(object_geometries_data.c.territory_id == territory_id)

    # Act
    with patch(
        "idu_api.urban_api.logic.impl.helpers.territories_physical_objects.check_existence"
    ) as mock_check_existence:
        mock_check_existence.return_value = False
        with pytest.raises(EntityNotFoundById):
            await get_physical_object_types_by_territory_id_from_db(
                mock_conn, territory_id, include_child_territories, cities_only
            )
    await get_physical_object_types_by_territory_id_from_db(mock_conn, territory_id, True, True)
    result = await get_physical_object_types_by_territory_id_from_db(
        mock_conn, territory_id, include_child_territories, cities_only
    )

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(
        isinstance(item, PhysicalObjectTypeDTO) for item in result
    ), "Each item should be a PhysicalObjectTypeDTO."
    assert isinstance(
        PhysicalObjectType.from_dto(result[0]), PhysicalObjectType
    ), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))
    mock_conn.execute_mock.assert_any_call(str(recursive_statement))


@pytest.mark.asyncio
async def test_get_physical_objects_by_territory_id_from_db(mock_conn: MockConnection):
    """Test the get_physical_objects_by_territory_id_from_db function."""

    # Arrange
    territory_id = 1
    include_child_territories, cities_only = False, False
    physical_object_type_id = 1
    name = "mock_string"
    limit, offset = 10, 0
    statement = (
        select(
            physical_objects_data,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            physical_object_types_dict.c.physical_object_function_id,
            physical_object_functions_dict.c.name.label("physical_object_function_name"),
            living_buildings_data.c.living_building_id,
            living_buildings_data.c.living_area,
            living_buildings_data.c.properties.label("living_building_properties"),
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
        )
        .select_from(
            urban_objects_data.join(
                physical_objects_data,
                physical_objects_data.c.physical_object_id == urban_objects_data.c.physical_object_id,
            )
            .join(
                object_geometries_data,
                urban_objects_data.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
            )
            .join(
                territories_data,
                territories_data.c.territory_id == object_geometries_data.c.territory_id,
            )
            .join(
                physical_object_types_dict,
                physical_objects_data.c.physical_object_type_id == physical_object_types_dict.c.physical_object_type_id,
            )
            .join(
                physical_object_functions_dict,
                physical_object_functions_dict.c.physical_object_function_id
                == physical_object_types_dict.c.physical_object_function_id,
            )
            .outerjoin(
                living_buildings_data,
                living_buildings_data.c.physical_object_id == physical_objects_data.c.physical_object_id,
            )
        )
        .distinct()
        .order_by(physical_objects_data.c.physical_object_id)
    )
    territories_cte = include_child_territories_cte(territory_id, True)
    recursive_statement = statement.where(
        object_geometries_data.c.territory_id.in_(select(territories_cte.c.territory_id))
    )
    statement = statement.where(object_geometries_data.c.territory_id == territory_id)
    statement_with_filters = statement.where(
        physical_objects_data.c.physical_object_type_id == physical_object_type_id,
        physical_objects_data.c.name.ilike(f"%{name}%"),
    )

    # Act
    with patch(
        "idu_api.urban_api.logic.impl.helpers.territories_physical_objects.check_existence"
    ) as mock_check_existence:
        mock_check_existence.return_value = False
        with pytest.raises(EntityNotFoundById):
            await get_physical_objects_by_territory_id_from_db(
                mock_conn,
                territory_id,
                None,
                None,
                None,
                include_child_territories,
                cities_only,
                None,
                "asc",
                paginate=False,
            )

    with patch("idu_api.urban_api.utils.pagination.verify_params") as mock_verify_params:
        mock_verify_params.return_value = (None, RawParams(limit=limit, offset=offset))
        statement = statement.offset(offset).limit(limit)
        page_result = await get_physical_objects_by_territory_id_from_db(
            mock_conn,
            territory_id,
            None,
            None,
            None,
            include_child_territories,
            cities_only,
            None,
            "asc",
            paginate=True,
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
            cursor_result = await get_physical_objects_by_territory_id_from_db(
                mock_conn,
                territory_id,
                None,
                None,
                None,
                include_child_territories,
                cities_only,
                None,
                "asc",
                paginate=True,
            )

    await get_physical_objects_by_territory_id_from_db(
        mock_conn, territory_id, None, None, None, True, True, None, "asc", paginate=False
    )
    list_result = await get_physical_objects_by_territory_id_from_db(
        mock_conn,
        territory_id,
        physical_object_type_id,
        None,
        name,
        include_child_territories,
        cities_only,
        None,
        "asc",
        paginate=False,
    )

    # Assert
    assert isinstance(page_result, PageDTO), "Result should be a PageDTO."
    assert all(
        isinstance(item, PhysicalObjectDTO) for item in page_result.items
    ), "Each item should be a PhysicalObjectDTO."
    assert isinstance(
        PhysicalObject.from_dto(page_result.items[0]), PhysicalObject
    ), "Couldn't create pydantic model from DTO."
    assert isinstance(cursor_result, PageDTO), "Result should be a PageDTO."
    assert all(
        isinstance(item, PhysicalObjectDTO) for item in cursor_result.items
    ), "Each item should be a PhysicalObjectDTO."
    assert isinstance(
        PhysicalObject.from_dto(cursor_result.items[0]), PhysicalObject
    ), "Couldn't create pydantic model from DTO."
    assert hasattr(
        cursor_result, "cursor_data"
    ), "Expected cursor_result to have an additional data for cursor pagination."
    assert isinstance(list_result, list), "Result should be a list."
    assert all(isinstance(item, PhysicalObjectDTO) for item in list_result), "Each item should be a PhysicalObjectDTO."
    assert isinstance(
        PhysicalObject.from_dto(list_result[0]), PhysicalObject
    ), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))
    mock_conn.execute_mock.assert_any_call(str(recursive_statement))
    mock_conn.execute_mock.assert_any_call(str(statement_with_filters))


@pytest.mark.asyncio
async def test_get_physical_objects_with_geometry_by_territory_id_from_db(mock_conn: MockConnection):
    """Test the get_physical_objects_with_geometry_by_territory_id_from_db function."""

    # Arrange
    territory_id = 1
    include_child_territories, cities_only = False, False
    physical_object_type_id = 1
    name = "mock_string"
    limit, offset = 10, 0
    statement = (
        select(
            physical_objects_data,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            physical_object_types_dict.c.physical_object_function_id,
            physical_object_functions_dict.c.name.label("physical_object_function_name"),
            object_geometries_data.c.object_geometry_id,
            object_geometries_data.c.address,
            object_geometries_data.c.osm_id,
            cast(ST_AsGeoJSON(object_geometries_data.c.geometry, DECIMAL_PLACES), JSONB).label("geometry"),
            cast(ST_AsGeoJSON(object_geometries_data.c.centre_point, DECIMAL_PLACES), JSONB).label("centre_point"),
            living_buildings_data.c.living_building_id,
            living_buildings_data.c.living_area,
            living_buildings_data.c.properties.label("living_building_properties"),
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
        )
        .select_from(
            urban_objects_data.join(
                physical_objects_data,
                physical_objects_data.c.physical_object_id == urban_objects_data.c.physical_object_id,
            )
            .join(
                object_geometries_data,
                urban_objects_data.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
            )
            .join(territories_data, territories_data.c.territory_id == object_geometries_data.c.territory_id)
            .join(
                physical_object_types_dict,
                physical_objects_data.c.physical_object_type_id == physical_object_types_dict.c.physical_object_type_id,
            )
            .join(
                physical_object_functions_dict,
                physical_object_functions_dict.c.physical_object_function_id
                == physical_object_types_dict.c.physical_object_function_id,
            )
            .outerjoin(
                living_buildings_data,
                living_buildings_data.c.physical_object_id == physical_objects_data.c.physical_object_id,
            )
        )
        .distinct()
        .order_by(physical_objects_data.c.physical_object_id)
    )
    territories_cte = include_child_territories_cte(territory_id, True)
    recursive_statement = statement.where(
        object_geometries_data.c.territory_id.in_(select(territories_cte.c.territory_id))
    )
    statement = statement.where(object_geometries_data.c.territory_id == territory_id)
    statement_with_filters = statement.where(
        physical_objects_data.c.physical_object_type_id == physical_object_type_id,
        physical_objects_data.c.name.ilike(f"%{name}%"),
    )

    # Act
    with patch(
        "idu_api.urban_api.logic.impl.helpers.territories_physical_objects.check_existence"
    ) as mock_check_existence:
        mock_check_existence.return_value = False
        with pytest.raises(EntityNotFoundById):
            await get_physical_objects_with_geometry_by_territory_id_from_db(
                mock_conn,
                territory_id,
                None,
                None,
                None,
                include_child_territories,
                cities_only,
                None,
                "asc",
                paginate=False,
            )

    with patch("idu_api.urban_api.utils.pagination.verify_params") as mock_verify_params:
        mock_verify_params.return_value = (None, RawParams(limit=limit, offset=offset))
        statement = statement.offset(offset).limit(limit)
        page_result = await get_physical_objects_with_geometry_by_territory_id_from_db(
            mock_conn,
            territory_id,
            None,
            None,
            None,
            include_child_territories,
            cities_only,
            None,
            "asc",
            paginate=True,
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
            cursor_result = await get_physical_objects_with_geometry_by_territory_id_from_db(
                mock_conn,
                territory_id,
                None,
                None,
                None,
                include_child_territories,
                cities_only,
                None,
                "asc",
                paginate=True,
            )

    await get_physical_objects_with_geometry_by_territory_id_from_db(
        mock_conn, territory_id, None, None, None, True, True, None, "asc", paginate=False
    )
    list_result = await get_physical_objects_with_geometry_by_territory_id_from_db(
        mock_conn,
        territory_id,
        physical_object_type_id,
        None,
        name,
        include_child_territories,
        cities_only,
        None,
        "asc",
        paginate=False,
    )
    geojson_result = await GeoJSONResponse.from_list([r.to_geojson_dict() for r in list_result])

    # Assert
    assert isinstance(page_result, PageDTO), "Result should be a PageDTO."
    assert all(
        isinstance(item, PhysicalObjectWithGeometryDTO) for item in page_result.items
    ), "Each item should be a PhysicalObjectWithGeometryDTO."
    assert isinstance(
        PhysicalObjectWithGeometry.from_dto(page_result.items[0]), PhysicalObjectWithGeometry
    ), "Couldn't create pydantic model from DTO."
    assert isinstance(cursor_result, PageDTO), "Result should be a PageDTO."
    assert all(
        isinstance(item, PhysicalObjectWithGeometryDTO) for item in cursor_result.items
    ), "Each item should be a PhysicalObjectWithGeometryDTO."
    assert isinstance(
        PhysicalObjectWithGeometry.from_dto(cursor_result.items[0]), PhysicalObjectWithGeometry
    ), "Couldn't create pydantic model from DTO."
    assert hasattr(
        cursor_result, "cursor_data"
    ), "Expected cursor_result to have an additional data for cursor pagination."
    assert isinstance(list_result, list), "Result should be a list."
    assert all(
        isinstance(item, PhysicalObjectWithGeometryDTO) for item in list_result
    ), "Each item should be a PhysicalObjectWithGeometryDTO."
    assert isinstance(
        PhysicalObject(**geojson_result.features[0].properties), PhysicalObject
    ), "Couldn't create pydantic model from geojson properties."
    mock_conn.execute_mock.assert_any_call(str(statement))
    mock_conn.execute_mock.assert_any_call(str(recursive_statement))
    mock_conn.execute_mock.assert_any_call(str(statement_with_filters))
