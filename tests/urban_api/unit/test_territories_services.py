"""Unit tests for territory-related services are defined here."""

from collections.abc import Callable
from unittest.mock import AsyncMock, patch

import pytest
from fastapi_pagination.bases import CursorRawParams, RawParams
from geoalchemy2.functions import ST_AsEWKB
from sqlalchemy import func, select

from idu_api.common.db.entities import (
    object_geometries_data,
    service_types_dict,
    services_data,
    territories_data,
    territory_types_dict,
    urban_functions_dict,
    urban_objects_data,
)
from idu_api.urban_api.dto import PageDTO, ServiceDTO, ServicesCountCapacityDTO, ServiceTypeDTO, ServiceWithGeometryDTO
from idu_api.urban_api.exceptions.logic.common import EntityNotFoundById
from idu_api.urban_api.logic.impl.helpers.territories_services import (
    get_service_types_by_territory_id_from_db,
    get_services_by_territory_id_from_db,
    get_services_capacity_by_territory_id_from_db,
    get_services_with_geometry_by_territory_id_from_db,
)
from idu_api.urban_api.logic.impl.helpers.utils import include_child_territories_cte
from idu_api.urban_api.schemas import Service, ServicesCountCapacity, ServiceType, ServiceWithGeometry
from idu_api.urban_api.schemas.geometries import GeoJSONResponse
from tests.urban_api.helpers.connection import MockConnection

func: Callable

####################################################################################
#                           Default use-case tests                                 #
####################################################################################


@pytest.mark.asyncio
async def test_get_service_types_by_territory_id_from_db(mock_conn: MockConnection):
    """Test the get_service_types_by_territory_id_from_db function."""

    # Arrange
    territory_id = 1
    include_child_territories, cities_only = False, False
    statement = (
        select(service_types_dict, urban_functions_dict.c.name.label("urban_function_name"))
        .select_from(
            urban_objects_data.join(services_data, services_data.c.service_id == urban_objects_data.c.service_id)
            .join(
                object_geometries_data,
                urban_objects_data.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
            )
            .join(
                territories_data,
                territories_data.c.territory_id == object_geometries_data.c.territory_id,
            )
            .join(service_types_dict, service_types_dict.c.service_type_id == services_data.c.service_type_id)
            .join(
                urban_functions_dict,
                service_types_dict.c.urban_function_id == urban_functions_dict.c.urban_function_id,
            )
        )
        .order_by(service_types_dict.c.service_type_id)
        .distinct()
    )
    territories_cte = include_child_territories_cte(territory_id, True)
    recursive_statement = statement.where(
        object_geometries_data.c.territory_id.in_(select(territories_cte.c.territory_id))
    )
    statement = statement.where(object_geometries_data.c.territory_id == territory_id)

    # Act
    with patch("idu_api.urban_api.logic.impl.helpers.territories_services.check_existence") as mock_check_existence:
        mock_check_existence.return_value = False
        with pytest.raises(EntityNotFoundById):
            await get_service_types_by_territory_id_from_db(
                mock_conn, territory_id, include_child_territories, cities_only
            )
    await get_service_types_by_territory_id_from_db(mock_conn, territory_id, True, True)
    result = await get_service_types_by_territory_id_from_db(
        mock_conn, territory_id, include_child_territories, cities_only
    )

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(isinstance(item, ServiceTypeDTO) for item in result), "Each item should be a ServiceTypeDTO."
    assert isinstance(ServiceType.from_dto(result[0]), ServiceType), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))
    mock_conn.execute_mock.assert_any_call(str(recursive_statement))


@pytest.mark.asyncio
async def test_get_services_by_territory_id_from_db(mock_conn: MockConnection):
    """Test the get_services_by_territory_id_from_db function."""

    # Arrange
    territory_id = 1
    include_child_territories, cities_only = False, False
    service_type_id = 1
    name = "mock_string"
    limit, offset = 10, 0
    statement = (
        select(
            services_data,
            service_types_dict.c.urban_function_id,
            urban_functions_dict.c.name.label("urban_function_name"),
            service_types_dict.c.name.label("service_type_name"),
            service_types_dict.c.capacity_modeled.label("service_type_capacity_modeled"),
            service_types_dict.c.code.label("service_type_code"),
            service_types_dict.c.infrastructure_type,
            service_types_dict.c.properties.label("service_type_properties"),
            territory_types_dict.c.name.label("territory_type_name"),
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
        )
        .select_from(
            urban_objects_data.join(services_data, services_data.c.service_id == urban_objects_data.c.service_id)
            .join(
                object_geometries_data,
                urban_objects_data.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
            )
            .join(territories_data, territories_data.c.territory_id == object_geometries_data.c.territory_id)
            .join(service_types_dict, service_types_dict.c.service_type_id == services_data.c.service_type_id)
            .join(
                urban_functions_dict,
                service_types_dict.c.urban_function_id == urban_functions_dict.c.urban_function_id,
            )
            .outerjoin(
                territory_types_dict, territory_types_dict.c.territory_type_id == services_data.c.territory_type_id
            )
        )
        .distinct()
        .order_by(services_data.c.service_id)
    )
    territories_cte = include_child_territories_cte(territory_id, True)
    recursive_statement = statement.where(
        object_geometries_data.c.territory_id.in_(select(territories_cte.c.territory_id))
    )
    statement = statement.where(object_geometries_data.c.territory_id == territory_id)
    statement_with_filters = statement.where(
        services_data.c.service_type_id == service_type_id,
        services_data.c.name.ilike(f"%{name}%"),
    )

    # Act
    with patch("idu_api.urban_api.logic.impl.helpers.territories_services.check_existence") as mock_check_existence:
        mock_check_existence.return_value = False
        with pytest.raises(EntityNotFoundById):
            await get_services_by_territory_id_from_db(
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
        page_result = await get_services_by_territory_id_from_db(
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
            cursor_result = await get_services_by_territory_id_from_db(
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

    await get_services_by_territory_id_from_db(
        mock_conn, territory_id, None, None, None, True, True, None, "asc", paginate=False
    )
    list_result = await get_services_by_territory_id_from_db(
        mock_conn,
        territory_id,
        service_type_id,
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
    assert all(isinstance(item, ServiceDTO) for item in page_result.items), "Each item should be a ServiceDTO."
    assert isinstance(Service.from_dto(page_result.items[0]), Service), "Couldn't create pydantic model from DTO."
    assert isinstance(cursor_result, PageDTO), "Result should be a PageDTO."
    assert all(isinstance(item, ServiceDTO) for item in cursor_result.items), "Each item should be a ServiceDTO."
    assert isinstance(Service.from_dto(cursor_result.items[0]), Service), "Couldn't create pydantic model from DTO."
    assert hasattr(
        cursor_result, "cursor_data"
    ), "Expected cursor_result to have an additional data for cursor pagination."
    assert isinstance(list_result, list), "Result should be a list."
    assert all(isinstance(item, ServiceDTO) for item in list_result), "Each item should be a ServiceDTO."
    assert isinstance(Service.from_dto(list_result[0]), Service), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))
    mock_conn.execute_mock.assert_any_call(str(recursive_statement))
    mock_conn.execute_mock.assert_any_call(str(statement_with_filters))


@pytest.mark.asyncio
async def test_get_services_with_geometry_by_territory_id_from_db(mock_conn: MockConnection):
    """Test the get_services_with_geometry_by_territory_id_from_db function."""

    # Arrange
    territory_id = 1
    include_child_territories, cities_only = False, False
    service_type_id = 1
    name = "mock_string"
    limit, offset = 10, 0
    statement = (
        select(
            services_data,
            service_types_dict.c.urban_function_id,
            urban_functions_dict.c.name.label("urban_function_name"),
            service_types_dict.c.name.label("service_type_name"),
            service_types_dict.c.capacity_modeled.label("service_type_capacity_modeled"),
            service_types_dict.c.code.label("service_type_code"),
            service_types_dict.c.infrastructure_type,
            service_types_dict.c.properties.label("service_type_properties"),
            territory_types_dict.c.name.label("territory_type_name"),
            object_geometries_data.c.object_geometry_id,
            object_geometries_data.c.address,
            object_geometries_data.c.osm_id,
            ST_AsEWKB(object_geometries_data.c.geometry).label("geometry"),
            ST_AsEWKB(object_geometries_data.c.centre_point).label("centre_point"),
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
        )
        .select_from(
            urban_objects_data.join(services_data, services_data.c.service_id == urban_objects_data.c.service_id)
            .join(
                object_geometries_data,
                urban_objects_data.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
            )
            .join(territories_data, territories_data.c.territory_id == object_geometries_data.c.territory_id)
            .join(service_types_dict, service_types_dict.c.service_type_id == services_data.c.service_type_id)
            .join(
                urban_functions_dict,
                service_types_dict.c.urban_function_id == urban_functions_dict.c.urban_function_id,
            )
            .outerjoin(
                territory_types_dict, territory_types_dict.c.territory_type_id == services_data.c.territory_type_id
            )
        )
        .distinct()
        .order_by(services_data.c.service_id)
    )
    territories_cte = include_child_territories_cte(territory_id, True)
    recursive_statement = statement.where(
        object_geometries_data.c.territory_id.in_(select(territories_cte.c.territory_id))
    )
    statement = statement.where(object_geometries_data.c.territory_id == territory_id)
    statement_with_filters = statement.where(
        services_data.c.service_type_id == service_type_id,
        services_data.c.name.ilike(f"%{name}%"),
    )

    # Act
    with patch("idu_api.urban_api.logic.impl.helpers.territories_services.check_existence") as mock_check_existence:
        mock_check_existence.return_value = False
        with pytest.raises(EntityNotFoundById):
            await get_services_with_geometry_by_territory_id_from_db(
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
        page_result = await get_services_with_geometry_by_territory_id_from_db(
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
            cursor_result = await get_services_with_geometry_by_territory_id_from_db(
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

    await get_services_with_geometry_by_territory_id_from_db(
        mock_conn, territory_id, None, None, None, True, True, None, "asc", paginate=False
    )
    list_result = await get_services_with_geometry_by_territory_id_from_db(
        mock_conn,
        territory_id,
        service_type_id,
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
        isinstance(item, ServiceWithGeometryDTO) for item in page_result.items
    ), "Each item should be a ServiceWithGeometryDTO."
    assert isinstance(
        ServiceWithGeometry.from_dto(page_result.items[0]), ServiceWithGeometry
    ), "Couldn't create pydantic model from DTO."
    assert isinstance(cursor_result, PageDTO), "Result should be a PageDTO."
    assert all(
        isinstance(item, ServiceWithGeometryDTO) for item in cursor_result.items
    ), "Each item should be a ServiceWithGeometryDTO."
    assert isinstance(
        ServiceWithGeometry.from_dto(cursor_result.items[0]), ServiceWithGeometry
    ), "Couldn't create pydantic model from DTO."
    assert hasattr(
        cursor_result, "cursor_data"
    ), "Expected cursor_result to have an additional data for cursor pagination."
    assert isinstance(list_result, list), "Result should be a list."
    assert all(
        isinstance(item, ServiceWithGeometryDTO) for item in list_result
    ), "Each item should be a ServiceWithGeometryDTO."
    assert isinstance(
        Service(**geojson_result.features[0].properties), Service
    ), "Couldn't create pydantic model from geojson properties."
    mock_conn.execute_mock.assert_any_call(str(statement))
    mock_conn.execute_mock.assert_any_call(str(recursive_statement))
    mock_conn.execute_mock.assert_any_call(str(statement_with_filters))


@pytest.mark.asyncio
async def test_get_services_capacity_by_territory_id_from_db(mock_conn: MockConnection):
    """Test the get_services_capacity_by_territory_id_from_db function."""

    # Arrange
    territory_id = 1
    level = 2
    service_type_id = 1
    territories_cte = (
        select(territories_data.c.territory_id, territories_data.c.parent_id, territories_data.c.level)
        .where(territories_data.c.territory_id == territory_id)
        .cte(recursive=True)
    )
    territories_cte = territories_cte.union_all(
        select(territories_data.c.territory_id, territories_data.c.parent_id, territories_data.c.level).where(
            territories_data.c.parent_id == territories_cte.c.territory_id
        )
    )
    level_territories = select(territories_cte).where(territories_cte.c.level >= level).alias("level_territories")
    statement = (
        select(
            level_territories.c.territory_id,
            func.count(services_data.c.service_id).label("count"),
            func.coalesce(func.sum(services_data.c.capacity), 0).label("capacity"),
        )
        .select_from(
            level_territories.outerjoin(
                object_geometries_data, level_territories.c.territory_id == object_geometries_data.c.territory_id
            )
            .outerjoin(
                urban_objects_data,
                urban_objects_data.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
            )
            .outerjoin(services_data, services_data.c.service_id == urban_objects_data.c.service_id)
        )
        .group_by(level_territories.c.territory_id)
        .where(services_data.c.service_type_id == service_type_id)
    )

    # Act
    with patch("idu_api.urban_api.logic.impl.helpers.territories_services.check_existence") as mock_check_existence:
        mock_check_existence.return_value = False
        with pytest.raises(EntityNotFoundById):
            await get_services_capacity_by_territory_id_from_db(mock_conn, territory_id, level, service_type_id)
    result = await get_services_capacity_by_territory_id_from_db(mock_conn, territory_id, level, service_type_id)

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(
        isinstance(item, ServicesCountCapacityDTO) for item in result
    ), "Each item should be a ServicesCountCapacityDTO."
    assert all(
        isinstance(ServicesCountCapacity.from_dto(item), ServicesCountCapacity) for item in result
    ), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))
