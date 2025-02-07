"""Unit tests for territory objects are defined here."""

from collections.abc import Callable
from datetime import date, datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest
from fastapi_pagination.bases import CursorRawParams, RawParams
from geoalchemy2.functions import ST_AsGeoJSON, ST_GeomFromText
from shapely.geometry import LineString, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon
from sqlalchemy import cast, func, insert, select, text, update
from sqlalchemy.dialects.postgresql import JSONB

from idu_api.common.db.entities import (
    target_city_types_dict,
    territories_data,
    territory_types_dict,
)
from idu_api.urban_api.dto import PageDTO, TerritoryDTO, TerritoryWithoutGeometryDTO
from idu_api.urban_api.exceptions.logic.common import EntitiesNotFoundByIds, EntityNotFoundById, TooManyObjectsError
from idu_api.urban_api.logic.impl.helpers.territories_objects import (
    add_territory_to_db,
    get_common_territory_for_geometry,
    get_intersecting_territories_for_geometry,
    get_territories_by_ids,
    get_territories_by_parent_id_from_db,
    get_territories_without_geometry_by_parent_id_from_db,
    patch_territory_to_db,
    put_territory_to_db,
)
from idu_api.urban_api.logic.impl.helpers.utils import DECIMAL_PLACES, OBJECTS_NUMBER_LIMIT, build_recursive_query
from idu_api.urban_api.schemas import Territory, TerritoryPatch, TerritoryPost, TerritoryPut, TerritoryWithoutGeometry
from idu_api.urban_api.schemas.geometries import GeoJSONResponse
from tests.urban_api.helpers.connection import MockConnection

func: Callable
Geom = Point | Polygon | MultiPolygon | LineString | MultiLineString | MultiPoint


@pytest.mark.asyncio
async def test_get_territories_by_ids(mock_conn: MockConnection):
    """Test the get_territories_by_ids function."""

    # Arrange
    ids = [1]
    not_found_ids = [1, 2]
    too_many_ids = list(range(OBJECTS_NUMBER_LIMIT + 1))
    territories_data_parents = territories_data.alias("territories_data_parents")
    admin_centers = territories_data.alias("admin_centers")
    statement = (
        select(
            territories_data.c.territory_id,
            territories_data.c.territory_type_id,
            territory_types_dict.c.name.label("territory_type_name"),
            territories_data.c.parent_id,
            territories_data_parents.c.name.label("parent_name"),
            territories_data.c.name,
            cast(ST_AsGeoJSON(territories_data.c.geometry, DECIMAL_PLACES), JSONB).label("geometry"),
            territories_data.c.level,
            territories_data.c.properties,
            cast(ST_AsGeoJSON(territories_data.c.centre_point, DECIMAL_PLACES), JSONB).label("centre_point"),
            territories_data.c.admin_center_id,
            admin_centers.c.name.label("admin_center_name"),
            territories_data.c.target_city_type_id,
            target_city_types_dict.c.name.label("target_city_type_name"),
            target_city_types_dict.c.description.label("target_city_type_description"),
            territories_data.c.okato_code,
            territories_data.c.oktmo_code,
            territories_data.c.is_city,
            territories_data.c.created_at,
            territories_data.c.updated_at,
        )
        .select_from(
            territories_data.join(
                territory_types_dict, territory_types_dict.c.territory_type_id == territories_data.c.territory_type_id
            )
            .outerjoin(
                target_city_types_dict,
                target_city_types_dict.c.target_city_type_id == territories_data.c.target_city_type_id,
            )
            .outerjoin(
                territories_data_parents,
                territories_data_parents.c.territory_id == territories_data.c.parent_id,
            )
            .outerjoin(admin_centers, admin_centers.c.territory_id == territories_data.c.admin_center_id)
        )
        .where(territories_data.c.territory_id.in_(ids))
    )

    # Act
    with pytest.raises(EntitiesNotFoundByIds):
        await get_territories_by_ids(mock_conn, not_found_ids)
    with pytest.raises(TooManyObjectsError):
        await get_territories_by_ids(mock_conn, too_many_ids)
    result = await get_territories_by_ids(mock_conn, ids)

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(isinstance(item, TerritoryDTO) for item in result), "Each item should be an TerritoryDTO."
    assert isinstance(Territory.from_dto(result[0]), Territory), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))


@pytest.mark.asyncio
async def test_add_territory_to_db(mock_conn: MockConnection, territory_post_req: TerritoryPost):
    """Test the add_territory_to_db function."""

    # Arrange
    async def check_parent_territory(conn, table, conditions):
        if table == territories_data and conditions == {"territory_id": territory_post_req.parent_id}:
            return False
        return True

    async def check_admin_center(conn, table, conditions):
        if table == territories_data and conditions == {"territory_id": territory_post_req.admin_center_id}:
            return False
        return True

    async def check_territory_type(conn, table, conditions):
        if table == territory_types_dict:
            return False
        return True

    async def check_target_city_type(conn, table, conditions):
        if table == target_city_types_dict:
            return False
        return True

    statement = (
        insert(territories_data)
        .values(
            name=territory_post_req.name,
            geometry=ST_GeomFromText(territory_post_req.geometry.as_shapely_geometry().wkt, text("4326")),
            centre_point=ST_GeomFromText(territory_post_req.centre_point.as_shapely_geometry().wkt, text("4326")),
            territory_type_id=territory_post_req.territory_type_id,
            parent_id=territory_post_req.parent_id,
            properties=territory_post_req.properties,
            admin_center_id=territory_post_req.admin_center_id,
            target_city_type_id=territory_post_req.target_city_type_id,
            okato_code=territory_post_req.okato_code,
            oktmo_code=territory_post_req.oktmo_code,
            is_city=territory_post_req.is_city,
        )
        .returning(territories_data.c.territory_id)
    )

    # Act
    with patch(
        "idu_api.urban_api.logic.impl.helpers.territories_objects.check_existence",
        new=AsyncMock(side_effect=check_parent_territory),
    ):
        with pytest.raises(EntityNotFoundById):
            await add_territory_to_db(mock_conn, territory_post_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.territories_objects.check_existence",
        new=AsyncMock(side_effect=check_admin_center),
    ):
        with pytest.raises(EntityNotFoundById):
            await add_territory_to_db(mock_conn, territory_post_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.territories_objects.check_existence",
        new=AsyncMock(side_effect=check_territory_type),
    ):
        with pytest.raises(EntityNotFoundById):
            await add_territory_to_db(mock_conn, territory_post_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.territories_objects.check_existence",
        new=AsyncMock(side_effect=check_target_city_type),
    ):
        with pytest.raises(EntityNotFoundById):
            await add_territory_to_db(mock_conn, territory_post_req)

    result = await add_territory_to_db(mock_conn, territory_post_req)

    # Assert
    assert isinstance(result, TerritoryDTO), "Result should be an TerritoryDTO."
    assert isinstance(Territory.from_dto(result), Territory), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))
    mock_conn.commit_mock.assert_called_once()


@pytest.mark.asyncio
async def test_put_territory_to_db(mock_conn: MockConnection, territory_put_req: TerritoryPut):
    """Test the put_territory_to_db function."""

    # Arrange
    territory_id = 1

    async def check_territory(conn, table, conditions):
        if table == territories_data and conditions == {"territory_id": territory_id}:
            return False
        return True

    async def check_parent_territory(conn, table, conditions):
        if table == territories_data and conditions == {"territory_id": territory_put_req.parent_id}:
            return False
        return True

    async def check_admin_center(conn, table, conditions):
        if table == territories_data and conditions == {"territory_id": territory_put_req.admin_center_id}:
            return False
        return True

    async def check_territory_type(conn, table, conditions):
        if table == territory_types_dict:
            return False
        return True

    async def check_target_city_type(conn, table, conditions):
        if table == target_city_types_dict:
            return False
        return True

    statement_update = (
        update(territories_data)
        .where(territories_data.c.territory_id == territory_id)
        .values(
            name=territory_put_req.name,
            geometry=ST_GeomFromText(territory_put_req.geometry.as_shapely_geometry().wkt, text("4326")),
            centre_point=ST_GeomFromText(territory_put_req.centre_point.as_shapely_geometry().wkt, text("4326")),
            territory_type_id=territory_put_req.territory_type_id,
            parent_id=territory_put_req.parent_id,
            properties=territory_put_req.properties,
            admin_center_id=territory_put_req.admin_center_id,
            target_city_type_id=territory_put_req.target_city_type_id,
            okato_code=territory_put_req.okato_code,
            oktmo_code=territory_put_req.oktmo_code,
            is_city=territory_put_req.is_city,
            updated_at=datetime.now(timezone.utc),
        )
    )

    # Act
    with patch(
        "idu_api.urban_api.logic.impl.helpers.territories_objects.check_existence",
        new=AsyncMock(side_effect=check_territory),
    ):
        with pytest.raises(EntityNotFoundById):
            await put_territory_to_db(mock_conn, territory_id, territory_put_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.territories_objects.check_existence",
        new=AsyncMock(side_effect=check_parent_territory),
    ):
        with pytest.raises(EntityNotFoundById):
            await put_territory_to_db(mock_conn, territory_id, territory_put_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.territories_objects.check_existence",
        new=AsyncMock(side_effect=check_admin_center),
    ):
        with pytest.raises(EntityNotFoundById):
            await put_territory_to_db(mock_conn, territory_id, territory_put_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.territories_objects.check_existence",
        new=AsyncMock(side_effect=check_territory_type),
    ):
        with pytest.raises(EntityNotFoundById):
            await put_territory_to_db(mock_conn, territory_id, territory_put_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.territories_objects.check_existence",
        new=AsyncMock(side_effect=check_target_city_type),
    ):
        with pytest.raises(EntityNotFoundById):
            await put_territory_to_db(mock_conn, territory_id, territory_put_req)
    result = await put_territory_to_db(mock_conn, territory_id, territory_put_req)

    # Assert
    assert isinstance(result, TerritoryDTO), "Result should be an TerritoryDTO."
    assert isinstance(Territory.from_dto(result), Territory), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement_update))
    mock_conn.commit_mock.assert_called_once()


@pytest.mark.asyncio
async def test_patch_territory_to_db(mock_conn: MockConnection, territory_patch_req: TerritoryPatch):
    """Test the patch_territory_to_db function."""

    # Arrange
    territory_id = 1

    async def check_territory(conn, table, conditions):
        if table == territories_data and conditions == {"territory_id": territory_id}:
            return False
        return True

    async def check_parent_territory(conn, table, conditions):
        if table == territories_data and conditions == {"territory_id": territory_patch_req.parent_id}:
            return False
        return True

    async def check_admin_center(conn, table, conditions):
        if table == territories_data and conditions == {"territory_id": territory_patch_req.admin_center_id}:
            return False
        return True

    async def check_territory_type(conn, table, conditions):
        if table == territory_types_dict:
            return False
        return True

    async def check_target_city_type(conn, table, conditions):
        if table == target_city_types_dict:
            return False
        return True

    statement_update = (
        update(territories_data)
        .where(territories_data.c.territory_id == territory_id)
        .values(**territory_patch_req.model_dump(exclude_unset=True), updated_at=datetime.now(timezone.utc))
    )

    # Act
    with patch(
        "idu_api.urban_api.logic.impl.helpers.territories_objects.check_existence",
        new=AsyncMock(side_effect=check_territory),
    ):
        with pytest.raises(EntityNotFoundById):
            await patch_territory_to_db(mock_conn, territory_id, territory_patch_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.territories_objects.check_existence",
        new=AsyncMock(side_effect=check_parent_territory),
    ):
        with pytest.raises(EntityNotFoundById):
            await patch_territory_to_db(mock_conn, territory_id, territory_patch_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.territories_objects.check_existence",
        new=AsyncMock(side_effect=check_admin_center),
    ):
        with pytest.raises(EntityNotFoundById):
            await patch_territory_to_db(mock_conn, territory_id, territory_patch_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.territories_objects.check_existence",
        new=AsyncMock(side_effect=check_territory_type),
    ):
        with pytest.raises(EntityNotFoundById):
            await patch_territory_to_db(mock_conn, territory_id, territory_patch_req)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.territories_objects.check_existence",
        new=AsyncMock(side_effect=check_target_city_type),
    ):
        with pytest.raises(EntityNotFoundById):
            await patch_territory_to_db(mock_conn, territory_id, territory_patch_req)
    result = await patch_territory_to_db(mock_conn, territory_id, territory_patch_req)

    # Assert
    assert isinstance(result, TerritoryDTO), "Result should be an instance of TerritoryDTO."
    assert isinstance(Territory.from_dto(result), Territory), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement_update))
    mock_conn.commit_mock.assert_called_once()


@pytest.mark.asyncio
async def test_get_territories_by_parent_id_from_db(mock_conn: MockConnection):
    """Test the get_territories_by_parent_id_from_db function."""

    # Arrange
    parent_id = 1
    filters = {
        "territory_type_id": 1,
        "name": "Test Territory",
        "cities_only": True,
        "created_at": date.today(),
        "order_by": None,
        "ordering": "asc",
    }
    limit, offset = 10, 0
    territories_data_parents = territories_data.alias("territories_data_parents")
    admin_centers = territories_data.alias("admin_centers")
    statement = select(
        territories_data.c.territory_id,
        territories_data.c.territory_type_id,
        territory_types_dict.c.name.label("territory_type_name"),
        territories_data.c.parent_id,
        territories_data_parents.c.name.label("parent_name"),
        territories_data.c.name,
        cast(ST_AsGeoJSON(territories_data.c.geometry, DECIMAL_PLACES), JSONB).label("geometry"),
        territories_data.c.level,
        territories_data.c.properties,
        cast(ST_AsGeoJSON(territories_data.c.centre_point, DECIMAL_PLACES), JSONB).label("centre_point"),
        territories_data.c.admin_center_id,
        admin_centers.c.name.label("admin_center_name"),
        territories_data.c.target_city_type_id,
        target_city_types_dict.c.name.label("target_city_type_name"),
        target_city_types_dict.c.description.label("target_city_type_description"),
        territories_data.c.okato_code,
        territories_data.c.oktmo_code,
        territories_data.c.is_city,
        territories_data.c.created_at,
        territories_data.c.updated_at,
    ).select_from(
        territories_data.join(
            territory_types_dict, territory_types_dict.c.territory_type_id == territories_data.c.territory_type_id
        )
        .outerjoin(
            target_city_types_dict,
            target_city_types_dict.c.target_city_type_id == territories_data.c.target_city_type_id,
        )
        .outerjoin(
            territories_data_parents,
            territories_data_parents.c.territory_id == territories_data.c.parent_id,
        )
        .outerjoin(admin_centers, admin_centers.c.territory_id == territories_data.c.admin_center_id)
    )
    recursive_statement = build_recursive_query(
        statement, territories_data, parent_id, "territories_recursive", "territory_id"
    )
    statement = statement.where(territories_data.c.parent_id == parent_id)
    requested_territories = statement.cte("requested_territories")
    requested_recursive_territories = recursive_statement.cte("requested_territories")
    statement = select(requested_territories).order_by(requested_territories.c.territory_id)
    recursive_statement = select(requested_recursive_territories).order_by(
        requested_recursive_territories.c.territory_id
    )
    statement_with_filters = statement.where(
        requested_territories.c.is_city.is_(filters["cities_only"]),
        requested_territories.c.name.ilike(f"%{filters['cities_only']}%"),
        func.date(requested_territories.c.created_at) == filters["created_at"],
        requested_territories.c.territory_type_id == filters["territory_type_id"],
    )

    # Act
    with patch("idu_api.urban_api.logic.impl.helpers.territories_objects.check_existence") as mock_check_existence:
        mock_check_existence.return_value = False
        with pytest.raises(EntityNotFoundById):
            await get_territories_by_parent_id_from_db(mock_conn, parent_id, False, **filters, paginate=False)

    with patch("idu_api.urban_api.utils.pagination.verify_params") as mock_verify_params:
        mock_verify_params.return_value = (None, RawParams(limit=limit, offset=offset))
        statement = statement.offset(offset).limit(limit)
        page_result = await get_territories_by_parent_id_from_db(
            mock_conn, parent_id, False, None, None, None, None, None, "asc", paginate=True
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
            cursor_result = await get_territories_by_parent_id_from_db(
                mock_conn, parent_id, False, **filters, paginate=True
            )

    await get_territories_by_parent_id_from_db(
        mock_conn, parent_id, True, None, None, None, None, None, "asc", paginate=False
    )
    list_result = await get_territories_by_parent_id_from_db(mock_conn, parent_id, False, **filters, paginate=False)
    geojson_result = await GeoJSONResponse.from_list([r.to_geojson_dict() for r in list_result])

    # Assert
    assert isinstance(page_result, PageDTO), "Result should be a PageDTO."
    assert all(isinstance(item, TerritoryDTO) for item in page_result.items), "Each item should be a TerritoryDTO."
    assert isinstance(Territory.from_dto(page_result.items[0]), Territory), "Couldn't create pydantic model from DTO."
    assert isinstance(cursor_result, PageDTO), "Result should be a PageDTO."
    assert all(isinstance(item, TerritoryDTO) for item in cursor_result.items), "Each item should be a TerritoryDTO."
    assert isinstance(Territory.from_dto(cursor_result.items[0]), Territory), "Couldn't create pydantic model from DTO."
    assert hasattr(
        cursor_result, "cursor_data"
    ), "Expected cursor_result to have an additional data for cursor pagination."
    assert isinstance(list_result, list), "Result should be a list."
    assert all(isinstance(item, TerritoryDTO) for item in list_result), "Each item should be a TerritoryDTO."
    assert isinstance(
        TerritoryWithoutGeometry(**geojson_result.features[0].properties), TerritoryWithoutGeometry
    ), "Couldn't create pydantic model from geojson properties."
    mock_conn.execute_mock.assert_any_call(str(statement))
    mock_conn.execute_mock.assert_any_call(str(recursive_statement))
    mock_conn.execute_mock.assert_any_call(str(statement_with_filters))


@pytest.mark.asyncio
async def test_get_territories_without_geometry_by_parent_id_from_db(mock_conn: MockConnection):
    """Test the get_territories_without_geometry_by_parent_id_from_db function."""

    # Arrange
    parent_id = 1
    filters = {
        "territory_type_id": 1,
        "name": "Test Territory",
        "cities_only": True,
        "created_at": date.today(),
        "order_by": None,
        "ordering": "asc",
    }
    limit, offset = 10, 0
    territories_data_parents = territories_data.alias("territories_data_parents")
    admin_centers = territories_data.alias("admin_centers")
    statement = select(
        territories_data.c.territory_id,
        territories_data.c.territory_type_id,
        territory_types_dict.c.name.label("territory_type_name"),
        territories_data.c.parent_id,
        territories_data_parents.c.name.label("parent_name"),
        territories_data.c.name,
        territories_data.c.level,
        territories_data.c.properties,
        territories_data.c.admin_center_id,
        admin_centers.c.name.label("admin_center_name"),
        territories_data.c.target_city_type_id,
        target_city_types_dict.c.name.label("target_city_type_name"),
        target_city_types_dict.c.description.label("target_city_type_description"),
        territories_data.c.okato_code,
        territories_data.c.oktmo_code,
        territories_data.c.is_city,
        territories_data.c.created_at,
        territories_data.c.updated_at,
    ).select_from(
        territories_data.join(
            territory_types_dict, territory_types_dict.c.territory_type_id == territories_data.c.territory_type_id
        )
        .outerjoin(
            target_city_types_dict,
            target_city_types_dict.c.target_city_type_id == territories_data.c.target_city_type_id,
        )
        .outerjoin(
            territories_data_parents,
            territories_data_parents.c.territory_id == territories_data.c.parent_id,
        )
        .outerjoin(admin_centers, admin_centers.c.territory_id == territories_data.c.admin_center_id)
    )
    recursive_statement = build_recursive_query(
        statement, territories_data, parent_id, "territories_recursive", "territory_id"
    )
    statement = statement.where(territories_data.c.parent_id == parent_id)
    requested_territories = statement.cte("requested_territories")
    requested_recursive_territories = recursive_statement.cte("requested_territories")
    statement = select(requested_territories).order_by(requested_territories.c.territory_id)
    recursive_statement = select(requested_recursive_territories).order_by(
        requested_recursive_territories.c.territory_id
    )
    statement_with_filters = statement.where(
        requested_territories.c.is_city.is_(filters["cities_only"]),
        requested_territories.c.name.ilike(f"%{filters['cities_only']}%"),
        func.date(requested_territories.c.created_at) == filters["created_at"],
        requested_territories.c.territory_type_id == filters["territory_type_id"],
    )

    # Act
    with patch("idu_api.urban_api.logic.impl.helpers.territories_objects.check_existence") as mock_check_existence:
        mock_check_existence.return_value = False
        with pytest.raises(EntityNotFoundById):
            await get_territories_without_geometry_by_parent_id_from_db(
                mock_conn, parent_id, False, **filters, paginate=False
            )

    with patch("idu_api.urban_api.utils.pagination.verify_params") as mock_verify_params:
        mock_verify_params.return_value = (None, RawParams(limit=limit, offset=offset))
        statement = statement.offset(offset).limit(limit)
        page_result = await get_territories_without_geometry_by_parent_id_from_db(
            mock_conn, parent_id, False, None, None, None, None, None, "asc", paginate=True
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
            cursor_result = await get_territories_without_geometry_by_parent_id_from_db(
                mock_conn, parent_id, False, **filters, paginate=True
            )

    await get_territories_without_geometry_by_parent_id_from_db(
        mock_conn, parent_id, True, None, None, None, None, None, "asc", paginate=False
    )
    list_result = await get_territories_without_geometry_by_parent_id_from_db(
        mock_conn, parent_id, False, **filters, paginate=False
    )

    # Assert
    assert isinstance(page_result, PageDTO), "Result should be a PageDTO."
    assert all(
        isinstance(item, TerritoryWithoutGeometryDTO) for item in page_result.items
    ), "Each item should be a TerritoryWithoutGeometryDTO."
    assert isinstance(
        TerritoryWithoutGeometry.from_dto(page_result.items[0]), TerritoryWithoutGeometry
    ), "Couldn't create pydantic model from DTO."
    assert isinstance(cursor_result, PageDTO), "Result should be a PageDTO."
    assert all(
        isinstance(item, TerritoryWithoutGeometryDTO) for item in cursor_result.items
    ), "Each item should be a TerritoryWithoutGeometryDTO."
    assert isinstance(
        TerritoryWithoutGeometry.from_dto(cursor_result.items[0]), TerritoryWithoutGeometry
    ), "Couldn't create pydantic model from DTO."
    assert hasattr(
        cursor_result, "cursor_data"
    ), "Expected cursor_result to have an additional data for cursor pagination."
    assert isinstance(list_result, list), "Result should be a list."
    assert all(
        isinstance(item, TerritoryWithoutGeometryDTO) for item in list_result
    ), "Each item should be a TerritoryWithoutGeometryDTO."
    assert isinstance(
        TerritoryWithoutGeometry.from_dto(list_result[0]), TerritoryWithoutGeometry
    ), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))
    mock_conn.execute_mock.assert_any_call(str(recursive_statement))
    mock_conn.execute_mock.assert_any_call(str(statement_with_filters))


@pytest.mark.asyncio
async def test_get_common_territory_for_geometry(mock_conn: MockConnection, shapely_geometry: Geom):
    """Test the get_common_territory_for_geometry function."""

    # Arrange
    statement = (
        select(territories_data.c.territory_id)
        .where(func.ST_Covers(territories_data.c.geometry, ST_GeomFromText(shapely_geometry.wkt, text("4326"))))
        .order_by(territories_data.c.level.desc())
        .limit(1)
    )

    # Act
    result = await get_common_territory_for_geometry(mock_conn, shapely_geometry)

    # Assert
    assert isinstance(result, TerritoryDTO), "Result should be a TerritoryDTO."
    assert isinstance(Territory.from_dto(result), Territory), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))


@pytest.mark.asyncio
async def test_get_intersecting_territories_for_geometry(mock_conn: MockConnection, shapely_geometry: Geom):
    """Test the get_intersecting_territories_for_geometry function."""

    # Arrange
    parent_territory = 1
    level_subqery = (
        select(territories_data.c.level + 1)
        .where(territories_data.c.territory_id == parent_territory)
        .scalar_subquery()
    )
    given_geometry = select(ST_GeomFromText(shapely_geometry.wkt, text("4326"))).cte("given_geometry")
    statement = select(territories_data.c.territory_id).where(
        territories_data.c.level == level_subqery,
        (
            func.ST_Intersects(territories_data.c.geometry, select(given_geometry).scalar_subquery())
            | func.ST_Covers(select(given_geometry).scalar_subquery(), territories_data.c.geometry)
            | func.ST_Covers(territories_data.c.geometry, select(given_geometry).scalar_subquery())
        ),
    )

    # Act
    with patch("idu_api.urban_api.logic.impl.helpers.territories_objects.check_existence") as mock_check_existence:
        mock_check_existence.return_value = False
        with pytest.raises(EntityNotFoundById):
            await get_intersecting_territories_for_geometry(mock_conn, parent_territory, shapely_geometry)
    result = await get_intersecting_territories_for_geometry(mock_conn, parent_territory, shapely_geometry)

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(isinstance(item, TerritoryDTO) for item in result), "Each item should be a TerritoryDTO."
    assert isinstance(Territory.from_dto(result[0]), Territory), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))
