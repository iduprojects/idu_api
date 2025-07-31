"""Unit tests for scenario buffer objects are defined here."""

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from geoalchemy2.functions import ST_AsEWKB, ST_Intersection, ST_Intersects
from sqlalchemy import ScalarSelect, delete, or_, select, union_all
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.sql.elements import literal
from sqlalchemy.sql.functions import coalesce

from idu_api.common.db.entities import (
    buffer_types_dict,
    buffers_data,
    object_geometries_data,
    physical_object_types_dict,
    physical_objects_data,
    projects_buffers_data,
    projects_object_geometries_data,
    projects_physical_objects_data,
    projects_services_data,
    projects_territory_data,
    projects_urban_objects_data,
    service_types_dict,
    services_data,
    territories_data,
    urban_objects_data,
)
from idu_api.urban_api.dto import (
    ScenarioBufferDTO,
    UserDTO,
)
from idu_api.urban_api.exceptions.logic.common import (
    EntityNotFoundById,
    EntityNotFoundByParams,
)
from idu_api.urban_api.exceptions.logic.projects import NotAllowedInRegionalScenario
from idu_api.urban_api.logic.impl.helpers.projects_buffers import (
    delete_buffer_from_db,
    get_buffer_from_db,
    get_buffers_by_scenario_id_from_db,
    get_context_buffers_from_db,
    put_buffer_to_db,
)
from idu_api.urban_api.logic.impl.helpers.utils import extract_values_from_model
from idu_api.urban_api.schemas import (
    ScenarioBuffer,
    ScenarioBufferDelete,
    ScenarioBufferPut,
)
from tests.urban_api.helpers.connection import MockConnection

####################################################################################
#                           Default use-case tests                                 #
####################################################################################


@pytest.mark.asyncio
async def test_get_buffers_by_scenario_id_from_db(mock_conn: MockConnection):
    """Test the get_buffers_by_scenario_id_from_db function."""

    # Arrange
    scenario_id = 1
    buffer_type_id = 1
    physical_object_type_id, service_type_id = 1, 1
    user = UserDTO(id="mock_string", is_superuser=False)

    project_geometry = (
        select(projects_territory_data.c.geometry).where(projects_territory_data.c.project_id == 1)
    ).scalar_subquery()
    public_urban_object_ids = (
        select(projects_urban_objects_data.c.public_urban_object_id)
        .where(projects_urban_objects_data.c.scenario_id == scenario_id)
        .where(projects_urban_objects_data.c.public_urban_object_id.isnot(None))
    ).cte(name="public_urban_object_ids")
    public_buffers_query = (
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
            ST_AsEWKB(ST_Intersection(buffers_data.c.geometry, project_geometry)).label("geometry"),
            buffers_data.c.is_custom,
            literal(False).label("is_scenario_object"),
            (~ST_Intersects(object_geometries_data.c.geometry, project_geometry)).label("is_locked"),
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
        .where(
            urban_objects_data.c.urban_object_id.not_in(select(public_urban_object_ids)),
            ST_Intersects(buffers_data.c.geometry, project_geometry),
            buffer_types_dict.c.buffer_type_id == buffer_type_id,
            physical_object_types_dict.c.physical_object_type_id == physical_object_type_id,
            service_types_dict.c.service_type_id == service_type_id,
        )
    )
    locked_regional_scenario_buffers_query = (
        select(
            buffer_types_dict.c.buffer_type_id,
            buffer_types_dict.c.name.label("buffer_type_name"),
            projects_urban_objects_data.c.urban_object_id,
            coalesce(
                projects_physical_objects_data.c.physical_object_id, physical_objects_data.c.physical_object_id
            ).label("physical_object_id"),
            coalesce(projects_physical_objects_data.c.name, physical_objects_data.c.name).label("physical_object_name"),
            physical_object_types_dict.c.physical_object_type_id,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            coalesce(
                projects_object_geometries_data.c.object_geometry_id, object_geometries_data.c.object_geometry_id
            ).label("object_geometry_id"),
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            coalesce(projects_services_data.c.service_id, services_data.c.service_id).label("service_id"),
            coalesce(projects_services_data.c.name, services_data.c.name).label("service_name"),
            service_types_dict.c.service_type_id,
            service_types_dict.c.name.label("service_type_name"),
            ST_AsEWKB(ST_Intersection(projects_buffers_data.c.geometry, project_geometry)).label("geometry"),
            projects_buffers_data.c.is_custom,
            literal(True).label("is_scenario_object"),
            literal(True).label("is_locked"),
        )
        .select_from(
            projects_buffers_data.join(
                buffer_types_dict,
                buffer_types_dict.c.buffer_type_id == projects_buffers_data.c.buffer_type_id,
            )
            .outerjoin(
                projects_urban_objects_data,
                projects_urban_objects_data.c.urban_object_id == projects_buffers_data.c.urban_object_id,
            )
            .outerjoin(
                projects_physical_objects_data,
                projects_physical_objects_data.c.physical_object_id == projects_urban_objects_data.c.physical_object_id,
            )
            .outerjoin(
                projects_object_geometries_data,
                projects_object_geometries_data.c.object_geometry_id
                == projects_urban_objects_data.c.object_geometry_id,
            )
            .outerjoin(
                projects_services_data, projects_services_data.c.service_id == projects_urban_objects_data.c.service_id
            )
            .outerjoin(
                physical_objects_data,
                physical_objects_data.c.physical_object_id == projects_urban_objects_data.c.public_physical_object_id,
            )
            .outerjoin(
                physical_object_types_dict,
                or_(
                    physical_object_types_dict.c.physical_object_type_id
                    == projects_physical_objects_data.c.physical_object_type_id,
                    physical_object_types_dict.c.physical_object_type_id
                    == physical_objects_data.c.physical_object_type_id,
                ),
            )
            .outerjoin(
                object_geometries_data,
                object_geometries_data.c.object_geometry_id == projects_urban_objects_data.c.public_object_geometry_id,
            )
            .outerjoin(
                territories_data,
                or_(
                    territories_data.c.territory_id == projects_object_geometries_data.c.territory_id,
                    territories_data.c.territory_id == object_geometries_data.c.territory_id,
                ),
            )
            .outerjoin(services_data, services_data.c.service_id == projects_urban_objects_data.c.public_service_id)
            .outerjoin(
                service_types_dict,
                or_(
                    service_types_dict.c.service_type_id == projects_services_data.c.service_type_id,
                    service_types_dict.c.service_type_id == services_data.c.service_type_id,
                ),
            )
        )
        .where(
            projects_urban_objects_data.c.scenario_id == 1,
            ST_Intersects(projects_buffers_data.c.geometry, project_geometry),
            ~ST_Intersects(object_geometries_data.c.geometry, project_geometry),
            ~ST_Intersects(projects_object_geometries_data.c.geometry, project_geometry),
            buffer_types_dict.c.buffer_type_id == buffer_type_id,
            physical_object_types_dict.c.physical_object_type_id == physical_object_type_id,
            service_types_dict.c.service_type_id == service_type_id,
        )
    )
    scenario_buffers_query = (
        select(
            buffer_types_dict.c.buffer_type_id,
            buffer_types_dict.c.name.label("buffer_type_name"),
            projects_urban_objects_data.c.urban_object_id,
            coalesce(
                projects_physical_objects_data.c.physical_object_id, physical_objects_data.c.physical_object_id
            ).label("physical_object_id"),
            coalesce(projects_physical_objects_data.c.name, physical_objects_data.c.name).label("physical_object_name"),
            physical_object_types_dict.c.physical_object_type_id,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            coalesce(
                projects_object_geometries_data.c.object_geometry_id, object_geometries_data.c.object_geometry_id
            ).label("object_geometry_id"),
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            coalesce(projects_services_data.c.service_id, services_data.c.service_id).label("service_id"),
            coalesce(projects_services_data.c.name, services_data.c.name).label("service_name"),
            service_types_dict.c.service_type_id,
            service_types_dict.c.name.label("service_type_name"),
            ST_AsEWKB(projects_buffers_data.c.geometry).label("geometry"),
            projects_buffers_data.c.is_custom,
            literal(True).label("is_scenario_object"),
            literal(False).label("is_locked"),
        )
        .select_from(
            projects_buffers_data.join(
                buffer_types_dict,
                buffer_types_dict.c.buffer_type_id == projects_buffers_data.c.buffer_type_id,
            )
            .outerjoin(
                projects_urban_objects_data,
                projects_urban_objects_data.c.urban_object_id == projects_buffers_data.c.urban_object_id,
            )
            .outerjoin(
                projects_physical_objects_data,
                projects_physical_objects_data.c.physical_object_id == projects_urban_objects_data.c.physical_object_id,
            )
            .outerjoin(
                projects_object_geometries_data,
                projects_object_geometries_data.c.object_geometry_id
                == projects_urban_objects_data.c.object_geometry_id,
            )
            .outerjoin(
                projects_services_data, projects_services_data.c.service_id == projects_urban_objects_data.c.service_id
            )
            .outerjoin(
                physical_objects_data,
                physical_objects_data.c.physical_object_id == projects_urban_objects_data.c.public_physical_object_id,
            )
            .outerjoin(
                physical_object_types_dict,
                or_(
                    physical_object_types_dict.c.physical_object_type_id
                    == projects_physical_objects_data.c.physical_object_type_id,
                    physical_object_types_dict.c.physical_object_type_id
                    == physical_objects_data.c.physical_object_type_id,
                ),
            )
            .outerjoin(
                object_geometries_data,
                object_geometries_data.c.object_geometry_id == projects_urban_objects_data.c.public_object_geometry_id,
            )
            .outerjoin(
                territories_data,
                or_(
                    territories_data.c.territory_id == projects_object_geometries_data.c.territory_id,
                    territories_data.c.territory_id == object_geometries_data.c.territory_id,
                ),
            )
            .outerjoin(services_data, services_data.c.service_id == projects_urban_objects_data.c.public_service_id)
            .outerjoin(
                service_types_dict,
                or_(
                    service_types_dict.c.service_type_id == projects_services_data.c.service_type_id,
                    service_types_dict.c.service_type_id == services_data.c.service_type_id,
                ),
            )
        )
        .where(
            projects_urban_objects_data.c.scenario_id == scenario_id,
            buffer_types_dict.c.buffer_type_id == buffer_type_id,
            physical_object_types_dict.c.physical_object_type_id == physical_object_type_id,
            service_types_dict.c.service_type_id == service_type_id,
        )
    )

    union_query = union_all(
        public_buffers_query,
        locked_regional_scenario_buffers_query,
        scenario_buffers_query,
    )

    # Act
    with pytest.raises(NotAllowedInRegionalScenario):
        await get_buffers_by_scenario_id_from_db(
            mock_conn, scenario_id, buffer_type_id, physical_object_type_id, service_type_id, user
        )
    with patch("idu_api.urban_api.logic.impl.helpers.projects_buffers.check_scenario") as mock_check:
        result = await get_buffers_by_scenario_id_from_db(
            mock_conn, scenario_id, buffer_type_id, physical_object_type_id, service_type_id, user
        )

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(isinstance(item, ScenarioBufferDTO) for item in result), "Each item should be a ScenarioBufferDTO."
    assert isinstance(ScenarioBuffer.from_dto(result[0]), ScenarioBuffer), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(union_query))
    mock_check.assert_called_once_with(mock_conn, scenario_id, user, allow_regional=False, return_value=True)


@pytest.mark.asyncio
async def test_get_context_buffers_from_db(mock_conn: MockConnection):
    """Test the get_context_buffers_from_db function."""

    # Arrange
    project_id = 1
    year = datetime.today().year
    source = "mock_string"
    buffer_type_id = 1
    physical_object_type_id, service_type_id = 1, 1
    mock_geom = str(MagicMock(spec=ScalarSelect))
    user = UserDTO(id="mock_string", is_superuser=False)

    public_urban_object_ids = (
        select(projects_urban_objects_data.c.public_urban_object_id)
        .where(projects_urban_objects_data.c.scenario_id == 1)
        .where(projects_urban_objects_data.c.public_urban_object_id.isnot(None))
    ).cte(name="public_urban_object_ids")
    objects_intersecting = (
        select(object_geometries_data.c.object_geometry_id)
        .select_from(
            object_geometries_data.join(
                urban_objects_data,
                urban_objects_data.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
            )
        )
        .where(
            urban_objects_data.c.urban_object_id.not_in(select(public_urban_object_ids)),
            object_geometries_data.c.territory_id.in_([1])
            | ST_Intersects(object_geometries_data.c.geometry, mock_geom),
        )
        .cte(name="objects_intersecting")
    )
    public_buffers_query = (
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
            ST_AsEWKB(ST_Intersection(buffers_data.c.geometry, mock_geom)).label("geometry"),
            buffers_data.c.is_custom,
            literal(False).label("is_scenario_object"),
            literal(True).label("is_locked"),
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
            .join(
                objects_intersecting,
                objects_intersecting.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
            )
            .join(territories_data, territories_data.c.territory_id == object_geometries_data.c.territory_id)
            .outerjoin(services_data, services_data.c.service_id == urban_objects_data.c.service_id)
            .outerjoin(service_types_dict, service_types_dict.c.service_type_id == services_data.c.service_type_id)
        )
        .where(
            buffer_types_dict.c.buffer_type_id == buffer_type_id,
            physical_object_types_dict.c.physical_object_type_id == physical_object_type_id,
            service_types_dict.c.service_type_id == service_type_id,
        )
    )

    regional_scenario_buffers_query = (
        select(
            buffer_types_dict.c.buffer_type_id,
            buffer_types_dict.c.name.label("buffer_type_name"),
            projects_urban_objects_data.c.urban_object_id,
            coalesce(
                projects_physical_objects_data.c.physical_object_id, physical_objects_data.c.physical_object_id
            ).label("physical_object_id"),
            coalesce(projects_physical_objects_data.c.name, physical_objects_data.c.name).label("physical_object_name"),
            physical_object_types_dict.c.physical_object_type_id,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            coalesce(
                projects_object_geometries_data.c.object_geometry_id, object_geometries_data.c.object_geometry_id
            ).label("object_geometry_id"),
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            coalesce(projects_services_data.c.service_id, services_data.c.service_id).label("service_id"),
            coalesce(projects_services_data.c.name, services_data.c.name).label("service_name"),
            service_types_dict.c.service_type_id,
            service_types_dict.c.name.label("service_type_name"),
            ST_AsEWKB(ST_Intersection(projects_buffers_data.c.geometry, mock_geom)).label("geometry"),
            projects_buffers_data.c.is_custom,
            literal(True).label("is_scenario_object"),
            literal(True).label("is_locked"),
        )
        .select_from(
            projects_buffers_data.join(
                buffer_types_dict,
                buffer_types_dict.c.buffer_type_id == projects_buffers_data.c.buffer_type_id,
            )
            .outerjoin(
                projects_urban_objects_data,
                projects_urban_objects_data.c.urban_object_id == projects_buffers_data.c.urban_object_id,
            )
            .outerjoin(
                projects_physical_objects_data,
                projects_physical_objects_data.c.physical_object_id == projects_urban_objects_data.c.physical_object_id,
            )
            .outerjoin(
                projects_object_geometries_data,
                projects_object_geometries_data.c.object_geometry_id
                == projects_urban_objects_data.c.object_geometry_id,
            )
            .outerjoin(
                projects_services_data, projects_services_data.c.service_id == projects_urban_objects_data.c.service_id
            )
            .outerjoin(
                physical_objects_data,
                physical_objects_data.c.physical_object_id == projects_urban_objects_data.c.public_physical_object_id,
            )
            .outerjoin(
                physical_object_types_dict,
                or_(
                    physical_object_types_dict.c.physical_object_type_id
                    == projects_physical_objects_data.c.physical_object_type_id,
                    physical_object_types_dict.c.physical_object_type_id
                    == physical_objects_data.c.physical_object_type_id,
                ),
            )
            .outerjoin(
                object_geometries_data,
                object_geometries_data.c.object_geometry_id == projects_urban_objects_data.c.public_object_geometry_id,
            )
            .outerjoin(
                territories_data,
                or_(
                    territories_data.c.territory_id == projects_object_geometries_data.c.territory_id,
                    territories_data.c.territory_id == object_geometries_data.c.territory_id,
                ),
            )
            .outerjoin(services_data, services_data.c.service_id == projects_urban_objects_data.c.public_service_id)
            .outerjoin(
                service_types_dict,
                or_(
                    service_types_dict.c.service_type_id == projects_services_data.c.service_type_id,
                    service_types_dict.c.service_type_id == services_data.c.service_type_id,
                ),
            )
        )
        .where(
            projects_urban_objects_data.c.scenario_id == 1,
            ST_Intersects(projects_buffers_data.c.geometry, mock_geom),
            buffer_types_dict.c.buffer_type_id == buffer_type_id,
            physical_object_types_dict.c.physical_object_type_id == physical_object_type_id,
            service_types_dict.c.service_type_id == service_type_id,
        )
    )
    union_query = union_all(public_buffers_query, regional_scenario_buffers_query)

    # Act
    with pytest.raises(NotAllowedInRegionalScenario):
        await get_context_buffers_from_db(mock_conn, project_id, year, source, buffer_type_id, user)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.projects_buffers.get_context_territories_geometry",
        new_callable=AsyncMock,
    ) as mock_get_context:
        mock_get_context.return_value = 1, mock_geom, [1]
        result = await get_context_buffers_from_db(mock_conn, project_id, year, source, buffer_type_id, user)

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(isinstance(item, ScenarioBufferDTO) for item in result), "Each item should be a BufferDTO."
    assert isinstance(ScenarioBuffer.from_dto(result[0]), ScenarioBuffer), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(union_query))


@pytest.mark.asyncio
async def test_get_buffer_from_db(mock_conn: MockConnection):
    """Test the get_buffer_from_db function."""

    # Arrange
    buffer_type_id, urban_object_id = 1, 1
    statement = (
        select(
            buffer_types_dict.c.buffer_type_id,
            buffer_types_dict.c.name.label("buffer_type_name"),
            projects_urban_objects_data.c.urban_object_id,
            coalesce(
                projects_physical_objects_data.c.physical_object_id, physical_objects_data.c.physical_object_id
            ).label("physical_object_id"),
            coalesce(projects_physical_objects_data.c.name, physical_objects_data.c.name).label("physical_object_name"),
            physical_object_types_dict.c.physical_object_type_id,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            coalesce(
                projects_object_geometries_data.c.object_geometry_id, object_geometries_data.c.object_geometry_id
            ).label("object_geometry_id"),
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            coalesce(projects_services_data.c.service_id, services_data.c.service_id).label("service_id"),
            coalesce(projects_services_data.c.name, services_data.c.name).label("service_name"),
            service_types_dict.c.service_type_id,
            service_types_dict.c.name.label("service_type_name"),
            ST_AsEWKB(projects_buffers_data.c.geometry).label("geometry"),
            projects_buffers_data.c.is_custom,
            literal(True).label("is_scenario_object"),
            literal(False).label("is_locked"),
        )
        .select_from(
            projects_buffers_data.join(
                buffer_types_dict,
                buffer_types_dict.c.buffer_type_id == projects_buffers_data.c.buffer_type_id,
            )
            .outerjoin(
                projects_urban_objects_data,
                projects_urban_objects_data.c.urban_object_id == projects_buffers_data.c.urban_object_id,
            )
            .outerjoin(
                projects_physical_objects_data,
                projects_physical_objects_data.c.physical_object_id == projects_urban_objects_data.c.physical_object_id,
            )
            .outerjoin(
                projects_object_geometries_data,
                projects_object_geometries_data.c.object_geometry_id
                == projects_urban_objects_data.c.object_geometry_id,
            )
            .outerjoin(
                projects_services_data, projects_services_data.c.service_id == projects_urban_objects_data.c.service_id
            )
            .outerjoin(
                physical_objects_data,
                physical_objects_data.c.physical_object_id == projects_urban_objects_data.c.public_physical_object_id,
            )
            .outerjoin(
                physical_object_types_dict,
                or_(
                    physical_object_types_dict.c.physical_object_type_id
                    == projects_physical_objects_data.c.physical_object_type_id,
                    physical_object_types_dict.c.physical_object_type_id
                    == physical_objects_data.c.physical_object_type_id,
                ),
            )
            .outerjoin(
                object_geometries_data,
                object_geometries_data.c.object_geometry_id == projects_urban_objects_data.c.public_object_geometry_id,
            )
            .outerjoin(
                territories_data,
                or_(
                    territories_data.c.territory_id == projects_object_geometries_data.c.territory_id,
                    territories_data.c.territory_id == object_geometries_data.c.territory_id,
                ),
            )
            .outerjoin(services_data, services_data.c.service_id == projects_urban_objects_data.c.public_service_id)
            .outerjoin(
                service_types_dict,
                or_(
                    service_types_dict.c.service_type_id == projects_services_data.c.service_type_id,
                    service_types_dict.c.service_type_id == services_data.c.service_type_id,
                ),
            )
        )
        .where(
            projects_buffers_data.c.buffer_type_id == buffer_type_id,
            projects_buffers_data.c.urban_object_id == urban_object_id,
        )
    )

    # Act
    result = await get_buffer_from_db(mock_conn, buffer_type_id, urban_object_id)

    # Assert
    assert isinstance(result, ScenarioBufferDTO), "Result should be a ScenarioBufferDTO."
    assert isinstance(ScenarioBuffer.from_dto(result), ScenarioBuffer), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))


@pytest.mark.asyncio
async def test_put_scenario_buffer_to_db(mock_conn: MockConnection, scenario_buffer_put_req: ScenarioBufferPut):
    """Test the put_buffer_to_db function."""

    # Arrange
    async def check_buffer_type(conn, table, conditions=None):
        if table == buffer_types_dict:
            return False
        return True

    async def check_buffer(conn, table, conditions=None):
        if table == projects_buffers_data:
            return False
        return True

    scenario_id = 1
    user = UserDTO(id="mock_string", is_superuser=False)

    check_urban_object = (
        select(projects_urban_objects_data.c.urban_object_id)
        .where(
            projects_urban_objects_data.c.physical_object_id == scenario_buffer_put_req.physical_object_id,
            projects_urban_objects_data.c.object_geometry_id == scenario_buffer_put_req.object_geometry_id,
            projects_urban_objects_data.c.service_id == scenario_buffer_put_req.service_id,
            projects_urban_objects_data.c.scenario_id == scenario_id,
        )
        .limit(1)
    )
    values = extract_values_from_model(scenario_buffer_put_req, exclude_unset=True, allow_null_geometry=True)
    statement = (
        insert(projects_buffers_data)
        .values(
            buffer_type_id=values["buffer_type_id"],
            urban_object_id=1,
            geometry=values["geometry"],
            is_custom=True,
        )
        .on_conflict_do_update(
            index_elements=["urban_object_id", "buffer_type_id"],
            set_={
                "geometry": values["geometry"],
                "is_custom": True,
            },
        )
    )

    # Act
    with pytest.raises(NotAllowedInRegionalScenario):
        await put_buffer_to_db(mock_conn, scenario_buffer_put_req, scenario_id, user)
    with patch("idu_api.urban_api.logic.impl.helpers.projects_buffers.check_scenario") as mock_check:
        await put_buffer_to_db(mock_conn, scenario_buffer_put_req, scenario_id, user)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.projects_buffers.check_existence",
        new=AsyncMock(side_effect=check_buffer_type),
    ):
        with patch("idu_api.urban_api.logic.impl.helpers.projects_buffers.check_scenario") as mock_check:
            with pytest.raises(EntityNotFoundById):
                await put_buffer_to_db(mock_conn, scenario_buffer_put_req, scenario_id, user)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.projects_buffers.check_existence",
        new=AsyncMock(side_effect=check_buffer),
    ):
        with patch("idu_api.urban_api.logic.impl.helpers.projects_buffers.check_scenario") as mock_check:
            result = await put_buffer_to_db(mock_conn, scenario_buffer_put_req, scenario_id, user)

    # Assert
    assert isinstance(result, ScenarioBufferDTO), "Result should be a ScenarioBufferDTO."
    assert isinstance(ScenarioBuffer.from_dto(result), ScenarioBuffer), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(check_urban_object))
    mock_conn.execute_mock.assert_any_call(str(statement))
    mock_check.assert_any_call(mock_conn, scenario_id, user, to_edit=True, allow_regional=False)
    assert mock_conn.commit_mock.call_count == 2, "Commit mock count should be one for one method."


@pytest.mark.asyncio
async def test_delete_buffer_from_db(mock_conn: MockConnection, scenario_buffer_delete_req: ScenarioBufferDelete):
    """Test the delete_buffer_from_db function."""

    # Arrange
    async def check_buffer_type(conn, table, conditions=None):
        if table == buffer_types_dict:
            return False
        return True

    async def check_buffer(conn, table, conditions=None):
        if table == projects_buffers_data:
            return False
        return True

    scenario_id = 1
    user = UserDTO(id="mock_string", is_superuser=False)

    check_urban_object = (
        select(projects_urban_objects_data.c.urban_object_id)
        .where(
            projects_urban_objects_data.c.physical_object_id == scenario_buffer_delete_req.physical_object_id,
            projects_urban_objects_data.c.object_geometry_id == scenario_buffer_delete_req.object_geometry_id,
            projects_urban_objects_data.c.service_id == scenario_buffer_delete_req.service_id,
            projects_urban_objects_data.c.scenario_id == scenario_id,
        )
        .limit(1)
    )
    statement = delete(projects_buffers_data).where(
        projects_buffers_data.c.buffer_type_id == scenario_buffer_delete_req.buffer_type_id,
        projects_buffers_data.c.urban_object_id == 1,
    )

    # Act
    with patch(
        "idu_api.urban_api.logic.impl.helpers.projects_buffers.check_existence",
        new=AsyncMock(side_effect=check_buffer_type),
    ):
        with patch("idu_api.urban_api.logic.impl.helpers.projects_buffers.check_scenario") as mock_check:
            with pytest.raises(EntityNotFoundById):
                await delete_buffer_from_db(mock_conn, scenario_buffer_delete_req, scenario_id, user)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.projects_buffers.check_existence",
        new=AsyncMock(side_effect=check_buffer),
    ):
        with patch("idu_api.urban_api.logic.impl.helpers.projects_buffers.check_scenario") as mock_check:
            with pytest.raises(EntityNotFoundByParams):
                await delete_buffer_from_db(mock_conn, scenario_buffer_delete_req, scenario_id, user)
    with patch("idu_api.urban_api.logic.impl.helpers.projects_buffers.check_scenario") as mock_check:
        result = await delete_buffer_from_db(mock_conn, scenario_buffer_delete_req, scenario_id, user)

    # Assert
    assert result == {"status": "ok"}, "Result should be {'status': 'ok'}."
    mock_conn.execute_mock.assert_any_call(str(check_urban_object))
    mock_conn.execute_mock.assert_any_call(str(statement))
    mock_conn.commit_mock.assert_called_once()
