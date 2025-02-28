# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""create indexes

Revision ID: 4b00fa33ec4a
Revises: 0536532d2ca4
Create Date: 2025-02-17 14:55:53.343171

"""

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "4b00fa33ec4a"
down_revision: Union[str, None] = "0536532d2ca4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create indexes on all tables"""

    # functional zones
    for schema in ("public", "user_projects"):
        op.create_index(
            "functional_zones_data_geometry_idx",
            "functional_zones_data",
            ["geometry"],
            postgresql_using="gist",
            schema=schema,
        )
    op.create_index(
        "functional_zones_data_territory_year_source_idx",
        "functional_zones_data",
        ["territory_id", "year", "source"],
    )
    op.create_index(
        "functional_zones_data_scenario_year_source_idx",
        "functional_zones_data",
        ["scenario_id", "year", "source"],
        schema="user_projects",
    )

    # living buildings
    for schema in ("public", "user_projects"):
        op.create_index(
            "buildings_data_physical_object_id_idx",
            "buildings_data",
            ["physical_object_id"],
            schema=schema,
            if_not_exists=True,
        )

    # object geometries
    for schema in ("public", "user_projects"):
        op.create_index(
            "object_geometries_data_territory_id_idx",
            "object_geometries_data",
            ["territory_id"],
            schema=schema,
        )
        op.create_index(
            "object_geometries_data_geometry_idx",
            "object_geometries_data",
            ["geometry"],
            postgresql_using="gist",
            schema=schema,
        )

    # physical objects
    for schema in ("public", "user_projects"):
        op.create_index(
            "pod_physical_object_type_id_idx",
            "physical_objects_data",
            ["physical_object_type_id"],
            schema=schema,
        )

    # services
    for schema in ("public", "user_projects"):
        op.create_index("services_data_service_type_id_idx", "services_data", ["service_type_id"], schema=schema)

    # territories
    for column in ("parent_id", "admin_center_id"):
        op.create_index(
            f"territories_data_{column}_idx",
            "territories_data",
            [column],
        )
    op.create_index(
        "territories_data_geometry_idx",
        "territories_data",
        ["geometry"],
        postgresql_using="gist",
    )

    # indicator values
    for column in ("territory_id", "indicator_id"):
        op.create_index(
            f"territory_indicators_data_{column}_idx",
            "territory_indicators_data",
            [column],
        )
    op.create_index(
        "tid_territory_id_indicator_id_value_type_idx",
        "territory_indicators_data",
        ["territory_id", "indicator_id", "value_type"],
    )
    for column in ("scenario_id", "indicator_id"):
        op.create_index(
            f"indicators_data_{column}_idx",
            "indicators_data",
            [column],
            schema="user_projects",
        )

    # urban objects
    for column in ("physical_object_id", "object_geometry_id", "service_id"):
        for schema in ("public", "user_projects"):
            op.create_index(
                f"urban_objects_data_{column}_idx",
                "urban_objects_data",
                [column],
                schema=schema,
                if_not_exists=True,
            )
    for column in ("public_physical_object_id", "public_object_geometry_id", "public_service_id"):
        op.create_index(
            f"urban_objects_data_{column}_idx",
            "urban_objects_data",
            [column],
            schema="user_projects",
        )
    op.create_index(
        "urban_objects_data_scenario_public_idx",
        "urban_objects_data",
        ["scenario_id", "public_urban_object_id"],
        schema="user_projects",
    )

    # hexagons
    op.create_index(
        "hexagons_data_territory_id_idx",
        "hexagons_data",
        ["territory_id"],
        schema="user_projects",
    )

    # projects
    op.create_index(
        "projects_data_territory_id_idx",
        "projects_data",
        ["territory_id"],
        schema="user_projects",
    )
    op.create_index(
        "projects_territory_data_project_id_idx",
        "projects_territory_data",
        ["project_id"],
        schema="user_projects",
    )
    op.create_index(
        "projects_territory_data_geometry_idx",
        "projects_territory_data",
        ["geometry"],
        postgresql_using="gist",
        schema="user_projects",
    )

    # scenarios
    op.create_index(
        "scenarios_data_project_id_idx",
        "scenarios_data",
        ["project_id"],
        schema="user_projects",
    )


def downgrade() -> None:
    """Drop all indexes."""

    # functional zones
    op.drop_index("functional_zones_data_territory_year_source_idx", "functional_zones_data")
    op.drop_index("functional_zones_data_scenario_year_source_idx", "functional_zones_data", schema="user_projects")
    for schema in ("public", "user_projects"):
        op.drop_index("functional_zones_data_geometry_idx", "functional_zones_data", schema=schema)

    # buildings
    for schema in ("public", "user_projects"):
        op.drop_index("buildings_data_physical_object_id_idx", "buildings_data", schema=schema)

    # object geometries
    for schema in ("public", "user_projects"):
        op.drop_index("object_geometries_data_territory_id_idx", "object_geometries_data", schema=schema)
        op.drop_index(
            "object_geometries_data_geometry_idx",
            "object_geometries_data",
            postgresql_using="gist",
            schema=schema,
        )

    # physical objects
    for schema in ("public", "user_projects"):
        op.drop_index("pod_physical_object_type_id_idx", "physical_objects_data", schema=schema)

    # services
    for schema in ("public", "user_projects"):
        op.drop_index("services_data_service_type_id_idx", "services_data", schema=schema)

    # territories
    for column in ("parent_id", "admin_center_id", "geometry"):
        op.drop_index(f"territories_data_{column}_idx", "territories_data")

    # indicator values
    for index in (
        "territory_indicators_data_territory_id_idx",
        "territory_indicators_data_indicator_id_idx",
        "tid_territory_id_indicator_id_value_type_idx",
    ):
        op.drop_index(index, "territory_indicators_data")
    for column in ("scenario_id", "indicator_id"):
        op.drop_index(f"indicators_data_{column}_idx", "indicators_data", schema="user_projects")

    # urban objects
    for column in ("physical_object_id", "object_geometry_id", "service_id"):
        for schema in ("public", "user_projects"):
            op.drop_index(f"urban_objects_data_{column}_idx", "urban_objects_data", schema=schema)
    for column in ("public_physical_object_id", "public_object_geometry_id", "public_service_id"):
        op.drop_index(f"urban_objects_data_{column}_idx", "urban_objects_data", schema="user_projects")
    op.drop_index("urban_objects_data_scenario_public_idx", "urban_objects_data", schema="user_projects")

    # hexagons
    op.drop_index("hexagons_data_territory_id_idx", "hexagons_data", schema="user_projects")

    # projects
    op.drop_index("projects_data_territory_id_idx", "projects_data", schema="user_projects")
    op.drop_index("projects_territory_data_project_id_idx", "projects_territory_data", schema="user_projects")
    op.drop_index("projects_territory_data_geometry_idx", "projects_territory_data", schema="user_projects")

    # scenarios
    op.drop_index("scenarios_data_project_id_idx", "scenarios_data", schema="user_projects")
