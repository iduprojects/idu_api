# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""add user_projects schema

Revision ID: 062b5355e31d
Revises: 7c1977523140
Create Date: 2024-08-27 18:04:17.184195

"""
from typing import Sequence, Union

import geoalchemy2
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "062b5355e31d"
down_revision: Union[str, None] = "7c1977523140"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("create schema if not exists user_projects")
    op.execute("create extension if not exists postgis")
    op.execute(sa.schema.CreateSequence(sa.Sequence("project_territory_id_seq", schema="user_projects")))
    op.execute(sa.schema.CreateSequence(sa.Sequence("physical_objects_id_seq", schema="user_projects")))
    op.execute(sa.schema.CreateSequence(sa.Sequence("project_id_seq", schema="user_projects")))
    op.execute(sa.schema.CreateSequence(sa.Sequence("living_buildings_id_seq", schema="user_projects")))
    op.execute(sa.schema.CreateSequence(sa.Sequence("object_geometries_id_seq", schema="user_projects")))
    op.execute(sa.schema.CreateSequence(sa.Sequence("scenarios_data_id_seq", schema="user_projects")))
    op.execute(sa.schema.CreateSequence(sa.Sequence("services_id_seq", schema="user_projects")))
    op.execute(sa.schema.CreateSequence(sa.Sequence("urban_objects_id_seq", schema="user_projects")))
    op.create_table(
        "projects_territory_data",
        sa.Column(
            "project_territory_id",
            sa.Integer(),
            server_default=sa.text("nextval('user_projects.project_territory_id_seq')"),
            nullable=False,
        ),
        sa.Column("parent_territory_id", sa.Integer(), nullable=True),
        sa.Column(
            "geometry",
            geoalchemy2.types.Geometry(
                spatial_index=False, from_text="ST_GeomFromEWKT", name="geometry", nullable=False
            ),
            nullable=False,
        ),
        sa.Column(
            "centre_point",
            geoalchemy2.types.Geometry(
                geometry_type="POINT", spatial_index=False, from_text="ST_GeomFromEWKT", name="geometry", nullable=False
            ),
            nullable=False,
        ),
        sa.Column(
            "properties", postgresql.JSONB(astext_type=sa.Text()), server_default=sa.text("'{}'::jsonb"), nullable=False
        ),
        sa.ForeignKeyConstraint(
            ["parent_territory_id"],
            ["user_projects.projects_territory_data.project_territory_id"],
            name=op.f("projects_territory_data_fk_parent_territory_id__projects_territory_data"),
        ),
        sa.PrimaryKeyConstraint("project_territory_id", name=op.f("projects_territory_data_pk")),
        schema="user_projects",
    )
    op.create_table(
        "object_geometries_data",
        sa.Column(
            "object_geometry_id",
            sa.Integer(),
            server_default=sa.text("nextval('user_projects.object_geometries_id_seq')"),
            nullable=False,
        ),
        sa.Column("territory_id", sa.Integer(), nullable=False),
        sa.Column(
            "geometry",
            geoalchemy2.types.Geometry(
                spatial_index=False, from_text="ST_GeomFromEWKT", name="geometry", nullable=False
            ),
            nullable=False,
        ),
        sa.Column(
            "centre_point",
            geoalchemy2.types.Geometry(
                geometry_type="POINT", spatial_index=False, from_text="ST_GeomFromEWKT", name="geometry", nullable=False
            ),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["territory_id"],
            ["territories_data.territory_id"],
            name=op.f("object_geometries_data_fk_territory_id__territories_data"),
        ),
        sa.PrimaryKeyConstraint("object_geometry_id", name=op.f("object_geometries_data_pk")),
        schema="user_projects",
    )
    op.create_table(
        "services_data",
        sa.Column(
            "service_id",
            sa.Integer(),
            server_default=sa.text("nextval('user_projects.services_id_seq')"),
            nullable=False,
        ),
        sa.Column("service_type_id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column(
            "properties", postgresql.JSONB(astext_type=sa.Text()), server_default=sa.text("'{}'::jsonb"), nullable=False
        ),
        sa.Column("list_label", sa.String(length=20), nullable=False),
        sa.Column("capacity_real", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["service_type_id"],
            ["service_types_dict.service_type_id"],
            name=op.f("services_data_fk_service_type_id__service_types_dict"),
        ),
        sa.PrimaryKeyConstraint("service_id", name=op.f("services_data_pk")),
        schema="user_projects",
    )
    op.create_table(
        "living_buildings_data",
        sa.Column(
            "living_building_id",
            sa.Integer(),
            server_default=sa.text("nextval('user_projects.living_buildings_id_seq')"),
            nullable=False,
        ),
        sa.Column("physical_object_id", sa.Integer(), nullable=False),
        sa.Column("residental_number", sa.Integer(), nullable=False),
        sa.Column("living_area", sa.Integer(), nullable=False),
        sa.Column(
            "properties", postgresql.JSONB(astext_type=sa.Text()), server_default=sa.text("'{}'::jsonb"), nullable=False
        ),
        sa.ForeignKeyConstraint(
            ["physical_object_id"],
            ["physical_objects_data.physical_object_id"],
            name=op.f("living_buildings_data_fk_physical_object_id__physical_objects_data"),
        ),
        sa.PrimaryKeyConstraint("living_building_id", name=op.f("living_buildings_data_pk")),
        schema="user_projects",
    )
    op.create_table(
        "projects_data",
        sa.Column(
            "project_id",
            sa.Integer(),
            server_default=sa.text("nextval('user_projects.project_id_seq')"),
            nullable=False,
        ),
        sa.Column("user_id", sa.String(length=200), nullable=False),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column("project_territory_id", sa.Integer(), nullable=False),
        sa.Column("description", sa.String(length=600), nullable=True),
        sa.Column("public", sa.Boolean(), nullable=False),
        sa.Column("image_url", sa.String(length=200), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(
            ["project_territory_id"],
            ["user_projects.projects_territory_data.project_territory_id"],
            name=op.f("projects_data_fk_project_territory_id__projects_territory_data"),
        ),
        sa.PrimaryKeyConstraint("project_id", name=op.f("projects_data_pk")),
        schema="user_projects",
    )
    op.create_table(
        "scenarios_data",
        sa.Column(
            "scenario_id",
            sa.Integer(),
            server_default=sa.text("nextval('user_projects.scenarios_data_id_seq')"),
            nullable=False,
        ),
        sa.Column("project_id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column(
            "properties", postgresql.JSONB(astext_type=sa.Text()), server_default=sa.text("'{}'::jsonb"), nullable=False
        ),
        sa.ForeignKeyConstraint(
            ["project_id"],
            ["user_projects.projects_data.project_id"],
            name=op.f("scenarios_data_fk_project_id__projects_data"),
        ),
        sa.PrimaryKeyConstraint("scenario_id", name=op.f("scenarios_data_pk")),
        schema="user_projects",
    )
    op.create_table(
        "profiles_data",
        sa.Column("scenario_id", sa.Integer(), nullable=False),
        sa.Column("profile_type_id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column(
            "geometry",
            geoalchemy2.types.Geometry(
                spatial_index=False, from_text="ST_GeomFromEWKT", name="geometry", nullable=False
            ),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["scenario_id"],
            ["user_projects.scenarios_data.scenario_id"],
            name=op.f("profiles_data_fk_scenario_id__scenarios_data"),
        ),
        schema="user_projects",
    )
    op.create_table(
        "indicators_data",
        sa.Column("scenario_id", sa.Integer(), nullable=False),
        sa.Column("indicator_id", sa.Integer(), nullable=False),
        sa.Column(
            "date_type", sa.Enum("year", "half_year", "quarter", "month", "day", name="date_field_type"), nullable=False
        ),
        sa.Column("date_value", sa.Date(), nullable=False),
        sa.Column("value", sa.Float(precision=53), nullable=False),
        sa.ForeignKeyConstraint(
            ["indicator_id"],
            ["indicators_dict.indicator_id"],
            name=op.f("indicators_data_fk_indicator_id__indicators_dict"),
        ),
        sa.ForeignKeyConstraint(
            ["scenario_id"],
            ["user_projects.scenarios_data.scenario_id"],
            name=op.f("indicators_data_fk_scenario_id__scenarios_data"),
        ),
        schema="user_projects",
    )
    op.create_table(
        "functional_zones_data",
        sa.Column("scenario_id", sa.Integer(), nullable=False),
        sa.Column("functional_zone_type_id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column(
            "geometry",
            geoalchemy2.types.Geometry(
                spatial_index=False, from_text="ST_GeomFromEWKT", name="geometry", nullable=False
            ),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["functional_zone_type_id"],
            ["functional_zone_types_dict.functional_zone_type_id"],
            name=op.f("functional_zones_data_fk_functional_zone_type_id__functional_zone_types_dict"),
        ),
        sa.ForeignKeyConstraint(
            ["scenario_id"],
            ["user_projects.scenarios_data.scenario_id"],
            name=op.f("functional_zones_data_fk_scenario_id__scenarios_data"),
        ),
        schema="user_projects",
    )
    op.create_table(
        "physical_objects_data",
        sa.Column(
            "physical_object_id",
            sa.Integer(),
            server_default=sa.text("nextval('user_projects.physical_objects_id_seq')"),
            nullable=False,
        ),
        sa.Column("physical_object_type_id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column(
            "properties", postgresql.JSONB(astext_type=sa.Text()), server_default=sa.text("'{}'::jsonb"), nullable=False
        ),
        sa.Column("address", sa.String(length=300), nullable=True),
        sa.ForeignKeyConstraint(
            ["physical_object_type_id"],
            ["physical_object_types_dict.physical_object_type_id"],
            name=op.f("physical_objects_data_fk_physical_object_type_id__physical_object_types_dict"),
        ),
        sa.PrimaryKeyConstraint("physical_object_id", name=op.f("physical_objects_data_pk")),
        schema="user_projects",
    )
    op.create_table(
        "urban_objects_data",
        sa.Column(
            "urban_object_id",
            sa.Integer(),
            server_default=sa.text("nextval('user_projects.urban_objects_id_seq')"),
            nullable=False,
        ),
        sa.Column("object_geometry_id", sa.Integer(), nullable=False),
        sa.Column("physical_object_id", sa.Integer(), nullable=False),
        sa.Column("service_id", sa.Integer(), nullable=False),
        sa.Column("scenario_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["object_geometry_id"],
            ["user_projects.object_geometries_data.object_geometry_id"],
            name=op.f("urban_objects_data_fk_object_geometry_id__object_geometries_data"),
        ),
        sa.ForeignKeyConstraint(
            ["physical_object_id"],
            ["user_projects.physical_objects_data.physical_object_id"],
            name=op.f("urban_objects_data_fk_physical_object_id__physical_objects_data"),
        ),
        sa.ForeignKeyConstraint(
            ["scenario_id"],
            ["user_projects.scenarios_data.scenario_id"],
            name=op.f("urban_objects_data_fk_scenario_id__scenarios_data"),
        ),
        sa.ForeignKeyConstraint(
            ["service_id"],
            ["user_projects.services_data.service_id"],
            name=op.f("urban_objects_data_fk_service_id__services_data"),
        ),
        sa.PrimaryKeyConstraint("urban_object_id", name=op.f("urban_objects_data_pk")),
        schema="user_projects",
    )


def downgrade() -> None:
    op.drop_table("urban_objects_data", schema="user_projects")
    op.drop_table("physical_objects_data", schema="user_projects")
    op.drop_table("functional_zones_data", schema="user_projects")
    op.drop_table("indicators_data", schema="user_projects")
    op.drop_table("profiles_data", schema="user_projects")
    op.drop_table("scenarios_data", schema="user_projects")
    op.drop_table("projects_data", schema="user_projects")
    op.drop_table("living_buildings_data", schema="user_projects")
    op.drop_table("services_data", schema="user_projects")
    op.drop_table("object_geometries_data", schema="user_projects")
    op.drop_table("projects_territory_data", schema="user_projects")
    op.execute(sa.schema.DropSequence(sa.Sequence("project_territory_id_seq", schema="user_projects")))
    op.execute(sa.schema.DropSequence(sa.Sequence("physical_objects_id_seq", schema="user_projects")))
    op.execute(sa.schema.DropSequence(sa.Sequence("project_id_seq", schema="user_projects")))
    op.execute(sa.schema.DropSequence(sa.Sequence("living_buildings_id_seq", schema="user_projects")))
    op.execute(sa.schema.DropSequence(sa.Sequence("object_geometries_id_seq", schema="user_projects")))
    op.execute(sa.schema.DropSequence(sa.Sequence("scenarios_data_id_seq", schema="user_projects")))
    op.execute(sa.schema.DropSequence(sa.Sequence("services_id_seq", schema="user_projects")))
    op.execute(sa.schema.DropSequence(sa.Sequence("urban_objects_id_seq", schema="user_projects")))
    op.execute("drop schema if exists user_projects")
    # TODO: лучше не удалять postgis?
    # op.execute("drop extension if exists postgis")
