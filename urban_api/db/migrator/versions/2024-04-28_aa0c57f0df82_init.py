# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""init

Revision ID: aa0c57f0df82
Revises: 
Create Date: 2024-04-28 15:24:02.664341

"""

from typing import Sequence, Union

import geoalchemy2
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "aa0c57f0df82"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # extensions

    op.execute(sa.text("CREATE EXTENSION IF NOT EXISTS postgis"))

    # sequences

    op.execute(sa.schema.CreateSequence(sa.Sequence("functional_zone_types_dict_id_seq")))
    op.execute(sa.schema.CreateSequence(sa.Sequence("measurement_units_dict_id_seq")))
    op.execute(sa.schema.CreateSequence(sa.Sequence("physical_object_types_dict_id_seq")))
    op.execute(sa.schema.CreateSequence(sa.Sequence("territory_types_dict_id_seq")))
    op.execute(sa.schema.CreateSequence(sa.Sequence("urban_functions_dict_id_seq")))
    op.execute(sa.schema.CreateSequence(sa.Sequence("indicators_dict_id_seq")))
    op.execute(sa.schema.CreateSequence(sa.Sequence("physical_objects_data_id_seq")))
    op.execute(sa.schema.CreateSequence(sa.Sequence("service_types_dict_id_seq")))
    op.execute(sa.schema.CreateSequence(sa.Sequence("territories_data_id_seq")))
    op.execute(sa.schema.CreateSequence(sa.Sequence("functional_zones_data_id_seq")))
    op.execute(sa.schema.CreateSequence(sa.Sequence("living_buildings_data_id_seq")))
    op.execute(sa.schema.CreateSequence(sa.Sequence("object_geometries_data_id_seq")))
    op.execute(sa.schema.CreateSequence(sa.Sequence("service_types_normatives_data_id_seq")))
    op.execute(sa.schema.CreateSequence(sa.Sequence("services_data_id_seq")))
    op.execute(sa.schema.CreateSequence(sa.Sequence("urban_objects_data_id_seq")))

    # tables

    op.create_table(
        "functional_zone_types_dict",
        sa.Column(
            "functional_zone_type_id",
            sa.Integer(),
            server_default=sa.text("nextval('functional_zone_types_dict_id_seq')"),
            nullable=False,
        ),
        sa.Column("name", sa.String(length=100), nullable=False),
        sa.Column("zone_nickname", sa.String(length=100), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("functional_zone_type_id", name=op.f("functional_zone_types_dict_pk")),
        sa.UniqueConstraint("name", name=op.f("functional_zone_types_dict_name_key")),
    )

    op.create_table(
        "measurement_units_dict",
        sa.Column(
            "measurement_unit_id",
            sa.Integer(),
            server_default=sa.text("nextval('measurement_units_dict_id_seq')"),
            nullable=False,
        ),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.PrimaryKeyConstraint("measurement_unit_id", name=op.f("measurement_units_dict_pk")),
        sa.UniqueConstraint("name", name=op.f("measurement_units_dict_name_key")),
    )

    op.create_table(
        "physical_object_types_dict",
        sa.Column(
            "physical_object_type_id",
            sa.Integer(),
            server_default=sa.text("nextval('physical_object_types_dict_id_seq')"),
            nullable=False,
        ),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.PrimaryKeyConstraint("physical_object_type_id", name=op.f("physical_object_types_dict_pk")),
        sa.UniqueConstraint("name", name=op.f("physical_object_types_dict_name_key")),
    )

    op.create_table(
        "territory_types_dict",
        sa.Column(
            "territory_type_id",
            sa.Integer(),
            server_default=sa.text("nextval('territory_types_dict_id_seq')"),
            nullable=False,
        ),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.PrimaryKeyConstraint("territory_type_id", name=op.f("territory_types_dict_pk")),
        sa.UniqueConstraint("name", name=op.f("territory_types_dict_name_key")),
    )

    op.create_table(
        "urban_functions_dict",
        sa.Column(
            "urban_function_id",
            sa.Integer(),
            server_default=sa.text("nextval('urban_functions_dict_id_seq')"),
            nullable=False,
        ),
        sa.Column("parent_urban_function_id", sa.Integer(), nullable=True),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column("level", sa.Integer(), nullable=False),
        sa.Column("list_label", sa.String(length=20), nullable=False),
        sa.Column("code", sa.String(length=50), nullable=False),
        sa.ForeignKeyConstraint(
            ["parent_urban_function_id"],
            ["urban_functions_dict.urban_function_id"],
            name=op.f("urban_functions_dict_fk_parent_urban_function_id__urban_functions_dict"),
        ),
        sa.PrimaryKeyConstraint("urban_function_id", name=op.f("urban_functions_dict_pk")),
        sa.UniqueConstraint("list_label", name=op.f("urban_functions_dict_list_label_key")),
        sa.UniqueConstraint("name", name=op.f("urban_functions_dict_name_key")),
    )

    op.create_table(
        "indicators_dict",
        sa.Column(
            "indicator_id", sa.Integer(), server_default=sa.text("nextval('indicators_dict_id_seq')"), nullable=False
        ),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column("measurement_unit_id", sa.Integer(), nullable=False),
        sa.Column("level", sa.Integer(), nullable=True),
        sa.Column("list_label", sa.String(length=20), nullable=False),
        sa.Column("parent_id", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(
            ["measurement_unit_id"],
            ["measurement_units_dict.measurement_unit_id"],
            name=op.f("indicators_dict_fk_measurement_unit_id__measurement_units_dict"),
        ),
        sa.ForeignKeyConstraint(
            ["parent_id"], ["indicators_dict.indicator_id"], name=op.f("indicators_dict_fk_parent_id__indicators_dict")
        ),
        sa.PrimaryKeyConstraint("indicator_id", name=op.f("indicators_dict_pk")),
        sa.UniqueConstraint("name", name=op.f("indicators_dict_name_key")),
    )

    op.create_table(
        "physical_objects_data",
        sa.Column(
            "physical_object_id",
            sa.Integer(),
            server_default=sa.text("nextval('physical_objects_data_id_seq')"),
            nullable=False,
        ),
        sa.Column("physical_object_type_id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=300), nullable=True),
        sa.Column(
            "properties", postgresql.JSONB(astext_type=sa.Text()), server_default=sa.text("'{}'::jsonb"), nullable=False
        ),
        sa.ForeignKeyConstraint(
            ["physical_object_type_id"],
            ["physical_object_types_dict.physical_object_type_id"],
            name=op.f("physical_objects_data_fk_physical_object_type_id__physical_object_types_dict"),
        ),
        sa.PrimaryKeyConstraint("physical_object_id", name=op.f("physical_objects_data_pk")),
    )

    op.create_table(
        "service_types_dict",
        sa.Column(
            "service_type_id",
            sa.Integer(),
            server_default=sa.text("nextval('service_types_dict_id_seq')"),
            nullable=False,
        ),
        sa.Column("urban_function_id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column("capacity_modeled", sa.Integer(), nullable=True),
        sa.Column("code", sa.String(length=50), nullable=False),
        sa.ForeignKeyConstraint(
            ["urban_function_id"],
            ["urban_functions_dict.urban_function_id"],
            name=op.f("service_types_dict_fk_urban_function_id__urban_functions_dict"),
        ),
        sa.PrimaryKeyConstraint("service_type_id", name=op.f("service_types_dict_pk")),
        sa.UniqueConstraint("name", name=op.f("service_types_dict_name_key")),
    )

    op.create_table(
        "territories_data",
        sa.Column(
            "territory_id", sa.Integer(), server_default=sa.text("nextval('territories_data_id_seq')"), nullable=False
        ),
        sa.Column("territory_type_id", sa.Integer(), nullable=False),
        sa.Column("parent_id", sa.Integer(), nullable=True),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column(
            "geometry",
            geoalchemy2.types.Geometry(
                spatial_index=False, from_text="ST_GeomFromEWKT", name="geometry", nullable=False
            ),
            nullable=False,
        ),
        sa.Column("level", sa.Integer(), nullable=False),
        sa.Column(
            "properties", postgresql.JSONB(astext_type=sa.Text()), server_default=sa.text("'{}'::jsonb"), nullable=False
        ),
        sa.Column(
            "centre_point",
            geoalchemy2.types.Geometry(
                geometry_type="POINT", spatial_index=False, from_text="ST_GeomFromEWKT", name="geometry", nullable=False
            ),
            nullable=False,
        ),
        sa.Column("admin_center", sa.Integer(), nullable=True),
        sa.Column("okato_code", sa.String(length=20), nullable=True),
        sa.ForeignKeyConstraint(
            ["parent_id"],
            ["territories_data.territory_id"],
            name=op.f("territories_data_fk_parent_id__territories_data"),
        ),
        sa.ForeignKeyConstraint(
            ["territory_type_id"],
            ["territory_types_dict.territory_type_id"],
            name=op.f("territories_data_fk_territory_type_id__territory_types_dict"),
        ),
        sa.PrimaryKeyConstraint("territory_id", name=op.f("territories_data_pk")),
    )

    op.create_table(
        "functional_zones_data",
        sa.Column(
            "functional_zone_id",
            sa.Integer(),
            server_default=sa.text("nextval('functional_zones_data_id_seq')"),
            nullable=False,
        ),
        sa.Column("territory_id", sa.Integer(), nullable=False),
        sa.Column("functional_zone_type_id", sa.Integer(), nullable=False),
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
            ["territory_id"],
            ["territories_data.territory_id"],
            name=op.f("functional_zones_data_fk_territory_id__territories_data"),
        ),
        sa.PrimaryKeyConstraint("functional_zone_id", name=op.f("functional_zones_data_pk")),
    )

    op.create_table(
        "living_buildings_data",
        sa.Column(
            "living_building_id",
            sa.Integer(),
            server_default=sa.text("nextval('living_buildings_data_id_seq')"),
            nullable=False,
        ),
        sa.Column("physical_object_id", sa.Integer(), nullable=False),
        sa.Column("residents_number", sa.Integer(), nullable=True),
        sa.Column("living_area", sa.Float(precision=53), nullable=True),
        sa.Column(
            "properties", postgresql.JSONB(astext_type=sa.Text()), server_default=sa.text("'{}'::jsonb"), nullable=False
        ),
        sa.ForeignKeyConstraint(
            ["physical_object_id"],
            ["physical_objects_data.physical_object_id"],
            name=op.f("living_buildings_data_fk_physical_object_id__physical_objects_data"),
        ),
        sa.PrimaryKeyConstraint("living_building_id", name=op.f("living_buildings_data_pk")),
    )

    op.create_table(
        "object_geometries_data",
        sa.Column(
            "object_geometry_id",
            sa.Integer(),
            server_default=sa.text("nextval('object_geometries_data_id_seq')"),
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
        sa.Column("address", sa.String(length=300), nullable=True),
        sa.ForeignKeyConstraint(
            ["territory_id"],
            ["territories_data.territory_id"],
            name=op.f("object_geometries_data_fk_territory_id__territories_data"),
        ),
        sa.PrimaryKeyConstraint("object_geometry_id", name=op.f("object_geometries_data_pk")),
    )

    op.create_table(
        "service_types_normatives_data",
        sa.Column(
            "normative_id",
            sa.Integer(),
            server_default=sa.text("nextval('service_types_normatives_data_id_seq')"),
            nullable=False,
        ),
        sa.Column("service_type_id", sa.Integer(), nullable=False),
        sa.Column("urban_function_id", sa.Integer(), nullable=False),
        sa.Column("territory_id", sa.Integer(), nullable=False),
        sa.Column("is_regulated", sa.Boolean(), nullable=False),
        sa.Column("radius_availability_meters", sa.Integer(), nullable=True),
        sa.Column("time_availability_minutes", sa.Integer(), nullable=True),
        sa.Column("services_per_1000_normative", sa.Float(precision=53), nullable=True),
        sa.Column("services_capacity_per_1000_normative", sa.Float(precision=53), nullable=True),
        sa.ForeignKeyConstraint(
            ["service_type_id"],
            ["service_types_dict.service_type_id"],
            name=op.f("service_types_normatives_data_fk_service_type_id__service_types_dict"),
        ),
        sa.ForeignKeyConstraint(
            ["territory_id"],
            ["territories_data.territory_id"],
            name=op.f("service_types_normatives_data_fk_territory_id__territories_data"),
        ),
        sa.ForeignKeyConstraint(
            ["urban_function_id"],
            ["urban_functions_dict.urban_function_id"],
            name=op.f("service_types_normatives_data_fk_urban_function_id__urban_functions_dict"),
        ),
        sa.PrimaryKeyConstraint("normative_id", name=op.f("service_types_normatives_data_pk")),
        sa.UniqueConstraint(
            "service_type_id",
            "urban_function_id",
            "territory_id",
            name=op.f("service_types_normatives_data_service_type_id_urban_function_id_territory_id_key"),
        ),
    )

    op.create_table(
        "services_data",
        sa.Column(
            "service_id", sa.Integer(), server_default=sa.text("nextval('services_data_id_seq')"), nullable=False
        ),
        sa.Column("service_type_id", sa.Integer(), nullable=False),
        sa.Column("territory_type_id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column("capacity_real", sa.Integer(), nullable=True),
        sa.Column(
            "properties", postgresql.JSONB(astext_type=sa.Text()), server_default=sa.text("'{}'::jsonb"), nullable=False
        ),
        sa.ForeignKeyConstraint(
            ["service_type_id"],
            ["service_types_dict.service_type_id"],
            name=op.f("services_data_fk_service_type_id__service_types_dict"),
        ),
        sa.ForeignKeyConstraint(
            ["territory_type_id"],
            ["territory_types_dict.territory_type_id"],
            name=op.f("services_data_fk_territory_type_id__territory_types_dict"),
        ),
        sa.PrimaryKeyConstraint("service_id", name=op.f("services_data_pk")),
    )

    op.create_table(
        "territory_indicators_data",
        sa.Column("indicator_id", sa.Integer(), nullable=False),
        sa.Column("territory_id", sa.Integer(), nullable=False),
        sa.Column(
            "date_type", sa.Enum("year", "half_year", "quarter", "month", "day", name="date_field_type"), nullable=False
        ),
        sa.Column("date_value", sa.Date(), nullable=False),
        sa.Column("value", sa.Float(precision=53), nullable=False),
        sa.Column("value_type", sa.Enum("real", "forecast", "target", name="indicator_value_type"), nullable=False),
        sa.Column("information_source", sa.String(length=300), nullable=True),
        sa.ForeignKeyConstraint(
            ["indicator_id"],
            ["indicators_dict.indicator_id"],
            name=op.f("territory_indicators_data_fk_indicator_id__indicators_dict"),
        ),
        sa.ForeignKeyConstraint(
            ["territory_id"],
            ["territories_data.territory_id"],
            name=op.f("territory_indicators_data_fk_territory_id__territories_data"),
        ),
        sa.PrimaryKeyConstraint(
            "indicator_id", "territory_id", "date_type", "date_value", name=op.f("territory_indicators_data_pk")
        ),
    )

    op.create_table(
        "urban_objects_data",
        sa.Column(
            "urban_object_id",
            sa.Integer(),
            server_default=sa.text("nextval('urban_objects_data_id_seq')"),
            nullable=False,
        ),
        sa.Column("physical_object_id", sa.Integer(), nullable=False),
        sa.Column("object_geometry_id", sa.Integer(), nullable=False),
        sa.Column("service_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["object_geometry_id"],
            ["object_geometries_data.object_geometry_id"],
            name=op.f("urban_objects_data_fk_object_geometry_id__object_geometries_data"),
        ),
        sa.ForeignKeyConstraint(
            ["physical_object_id"],
            ["physical_objects_data.physical_object_id"],
            name=op.f("urban_objects_data_fk_physical_object_id__physical_objects_data"),
        ),
        sa.ForeignKeyConstraint(
            ["service_id"], ["services_data.service_id"], name=op.f("urban_objects_data_fk_service_id__services_data")
        ),
        sa.PrimaryKeyConstraint("urban_object_id", name=op.f("urban_objects_data_pk")),
        sa.UniqueConstraint(
            "physical_object_id",
            "object_geometry_id",
            name=op.f("urban_objects_data_physical_object_id_object_geometry_id_key"),
        ),
    )


def downgrade() -> None:
    # tables
    op.drop_constraint(op.f("urban_functions_dict_list_label_key"), "urban_functions_dict", type_="unique")

    op.drop_table("urban_objects_data")
    op.drop_table("territory_indicators_data")
    op.drop_table("services_data")
    op.drop_table("service_types_normatives_data")
    op.drop_table("object_geometries_data")
    op.drop_table("living_buildings_data")
    op.drop_table("functional_zones_data")
    op.drop_table("territories_data")
    op.drop_table("service_types_dict")
    op.drop_table("physical_objects_data")
    op.drop_table("indicators_dict")
    op.drop_table("urban_functions_dict")
    op.drop_table("territory_types_dict")
    op.drop_table("physical_object_types_dict")
    op.drop_table("measurement_units_dict")
    op.drop_table("functional_zone_types_dict")

    # sequences

    op.execute(sa.schema.DropSequence(sa.Sequence("urban_objects_data_id_seq")))
    op.execute(sa.schema.DropSequence(sa.Sequence("services_data_id_seq")))
    op.execute(sa.schema.DropSequence(sa.Sequence("service_types_normatives_data_id_seq")))
    op.execute(sa.schema.DropSequence(sa.Sequence("object_geometries_data_id_seq")))
    op.execute(sa.schema.DropSequence(sa.Sequence("living_buildings_data_id_seq")))
    op.execute(sa.schema.DropSequence(sa.Sequence("functional_zones_data_id_seq")))
    op.execute(sa.schema.DropSequence(sa.Sequence("territories_data_id_seq")))
    op.execute(sa.schema.DropSequence(sa.Sequence("service_types_dict_id_seq")))
    op.execute(sa.schema.DropSequence(sa.Sequence("physical_objects_data_id_seq")))
    op.execute(sa.schema.DropSequence(sa.Sequence("indicators_dict_id_seq")))
    op.execute(sa.schema.DropSequence(sa.Sequence("urban_functions_dict_id_seq")))
    op.execute(sa.schema.DropSequence(sa.Sequence("territory_types_dict_id_seq")))
    op.execute(sa.schema.DropSequence(sa.Sequence("physical_object_types_dict_id_seq")))
    op.execute(sa.schema.DropSequence(sa.Sequence("measurement_units_dict_id_seq")))
    op.execute(sa.schema.DropSequence(sa.Sequence("functional_zone_types_dict_id_seq")))

    # types

    op.execute(sa.text("DROP TYPE date_field_type"))
    op.execute(sa.text("DROP TYPE indicator_value_type"))

    # extensions

    op.execute(sa.text("DROP EXTENSION postgis"))
