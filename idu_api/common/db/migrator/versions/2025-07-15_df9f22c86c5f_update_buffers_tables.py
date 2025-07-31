# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""update buffers tables

Revision ID: df9f22c86c5f
Revises: 7d75e7a728f1
Create Date: 2025-07-15 12:59:34.510416

"""
from textwrap import dedent
from typing import Sequence, Union

import geoalchemy2
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "df9f22c86c5f"
down_revision: Union[str, None] = "7d75e7a728f1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # drop `public.buffers_data.buffer_geometry_id` column and add geometry column
    op.add_column(
        "buffers_data",
        sa.Column("is_custom", sa.Boolean(), server_default=sa.false(), nullable=False),
    ),
    op.add_column(
        "buffers_data",
        sa.Column(
            "geometry",
            geoalchemy2.types.Geometry(
                spatial_index=False, from_text="ST_GeomFromEWKT", name="geometry", nullable=False
            ),
            nullable=False,
        ),
    )
    op.execute(
        sa.text(
            dedent(
                """
                UPDATE buffers_data b
                SET geometry = ogd.geometry
                FROM object_geometries_data ogd
                WHERE b.buffer_geometry_id = ogd.object_geometry_id;
                """
            )
        )
    )
    op.execute(
        sa.text(
            dedent(
                """
                DELETE FROM object_geometries_data
                WHERE object_geometry_id IN (SELECT buffer_geometry_id FROM buffers_data);
                """
            )
        )
    )
    op.drop_constraint("buffers_data_fk_object_geometry_id__object_geometries", "buffers_data")
    op.drop_constraint("buffers_data_pk", "buffers_data")
    op.drop_column("buffers_data", "buffer_geometry_id")
    op.create_primary_key("buffers_data_pk", "buffers_data", ["buffer_type_id", "urban_object_id"])

    # create `public.default_buffer_values_dict` table
    op.execute(sa.schema.CreateSequence(sa.Sequence("default_buffer_values_dict_id_seq")))
    op.create_table(
        "default_buffer_values_dict",
        sa.Column(
            "default_buffer_value_id",
            sa.Integer(),
            server_default=sa.text("nextval('default_buffer_values_dict_id_seq')"),
            nullable=False,
        ),
        sa.Column("buffer_type_id", sa.Integer(), nullable=False),
        sa.Column("physical_object_type_id", sa.Integer(), nullable=True),
        sa.Column("service_type_id", sa.Integer(), nullable=True),
        sa.Column("buffer_value", sa.Float(precision=53), nullable=False),
        sa.ForeignKeyConstraint(
            ["buffer_type_id"],
            ["buffer_types_dict.buffer_type_id"],
            name=op.f("default_buffer_values_dict_fk_buffer_type_id"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["physical_object_type_id"],
            ["physical_object_types_dict.physical_object_type_id"],
            name=op.f("default_buffer_values_dict_fk_physical_object_type_id"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["service_type_id"],
            ["service_types_dict.service_type_id"],
            name=op.f("default_buffer_values_dict_fk_service_type_id"),
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint(
            "buffer_type_id",
            "physical_object_type_id",
            "service_type_id",
            name=op.f("default_buffer_values_dict_unique"),
        ),
        sa.PrimaryKeyConstraint("default_buffer_value_id", name=op.f("default_buffer_values_dict_pk")),
    )

    # create `user_projects.buffers_data` table
    op.create_table(
        "buffers_data",
        sa.Column("buffer_type_id", sa.Integer(), nullable=False),
        sa.Column("urban_object_id", sa.Integer(), nullable=False),
        sa.Column(
            "geometry",
            geoalchemy2.types.Geometry(
                spatial_index=False, from_text="ST_GeomFromEWKT", name="geometry", nullable=False
            ),
            nullable=False,
        ),
        sa.Column("is_custom", sa.Boolean(), server_default=sa.false(), nullable=False),
        sa.ForeignKeyConstraint(
            ["buffer_type_id"],
            ["buffer_types_dict.buffer_type_id"],
            name=op.f("user_projects_buffers_data_fk_buffer_type_id"),
        ),
        sa.ForeignKeyConstraint(
            ["urban_object_id"],
            ["user_projects.urban_objects_data.urban_object_id"],
            name=op.f("user_projects_buffers_data_fk_urban_object_id"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("buffer_type_id", "urban_object_id", name=op.f("buffers_data_pk")),
        schema="user_projects",
    )

    # create indexes
    for schema in ("public", "user_projects"):
        op.create_index(
            "buffers_data_type_urban_object_idx",
            "buffers_data",
            ["buffer_type_id", "urban_object_id"],
            schema=schema,
        )
        op.create_index(
            "buffers_data_geometry_idx",
            "buffers_data",
            ["geometry"],
            postgresql_using="gist",
            schema=schema,
        )


def downgrade() -> None:
    # drop indexes
    for schema in ("public", "user_projects"):
        op.drop_index("buffers_data_type_urban_object_idx", "buffers_data", schema=schema)
        op.drop_index("buffers_data_geometry_idx", "buffers_data", schema=schema)
    # drop `user_projects.buffers_data` table
    op.drop_table("buffers_data", schema="user_projects")

    # drop `default_buffer_values_dict` table
    op.drop_table("default_buffer_values_dict")
    op.execute(sa.schema.DropSequence(sa.Sequence("default_buffer_values_dict_id_seq")))

    # revert changes in `public.buffers_data` table
    op.execute(sa.text(dedent("DELETE FROM public.buffers_data")))
    op.drop_constraint("buffers_data_pk", "buffers_data", type_="primary")
    op.drop_column("buffers_data", "geometry")
    op.drop_column("buffers_data", "is_custom")
    op.add_column(
        "buffers_data",
        sa.Column("buffer_geometry_id", sa.Integer(), nullable=False),
    )
    op.create_foreign_key(
        "buffers_data_fk_object_geometry_id__object_geometries",
        "buffers_data",
        "object_geometries_data",
        ["buffer_geometry_id"],
        ["object_geometry_id"],
    )
    op.create_primary_key(
        "buffers_data_pk", "buffers_data", ["buffer_type_id", "urban_object_id", "buffer_geometry_id"]
    )
