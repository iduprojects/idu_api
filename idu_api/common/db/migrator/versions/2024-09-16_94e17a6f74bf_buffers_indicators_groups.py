# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""buffers and indicator groups added, created_at and updated_at added to object_geometries_data and indicators_dict

Revision ID: 94e17a6f74bf
Revises: 38ff7a2d4779
Create Date: 2024-09-16 22:26:01.775927

"""
from typing import Callable, Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy import func

func: Callable

# revision identifiers, used by Alembic.
revision: str = "94e17a6f74bf"
down_revision: Union[str, None] = "38ff7a2d4779"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "object_geometries_data",
        sa.Column("created_at", sa.TIMESTAMP(True), nullable=False, server_default=func.now()),
    )
    op.add_column(
        "object_geometries_data",
        sa.Column("updated_at", sa.TIMESTAMP(True), nullable=False, server_default=func.now()),
    )
    op.add_column(
        "indicators_dict", sa.Column("created_at", sa.TIMESTAMP(True), nullable=False, server_default=func.now())
    )
    op.add_column(
        "indicators_dict", sa.Column("updated_at", sa.TIMESTAMP(True), nullable=False, server_default=func.now())
    )

    op.execute(sa.schema.CreateSequence(sa.Sequence("buffer_types_dict_id_seq")))
    op.execute(sa.schema.CreateSequence(sa.Sequence("indicators_groups_dict_id_seq")))

    op.create_table(
        "buffer_types_dict",
        sa.Column(
            "buffer_type_id",
            sa.Integer(),
            server_default=sa.text("nextval('buffer_types_dict_id_seq')"),
            nullable=False,
        ),
        sa.Column("name", sa.String(length=100), nullable=False),
        sa.PrimaryKeyConstraint("buffer_type_id", name=op.f("buffer_types_dict_pk")),
        sa.UniqueConstraint("name", name=op.f("buffer_types_dict_name_key")),
    )

    op.create_table(
        "buffers_data",
        sa.Column("buffer_type_id", sa.Integer(), nullable=False),
        sa.Column("urban_object_id", sa.Integer(), nullable=False),
        sa.Column("buffer_geometry_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["buffer_type_id"],
            ["buffer_types_dict.buffer_type_id"],
            name=op.f("buffers_data_fk_buffer_type_id__buffer_types_dict"),
        ),
        sa.ForeignKeyConstraint(
            ["urban_object_id"],
            ["urban_objects_data.urban_object_id"],
            name=op.f("buffers_data_fk_urban_object_id__urban_objects_data"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["buffer_geometry_id"],
            ["object_geometries_data.object_geometry_id"],
            name=op.f("buffers_data_fk_object_geometry_id__object_geometries"),
        ),
        sa.PrimaryKeyConstraint(
            "buffer_type_id", "urban_object_id", "buffer_geometry_id", name=op.f("buffers_data_pk")
        ),
    )

    op.create_table(
        "indicators_groups_dict",
        sa.Column(
            "indicators_group_id",
            sa.Integer(),
            server_default=sa.text("nextval('indicators_groups_dict_id_seq')"),
            nullable=False,
        ),
        sa.Column("name", sa.String(length=100), nullable=False),
        sa.PrimaryKeyConstraint("indicators_group_id", name=op.f("indicators_groups_dict_pk")),
        sa.UniqueConstraint("name", name=op.f("indicators_groups_dict_name_key")),
    )

    op.create_table(
        "indicators_groups_data",
        sa.Column("indicator_id", sa.Integer(), nullable=False),
        sa.Column("indicators_group_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["indicator_id"],
            ["indicators_dict.indicator_id"],
            name=op.f("indicators_groups_data_fk_indicator_id__indicators_dict"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["indicators_group_id"],
            ["indicators_groups_dict.indicators_group_id"],
            name=op.f("indicators_groups_data_fk_indicators_group_id__indicators_groups_dict"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("indicator_id", "indicators_group_id", name=op.f("indicators_groups_data_pk")),
    )


def downgrade() -> None:
    op.drop_table("indicators_groups_data")
    op.drop_table("indicators_groups_dict")
    op.drop_table("buffers_data")
    op.drop_table("buffer_types_dict")

    op.execute(sa.schema.DropSequence(sa.Sequence("indicators_groups_dict_id_seq")))
    op.execute(sa.schema.DropSequence(sa.Sequence("buffer_types_dict_id_seq")))

    op.drop_column("object_geometries_data", "created_at")
    op.drop_column("object_geometries_data", "updated_at")
    op.drop_column("indicators_dict", "created_at")
    op.drop_column("indicators_dict", "updated_at")
