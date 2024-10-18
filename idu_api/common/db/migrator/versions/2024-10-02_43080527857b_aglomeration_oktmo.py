# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""aglomeration oktmo

Revision ID: 43080527857b
Revises: 131faa7cad9c
Create Date: 2024-10-02 20:21:37.714218

"""
from typing import Sequence, Union

import geoalchemy2
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "43080527857b"
down_revision: Union[str, None] = "131faa7cad9c"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # sequences

    op.execute(sa.schema.CreateSequence(sa.Sequence("aglomeration_types_dict_id_seq")))
    op.execute(sa.schema.CreateSequence(sa.Sequence("aglomeration_data_id_seq")))

    # table
    op.create_table(
        "aglomeration_types_dict",
        sa.Column(
            "aglomeration_type_id",
            sa.Integer(),
            server_default=sa.text("nextval('aglomeration_types_dict_id_seq')"),
            nullable=False,
        ),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.PrimaryKeyConstraint("aglomeration_type_id", name=op.f("aglomeration_types_dict_pk")),
        sa.UniqueConstraint("name", name=op.f("aglomeration_types_dict_name_key")),
    )

    op.create_table(
        "aglomeration_data",
        sa.Column(
            "aglomeration_id",
            sa.Integer(),
            server_default=sa.text("nextval('aglomeration_data_id_seq')"),
            nullable=False,
        ),
        sa.Column("aglomeration_type_id", sa.Integer(), nullable=False),
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
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(
            ["parent_id"],
            ["aglomeration_data.aglomeration_id"],
            name=op.f("aglomeration_data_fk_parent_id__aglomeration_data"),
        ),
        sa.ForeignKeyConstraint(
            ["aglomeration_type_id"],
            ["aglomeration_types_dict.aglomeration_type_id"],
            name=op.f("aglomeration_data_fk_aglomeration_type_id__aglomeration_types_dict"),
        ),
        sa.PrimaryKeyConstraint("aglomeration_id", name=op.f("aglomeration_data_pk")),
    )

    # columns

    op.add_column("territories_data", sa.Column("oktmo_code", sa.String(length=20), nullable=True))


def downgrade() -> None:
    # columns
    op.drop_column("territories_data", "oktmo_code")

    # tables
    op.drop_table("aglomeration_data")
    op.drop_table("aglomeration_types_dict")

    # sequences
    op.execute(sa.schema.DropSequence(sa.Sequence("aglomeration_data_id_seq")))
    op.execute(sa.schema.DropSequence(sa.Sequence("aglomeration_types_dict_id_seq")))
