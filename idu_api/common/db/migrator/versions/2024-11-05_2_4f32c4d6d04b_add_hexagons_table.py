# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""add hexagons table

Revision ID: 4f32c4d6d04b
Revises: d8568eb83f18
Create Date: 2024-11-05 18:25:26.195570

"""
from typing import Sequence, Union

import geoalchemy2
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "4f32c4d6d04b"
down_revision: Union[str, None] = "d8568eb83f18"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # create `hexagons_data` table
    op.execute(sa.schema.CreateSequence(sa.Sequence("hexagons_data_id_seq", schema="user_projects")))
    op.create_table(
        "hexagons_data",
        sa.Column(
            "hexagon_id",
            sa.Integer(),
            server_default=sa.text("nextval('user_projects.hexagons_data_id_seq')"),
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
        sa.Column(
            "properties", postgresql.JSONB(astext_type=sa.Text()), server_default=sa.text("'{}'::jsonb"), nullable=False
        ),
        sa.ForeignKeyConstraint(
            ["territory_id"],
            ["public.territories_data.territory_id"],
            name=op.f("hexagons_data_fk_territory_id__territories_data"),
        ),
        sa.PrimaryKeyConstraint("hexagon_id", name=op.f("hexagons_data_pk")),
        schema="user_projects",
    )

    # fix `indicators_data`
    op.drop_constraint("indicators_data_fk_project_territory_id__ptd", "indicators_data", schema="user_projects")
    op.drop_column("indicators_data", "project_territory_id", schema="user_projects")
    op.add_column(
        "indicators_data",
        sa.Column("hexagon_id", sa.Integer(), nullable=True),
        schema="user_projects",
    )
    op.create_foreign_key(
        "indicators_data_fk_hexagon_id__hexagons_data",
        "indicators_data",
        "hexagons_data",
        ["hexagon_id"],
        ["hexagon_id"],
        source_schema="user_projects",
        referent_schema="user_projects",
    )


def downgrade() -> None:
    # reverse changes from `indicators_data`
    op.drop_constraint("indicators_data_fk_hexagon_id__hexagons_data", "indicators_data", schema="user_projects")
    op.drop_column("indicators_data", "hexagon_id", schema="user_projects")
    op.add_column(
        "indicators_data",
        sa.Column("project_territory_id", sa.Integer(), nullable=True),
        schema="user_projects",
    )
    op.create_foreign_key(
        "indicators_data_fk_project_territory_id__ptd",
        "indicators_data",
        "projects_territory_data",
        ["project_territory_id"],
        ["project_territory_id"],
        source_schema="user_projects",
        referent_schema="user_projects",
    )

    # drop table `hexagons_data`
    op.drop_table("hexagons_data", schema="user_projects")
    op.execute(sa.schema.DropSequence(sa.Sequence("hexagons_data_id_seq", schema="user_projects")))
