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
    op.execute(sa.schema.CreateSequence(sa.Sequence("project_id_seq")))
    op.execute(sa.schema.CreateSequence(sa.Sequence("project_territory_id_seq")))
    op.create_table(
        "projects_territory_data",
        sa.Column(
            "project_territory_id",
            sa.Integer(),
            server_default=sa.text("nextval('project_territory_id_seq')"),
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
        "projects_data",
        sa.Column(
            "project_id",
            sa.Integer(),
            server_default=sa.text("nextval('project_id_seq')"),
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


def downgrade() -> None:
    op.drop_table("projects_data", schema="user_projects")
    op.drop_table("projects_territory_data", schema="user_projects")
    op.execute(sa.schema.DropSequence(sa.Sequence("project_territory_id_seq")))
    op.execute(sa.schema.DropSequence(sa.Sequence("project_id_seq")))
    op.execute("drop extension if exists postgis")
    op.execute("drop schema if exists user_projects")
