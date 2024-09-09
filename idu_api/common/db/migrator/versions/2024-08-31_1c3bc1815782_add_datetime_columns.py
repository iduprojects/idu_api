# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""add datetime columns

Revision ID: 1c3bc1815782
Revises: 725b67283b5c
Create Date: 2024-08-29 15:57:04.557084

"""
from textwrap import dedent
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "1c3bc1815782"
down_revision: Union[str, None] = "725b67283b5c"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # columns
    op.add_column(
        "object_geometries_data",
        sa.Column("address", sa.String(length=300), nullable=True),
        schema="user_projects",
    )
    op.add_column(
        "physical_objects_data",
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
        schema="user_projects",
    )
    op.add_column(
        "physical_objects_data",
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
        schema="user_projects",
    )
    op.add_column(
        "services_data",
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
        schema="user_projects",
    )
    op.add_column(
        "services_data",
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
        schema="user_projects",
    )
    op.drop_column("physical_objects_data", "address", schema="user_projects")

    # constraints
    op.drop_constraint(
        "projects_territory_data_fk_parent_territory_id__project_5a39",
        "projects_territory_data",
        type_="foreignkey",
        schema="user_projects",
    )

    op.create_foreign_key(
        "projects_territory_data_fk_parent_territory_id__project_5a39",
        "projects_territory_data",
        "territories_data",
        ["parent_territory_id"],
        ["territory_id"],
        source_schema="user_projects",
        referent_schema="public",
    )


def downgrade() -> None:
    # columns
    op.add_column(
        "physical_objects_data",
        sa.Column("address", sa.String(length=300), nullable=True),
        schema="user_projects",
    )
    op.drop_column("object_geometries_data", "address", schema="user_projects")
    op.drop_column("physical_objects_data", "created_at", schema="user_projects")
    op.drop_column("physical_objects_data", "updated_at", schema="user_projects")
    op.drop_column("services_data", "created_at", schema="user_projects")
    op.drop_column("services_data", "updated_at", schema="user_projects")

    # data
    op.execute(
        sa.text(
            dedent(
                """
                DELETE FROM user_projects.projects_territory_data
                """
            )
        )
    )

    # constraints
    op.drop_constraint(
        "projects_territory_data_fk_parent_territory_id__project_5a39",
        "projects_territory_data",
        type_="foreignkey",
        schema="user_projects",
    )

    op.create_foreign_key(
        "projects_territory_data_fk_parent_territory_id__project_5a39",
        "projects_territory_data",
        "projects_territory_data",
        ["parent_territory_id"],
        ["project_territory_id"],
        source_schema="user_projects",
        referent_schema="user_projects",
    )
