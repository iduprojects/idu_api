# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""fix buildings profiles

Revision ID: 24a0fcf2e733
Revises: ac618e497103
Create Date: 2024-11-04 17:17:52.383721

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "24a0fcf2e733"
down_revision: Union[str, None] = "ac618e497103"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # fix `living_buildings_data`
    op.drop_column("living_buildings_data", "residental_number", schema="user_projects")
    op.drop_column("living_buildings_data", "residents_number")

    # fix `functional_zones_data`
    op.add_column(
        "functional_zones_data",
        sa.Column(
            "properties", postgresql.JSONB(astext_type=sa.Text()), server_default=sa.text("'{}'::jsonb"), nullable=False
        ),
    )
    op.add_column(
        "functional_zones_data",
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
    )
    op.add_column(
        "functional_zones_data",
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
    )

    # fix `profiles_data`
    op.add_column(
        "profiles_data",
        sa.Column(
            "properties", postgresql.JSONB(astext_type=sa.Text()), server_default=sa.text("'{}'::jsonb"), nullable=False
        ),
        schema="user_projects",
    )
    op.add_column(
        "profiles_data",
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
        schema="user_projects",
    )
    op.add_column(
        "profiles_data",
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
        schema="user_projects",
    )


def downgrade() -> None:
    # revert changes to `living_buildings_data`
    op.add_column(
        "living_buildings_data",
        sa.Column("residental_number", sa.Integer(), nullable=True),
        schema="user_projects",
    )
    op.add_column(
        "living_buildings_data",
        sa.Column("residents_number", sa.Integer(), nullable=True),
    )

    # revert changes to `functional_zones_data`
    op.drop_column("functional_zones_data", "properties")
    op.drop_column("functional_zones_data", "created_at")
    op.drop_column("functional_zones_data", "updated_at")

    # revert changes to `profiles_data`
    op.drop_column("profiles_data", "properties", schema="user_projects")
    op.drop_column("profiles_data", "created_at", schema="user_projects")
    op.drop_column("profiles_data", "updated_at", schema="user_projects")
