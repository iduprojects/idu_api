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
    op.add_column("functional_zones_data", sa.Column("name", sa.String(200), nullable=True))

    # fix `projects_functional_zones`
    op.add_column(
        "projects_functional_zones",
        sa.Column(
            "properties", postgresql.JSONB(astext_type=sa.Text()), server_default=sa.text("'{}'::jsonb"), nullable=False
        ),
        schema="user_projects",
    )
    op.add_column(
        "projects_functional_zones",
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
        schema="user_projects",
    )
    op.add_column(
        "projects_functional_zones",
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
        schema="user_projects",
    )
    op.drop_constraint(
        "profiles_data_fk_scenario_id__scenarios_data", "projects_functional_zones", schema="user_projects"
    )
    op.create_foreign_key(
        "profiles_data_fk_scenario_id__scenarios_data",
        "projects_functional_zones",
        "scenarios_data",
        ["scenario_id"],
        ["scenario_id"],
        source_schema="user_projects",
        referent_schema="user_projects",
        ondelete="CASCADE",
    )
    op.alter_column("projects_functional_zones", "name", nullable=True, schema="user_projects")


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
    op.drop_column("functional_zones_data", "name")

    # revert changes to `projects_functional_zones`
    op.drop_column("projects_functional_zones", "properties", schema="user_projects")
    op.drop_column("projects_functional_zones", "created_at", schema="user_projects")
    op.drop_column("projects_functional_zones", "updated_at", schema="user_projects")
    op.drop_constraint(
        "profiles_data_fk_scenario_id__scenarios_data", "projects_functional_zones", schema="user_projects"
    )
    op.create_foreign_key(
        "profiles_data_fk_scenario_id__scenarios_data",
        "projects_functional_zones",
        "scenarios_data",
        ["scenario_id"],
        ["scenario_id"],
        source_schema="user_projects",
        referent_schema="user_projects",
    )
    op.execute(sa.text("""DELETE FROM user_projects.projects_functional_zones WHERE name IS NULL"""))
    op.alter_column("projects_functional_zones", "name", nullable=False, schema="user_projects")
