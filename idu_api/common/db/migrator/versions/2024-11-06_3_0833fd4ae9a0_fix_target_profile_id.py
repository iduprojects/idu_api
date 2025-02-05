# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""fix target profile id

Revision ID: 0833fd4ae9a0
Revises: 5ae105651257
Create Date: 2024-11-06 15:31:09.250646

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0833fd4ae9a0"
down_revision: Union[str, None] = "5ae105651257"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # fix `target_profile_id` to `functional_zone_type_id`
    op.drop_constraint(
        "profiles_data_fk_func_zone_type_id__func_zone_type_dict", "projects_functional_zones", schema="user_projects"
    )
    op.drop_column("projects_functional_zones", "target_profile_id", schema="user_projects")
    op.drop_constraint(
        "scenarios_data_fk_func_zone_type_id__func_zone_type_dict", "scenarios_data", schema="user_projects"
    )
    op.drop_column("scenarios_data", "target_profile_id", schema="user_projects")
    op.add_column(
        "projects_functional_zones",
        sa.Column("functional_zone_type_id", sa.Integer(), nullable=False),
        schema="user_projects",
    )
    op.create_foreign_key(
        "profiles_data_fk_func_zone_type_id__func_zone_type_dict",
        "projects_functional_zones",
        "functional_zone_types_dict",
        ["functional_zone_type_id"],
        ["functional_zone_type_id"],
        source_schema="user_projects",
        referent_schema="public",
    )
    op.add_column(
        "scenarios_data",
        sa.Column("functional_zone_type_id", sa.Integer(), nullable=True),
        schema="user_projects",
    )
    op.create_foreign_key(
        "scenarios_data_fk_func_zone_type_id__func_zone_type_dict",
        "scenarios_data",
        "functional_zone_types_dict",
        ["functional_zone_type_id"],
        ["functional_zone_type_id"],
        source_schema="user_projects",
        referent_schema="public",
    )


def downgrade() -> None:
    # revert `functional_zone_type_id` back to `target_profile_id` in `projects_functional_zones`
    op.drop_constraint(
        "profiles_data_fk_func_zone_type_id__func_zone_type_dict", "projects_functional_zones", schema="user_projects"
    )
    op.drop_column("projects_functional_zones", "functional_zone_type_id", schema="user_projects")
    op.add_column(
        "projects_functional_zones",
        sa.Column("target_profile_id", sa.Integer(), nullable=True),
        schema="user_projects",
    )
    op.create_foreign_key(
        "profiles_data_fk_func_zone_type_id__func_zone_type_dict",
        "projects_functional_zones",
        "functional_zone_types_dict",
        ["target_profile_id"],
        ["functional_zone_type_id"],
        source_schema="user_projects",
        referent_schema="public",
    )

    # Revert `functional_zone_type_id` back to `target_profile_id` in `scenarios_data`
    op.drop_constraint(
        "scenarios_data_fk_func_zone_type_id__func_zone_type_dict", "scenarios_data", schema="user_projects"
    )
    op.drop_column("scenarios_data", "functional_zone_type_id", schema="user_projects")
    op.add_column(
        "scenarios_data",
        sa.Column("target_profile_id", sa.Integer(), nullable=True),
        schema="user_projects",
    )
    op.create_foreign_key(
        "scenarios_data_fk_func_zone_type_id__func_zone_type_dict",
        "scenarios_data",
        "functional_zone_types_dict",
        ["target_profile_id"],
        ["functional_zone_type_id"],
        source_schema="user_projects",
        referent_schema="public",
    )
