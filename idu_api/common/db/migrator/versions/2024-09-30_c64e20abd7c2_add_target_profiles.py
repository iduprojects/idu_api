# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""add target profiles

Revision ID: c64e20abd7c2
Revises: 94e17a6f74bf
Create Date: 2024-09-30 17:25:02.967507

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "c64e20abd7c2"
down_revision: Union[str, None] = "94e17a6f74bf"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # sequences
    op.execute(sa.schema.CreateSequence(sa.Sequence("target_profile_id_seq", schema="user_projects")))

    # tables
    op.create_table(
        "target_profiles_dict",
        sa.Column(
            "target_profile_id",
            sa.Integer(),
            server_default=sa.text("nextval('user_projects.target_profile_id_seq')"),
            nullable=False,
        ),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.PrimaryKeyConstraint("target_profile_id", name=op.f("target_profiles_dict_pk")),
        schema="user_projects",
    )

    # columns
    op.add_column(
        "scenarios_data",
        sa.Column(
            "target_profile_id",
            sa.Integer(),
            sa.ForeignKey("user_projects.target_profiles_dict.target_profile_id"),
            nullable=True,
        ),
        schema="user_projects",
    )
    op.add_column(
        "profiles_data",
        sa.Column(
            "target_profile_id",
            sa.Integer(),
            sa.ForeignKey("user_projects.target_profiles_dict.target_profile_id"),
            nullable=False,
        ),
        schema="user_projects",
    )
    op.drop_column("profiles_data", "profile_type_id", schema="user_projects")


def downgrade() -> None:
    # columns
    op.drop_column("profiles_data", "target_profile_id", schema="user_projects")
    op.drop_column("scenarios_data", "target_profile_id", schema="user_projects")
    op.add_column("profiles_data", sa.Column("profile_type_id", sa.Integer(), nullable=False), schema="user_projects")

    # tables
    op.drop_table("target_profiles_dict", schema="user_projects")

    # sequences
    op.execute(sa.schema.DropSequence(sa.Sequence("target_profile_id_seq", schema="user_projects")))
