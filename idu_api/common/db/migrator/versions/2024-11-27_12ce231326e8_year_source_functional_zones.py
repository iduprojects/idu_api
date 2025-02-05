# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""year source functional zones

Revision ID: 12ce231326e8
Revises: 39fd8a1c52c1
Create Date: 2024-11-27 13:15:38.165782

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "12ce231326e8"
down_revision: Union[str, None] = "39fd8a1c52c1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # add columns `year` and `source` to `public.functional_zones_data` and `projects_functional_zones`
    op.add_column(
        "functional_zones_data",
        sa.Column("year", sa.Integer(), nullable=True),
    )
    op.add_column(
        "functional_zones_data",
        sa.Column("source", sa.String(length=200), nullable=True),
    )
    op.add_column(
        "projects_functional_zones",
        sa.Column("year", sa.Integer(), nullable=True),
        schema="user_projects",
    )
    op.add_column(
        "projects_functional_zones",
        sa.Column("source", sa.String(length=200), nullable=True),
        schema="user_projects",
    )


def downgrade() -> None:
    # drop columns
    op.drop_column("functional_zones_data", "year")
    op.drop_column("functional_zones_data", "source")
    op.drop_column("projects_functional_zones", "year", schema="user_projects")
    op.drop_column("projects_functional_zones", "source", schema="user_projects")
