# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""year source not null

Revision ID: 26917509c474
Revises: 12ce231326e8
Create Date: 2024-11-29 12:43:10.271947

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "26917509c474"
down_revision: Union[str, None] = "12ce231326e8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # set columns `year` and `source` not null
    op.alter_column("functional_zones_data", "year", nullable=False)
    op.alter_column("functional_zones_data", "source", nullable=False)
    op.alter_column("profiles_data", "year", nullable=False, schema="user_projects")
    op.alter_column("profiles_data", "source", nullable=False, schema="user_projects")


def downgrade() -> None:
    # set columns `year` and `source` nullable = true
    op.alter_column("functional_zones_data", "year", nullable=True)
    op.alter_column("functional_zones_data", "source", nullable=True)
    op.alter_column("profiles_data", "year", nullable=True, schema="user_projects")
    op.alter_column("profiles_data", "source", nullable=True, schema="user_projects")
