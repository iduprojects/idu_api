# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""normative territory id nullable

Revision ID: db3b9d3503f4
Revises: 9cb5b596f01e
Create Date: 2024-07-09 14:20:16.813761

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "db3b9d3503f4"
down_revision: Union[str, None] = "9cb5b596f01e"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # columns
    op.alter_column("service_types_normatives_data", "territory_id", existing_type=sa.INTEGER(), nullable=True)


def downgrade() -> None:
    # columns
    op.alter_column("service_types_normatives_data", "territory_id", existing_type=sa.INTEGER(), nullable=False)
