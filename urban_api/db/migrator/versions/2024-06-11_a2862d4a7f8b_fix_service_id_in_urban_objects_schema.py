# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""fix service_id in urban_objects schema

Revision ID: a2862d4a7f8b
Revises: fb7133dc5105
Create Date: 2024-06-11 13:01:34.020752

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "a2862d4a7f8b"
down_revision: Union[str, None] = "fb7133dc5105"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # columns

    op.alter_column("urban_objects_data", "service_id", existing_type=sa.INTEGER(), nullable=True)


def downgrade() -> None:
    # columns

    op.alter_column("urban_objects_data", "service_id", existing_type=sa.INTEGER(), nullable=False)
