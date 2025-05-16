# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""rename soc_group_value_indicators_data -> soc_value_indicators_data

Revision ID: 20aa94c13267
Revises: 9028170b5b86
Create Date: 2025-05-16 20:33:31.186651

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "20aa94c13267"
down_revision: Union[str, None] = "9028170b5b86"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.rename_table("soc_group_value_indicators_data", "soc_value_indicators_data")


def downgrade() -> None:
    op.rename_table("soc_value_indicators_data", "soc_group_value_indicators_data")
