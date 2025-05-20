# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""fix soc value float fields

Revision ID: e519990b4b4f
Revises: 3af33ae762b1
Create Date: 2025-05-20 17:16:32.013118

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "e519990b4b4f"
down_revision: Union[str, None] = "3af33ae762b1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column("soc_values_dict", "normative_value", type_=sa.Float(precision=53))
    op.alter_column("soc_values_dict", "decree_value", type_=sa.Float(precision=53))


def downgrade() -> None:
    op.alter_column("soc_values_dict", "normative_value", type_=sa.Float(precision=10, asdecimal=2))
    op.alter_column("soc_values_dict", "decree_value", type_=sa.Float(precision=10, asdecimal=2))
