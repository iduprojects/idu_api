# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""po functions not null

Revision ID: 60365c39941e
Revises: 1a5a48e94edf
Create Date: 2024-10-22 11:26:42.604032

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "60365c39941e"
down_revision: Union[str, None] = "1a5a48e94edf"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # set not null for column `physical_object_function_id`
    op.alter_column("physical_object_types_dict", "physical_object_function_id", nullable=False)


def downgrade() -> None:
    op.alter_column("physical_object_types_dict", "physical_object_function_id", nullable=True)
