# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""fix territories data

Revision ID: 39fd8a1c52c1
Revises: 21b28a319b43
Create Date: 2024-11-08 16:08:06.868102

"""
from textwrap import dedent
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "39fd8a1c52c1"
down_revision: Union[str, None] = "21b28a319b43"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("territories_data", sa.Column("is_city", sa.Boolean(), server_default=sa.false(), nullable=False))

    op.execute(
        sa.text(
            dedent(
                """
                UPDATE territories_data
                SET is_city = TRUE
                WHERE properties @> '{"was_point": true}'
                """
            )
        )
    )

    op.execute(
        sa.text(
            dedent(
                """
                UPDATE territories_data
                SET properties = properties - 'was_point'
                WHERE properties ? 'was_point'
                """
            )
        )
    )

    op.create_foreign_key(
        "territories_data_fk_admin_center__territories_data",
        "territories_data",
        "territories_data",
        ["admin_center"],
        ["territory_id"],
        ondelete="SET NULL",
    )

    op.execute(sa.text("UPDATE territories_data SET admin_center = NULL"))


def downgrade() -> None:
    op.drop_constraint("territories_data_fk_admin_center__territories_data", "territories_data", "foreignkey")
    op.drop_column("territories_data", "is_city")
