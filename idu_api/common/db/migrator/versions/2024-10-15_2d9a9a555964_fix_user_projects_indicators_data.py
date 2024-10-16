# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""fix user_projects.indicators_data

Revision ID: 2d9a9a555964
Revises: c64e20abd7c2
Create Date: 2024-10-15 12:53:41.670635

"""
from textwrap import dedent
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "2d9a9a555964"
down_revision: Union[str, None] = "c64e20abd7c2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        sa.text(
            dedent(
                """
                ALTER TYPE user_projects.date_field_type ADD VALUE IF NOT EXISTS 'year';
                """
            )
        )
    )
    op.execute(
        sa.text(
            dedent(
                """
                ALTER TYPE user_projects.date_field_type ADD VALUE IF NOT EXISTS 'half_year';
                """
            )
        )
    )
    op.execute(
        sa.text(
            dedent(
                """
                ALTER TYPE user_projects.date_field_type ADD VALUE IF NOT EXISTS 'quarter';

                """
            )
        )
    )
    op.execute(
        sa.text(
            dedent(
                """
                ALTER TYPE user_projects.date_field_type ADD VALUE IF NOT EXISTS 'month';
                """
            )
        )
    )
    op.execute(
        sa.text(
            dedent(
                """
                ALTER TYPE user_projects.date_field_type ADD VALUE IF NOT EXISTS 'day';
                """
            )
        )
    )

    op.alter_column(
        "indicators_data",
        "date_type",
        existing_type=sa.Enum(
            "year", "half_year", "quarter", "month", "day", name="date_field_type", inherit_schema=True
        ),
        nullable=False,
        schema="user_projects",
    )


def downgrade() -> None:
    op.alter_column(
        "indicators_data",
        "date_type",
        existing_type=sa.Enum(name="date_field_type", inherit_schema=True),
        nullable=False,
        schema="user_projects",
    )
