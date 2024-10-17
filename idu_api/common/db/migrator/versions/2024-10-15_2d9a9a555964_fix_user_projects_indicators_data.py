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
    op.execute(sa.text("ALTER TYPE user_projects.date_field_type ADD VALUE IF NOT EXISTS 'year';"))
    op.execute(sa.text("ALTER TYPE user_projects.date_field_type ADD VALUE IF NOT EXISTS 'half_year';"))
    op.execute(sa.text("ALTER TYPE user_projects.date_field_type ADD VALUE IF NOT EXISTS 'quarter';"))
    op.execute(sa.text("ALTER TYPE user_projects.date_field_type ADD VALUE IF NOT EXISTS 'month';"))
    op.execute(sa.text("ALTER TYPE user_projects.date_field_type ADD VALUE IF NOT EXISTS 'day';"))

    op.execute(sa.text("CREATE TYPE user_projects.indicator_value_type AS ENUM ('real', 'forecast', 'target');"))

    op.alter_column(
        "indicators_data",
        "date_type",
        existing_type=sa.Enum(
            "year", "half_year", "quarter", "month", "day", name="date_field_type", inherit_schema=True
        ),
        nullable=False,
        schema="user_projects",
    )

    op.add_column(
        "indicators_data",
        sa.Column(
            "value_type",
            sa.Enum("real", "forecast", "target", name="indicator_value_type", schema="user_projects"),
            nullable=False,
        ),
        schema="user_projects",
    )

    op.add_column(
        "indicators_data",
        sa.Column("information_source", sa.String(length=300), nullable=True),
        schema="user_projects",
    )


def downgrade() -> None:
    op.drop_column(
        "indicators_data",
        "information_source",
        schema="user_projects",
    )

    op.drop_column(
        "indicators_data",
        "value_type",
        schema="user_projects",
    )

    op.alter_column(
        "indicators_data",
        "date_type",
        existing_type=sa.Enum(name="date_field_type", schema="user_projects"),
        nullable=False,
        schema="user_projects",
    )

    op.execute(sa.text("DROP TYPE user_projects.indicator_value_type;"))
