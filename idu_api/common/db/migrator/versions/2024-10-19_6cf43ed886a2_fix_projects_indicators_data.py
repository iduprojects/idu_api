# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""fix projects indicators data

Revision ID: 6cf43ed886a2
Revises: 737f3c396010
Create Date: 2024-10-19 19:17:07.752870

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "6cf43ed886a2"
down_revision: Union[str, None] = "737f3c396010"
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

    op.create_primary_key(
        "indicators_data_pk",
        "indicators_data",
        ["indicator_id", "scenario_id", "date_type", "date_value", "value_type", "information_source"],
        schema="user_projects",
    )


def downgrade() -> None:
    op.drop_constraint("indicators_data_pk", "indicators_data", schema="user_projects")

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
