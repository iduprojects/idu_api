# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""scenarios phase

Revision ID: afce48f863ca
Revises: aed1e90268cb
Create Date: 2025-05-14 16:10:25.520746

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "afce48f863ca"
down_revision: Union[str, None] = "aed1e90268cb"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

scenario_phase_enum = sa.Enum(
    "investment",
    "pre_design",
    "design",
    "construction",
    "operation",
    "decommission",
    name="scenario_phase",
    schema="user_projects",
)


def upgrade() -> None:
    # add columns `phase` and `phase_percentage` to `scenarios_data`
    scenario_phase_enum.create(op.get_bind(), checkfirst=True)
    op.add_column("scenarios_data", sa.Column("phase", scenario_phase_enum, nullable=True), schema="user_projects")
    op.add_column(
        "scenarios_data", sa.Column("phase_percentage", sa.Float(precision=3), nullable=True), schema="user_projects"
    )


def downgrade() -> None:
    # remove new columns from `scenarios_data`
    op.drop_column("scenarios_data", "phase", schema="user_projects")
    op.drop_column("scenarios_data", "phase_percentage", schema="user_projects")

    # Drop enum type
    scenario_phase_enum.drop(op.get_bind(), checkfirst=True)
