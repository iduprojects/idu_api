# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""hexagons indicators delete rule

Revision ID: f634ff858553
Revises: f6e7924d483f
Create Date: 2024-12-14 17:37:39.009140

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "f634ff858553"
down_revision: Union[str, None] = "f6e7924d483f"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_constraint(
        "indicators_data_fk_hexagon_id__hexagons_data",
        "indicators_data",
        schema="user_projects",
    )
    op.create_foreign_key(
        "indicators_data_fk_hexagon_id__hexagons_data",
        "indicators_data",
        "hexagons_data",
        ["hexagon_id"],
        ["hexagon_id"],
        source_schema="user_projects",
        referent_schema="user_projects",
        ondelete="CASCADE",
    )


def downgrade() -> None:
    op.drop_constraint(
        "indicators_data_fk_hexagon_id__hexagons_data",
        "indicators_data",
        schema="user_projects",
    )
    op.create_foreign_key(
        "indicators_data_fk_hexagon_id__hexagons_data",
        "indicators_data",
        "hexagons_data",
        ["hexagon_id"],
        ["hexagon_id"],
        source_schema="user_projects",
        referent_schema="user_projects",
    )
