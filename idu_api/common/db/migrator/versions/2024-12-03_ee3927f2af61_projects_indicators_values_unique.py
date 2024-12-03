# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""projects indicators values unique

Revision ID: ee3927f2af61
Revises: ad9702dd19b7
Create Date: 2024-12-03 19:23:43.132926

"""
from textwrap import dedent
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "ee3927f2af61"
down_revision: Union[str, None] = "ad9702dd19b7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        sa.text(
            dedent(
                """
                WITH ranked_records AS (
                    SELECT 
                        indicator_value_id,
                        ROW_NUMBER() OVER (
                            PARTITION BY indicator_id, scenario_id, territory_id, hexagon_id
                            ORDER BY updated_at DESC
                        ) AS rnk
                    FROM user_projects.indicators_data
                )
                DELETE FROM user_projects.indicators_data
                WHERE indicator_value_id IN (
                    SELECT indicator_value_id
                    FROM ranked_records
                    WHERE rnk > 1
                );
                """
            )
        )
    )
    op.create_unique_constraint(
        "indicators_data_unique",
        "indicators_data",
        ["indicator_id", "scenario_id", "territory_id", "hexagon_id"],
        schema="user_projects",
    )


def downgrade() -> None:
    op.drop_constraint("indicators_data_unique", "indicators_data", schema="user_projects")
