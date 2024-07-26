# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""fix territory indicators schema

Revision ID: f2982fcbb0c6
Revises: 0e4c4b88e28c
Create Date: 2024-07-26 09:12:27.664009

"""
from textwrap import dedent
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "f2982fcbb0c6"
down_revision: Union[str, None] = "0e4c4b88e28c"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # data

    op.execute(
        sa.text(
            dedent(
                """
                UPDATE territory_indicators_data 
                SET information_source = 'Unknown Source' 
                WHERE information_source IS NULL
                """
            )
        )
    )

    # columns

    op.alter_column(
        "territory_indicators_data", "information_source", existing_type=sa.VARCHAR(length=300), nullable=False
    )

    # constraints

    op.drop_constraint("territory_indicators_data_pk", "territory_indicators_data", type_="primary")
    op.create_primary_key(
        "territory_indicators_data_pk",
        "territory_indicators_data",
        ["indicator_id", "territory_id", "date_type", "date_value", "value_type", "information_source"],
    )


def downgrade() -> None:
    # data

    op.execute(
        sa.text(
            dedent(
                """
                WITH RankedData AS (
                    SELECT 
                        indicator_id, 
                        territory_id, 
                        date_type, 
                        date_value, 
                        value_type, 
                        information_source,
                        ROW_NUMBER() OVER (
                            PARTITION BY indicator_id, territory_id, date_type, date_value
                            ORDER BY 
                                CASE 
                                    WHEN value_type = 'real' THEN 1
                                    WHEN value_type = 'target' THEN 2
                                    ELSE 3 
                                END, 
                                (SELECT NULL) -- Ensure deterministic order in case of ties
                        ) AS rn
                    FROM territory_indicators_data
                )
                DELETE FROM territory_indicators_data
                WHERE (indicator_id, territory_id, date_type, date_value, value_type, information_source) NOT IN (
                    SELECT indicator_id, territory_id, date_type, date_value, value_type, information_source
                    FROM RankedData
                    WHERE rn = 1
                );
                """
            )
        )
    )

    # constraints

    op.drop_constraint("territory_indicators_data_pk", "territory_indicators_data", type_="primary")

    op.create_primary_key(
        "territory_indicators_data_pk",
        "territory_indicators_data",
        ["indicator_id", "territory_id", "date_type", "date_value"],
    )

    # columns

    op.alter_column(
        "territory_indicators_data", "information_source", existing_type=sa.VARCHAR(length=300), nullable=True
    )
