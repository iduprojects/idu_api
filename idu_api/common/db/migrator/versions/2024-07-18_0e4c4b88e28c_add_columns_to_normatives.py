# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""add columns to normatives

Revision ID: 0e4c4b88e28c
Revises: 74ca0cb0643c
Create Date: 2024-07-18 12:29:11.393323

"""
from textwrap import dedent
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0e4c4b88e28c"
down_revision: Union[str, None] = "74ca0cb0643c"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # columns

    op.add_column("service_types_normatives_data", sa.Column("source", sa.String(length=300), nullable=True))
    op.add_column("service_types_normatives_data", sa.Column("year", sa.Integer(), nullable=True))
    op.add_column(
        "service_types_normatives_data",
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
    )
    op.add_column(
        "service_types_normatives_data",
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
    )

    # data

    op.execute("UPDATE service_types_normatives_data SET year = 2024")

    # columns

    op.alter_column("service_types_normatives_data", "year", nullable=False)

    # constraints

    op.drop_constraint(
        "service_types_normatives_data_service_type_territory_key", "service_types_normatives_data", type_="unique"
    )
    op.create_unique_constraint(
        "service_types_normatives_data_service_type_territory_key",
        "service_types_normatives_data",
        ["service_type_id", "territory_id", "year"],
    )
    op.drop_constraint(
        "service_types_normatives_data_urban_func_territory_key", "service_types_normatives_data", type_="unique"
    )
    op.create_unique_constraint(
        "service_types_normatives_data_urban_func_territory_key",
        "service_types_normatives_data",
        ["urban_function_id", "territory_id", "year"],
    )


def downgrade() -> None:
    # data

    op.execute(
        sa.text(
            dedent(
                """
                WITH duplicates AS (
                    SELECT
                        normative_id,
                        ROW_NUMBER() OVER (
                            PARTITION BY service_type_id, urban_function_id, territory_id
                            ORDER BY year DESC, normative_id
                        ) AS row_num
                    FROM service_types_normatives_data
                    WHERE
                        (service_type_id IS NOT NULL OR urban_function_id IS NOT NULL) AND
                        (service_type_id IS NULL OR urban_function_id IS NULL) AND
                        (territory_id IS NOT NULL OR territory_id IS NULL)
                )
                DELETE FROM service_types_normatives_data
                USING duplicates
                WHERE service_types_normatives_data.normative_id = duplicates.normative_id
                AND duplicates.row_num > 1;
                """
            )
        )
    )

    # constraints

    op.drop_constraint(
        "service_types_normatives_data_urban_func_territory_key", "service_types_normatives_data", type_="unique"
    )
    op.create_unique_constraint(
        "service_types_normatives_data_urban_func_territory_key",
        "service_types_normatives_data",
        ["urban_function_id", "territory_id"],
    )
    op.drop_constraint(
        "service_types_normatives_data_service_type_territory_key", "service_types_normatives_data", type_="unique"
    )
    op.create_unique_constraint(
        "service_types_normatives_data_service_type_territory_key",
        "service_types_normatives_data",
        ["service_type_id", "territory_id"],
    )

    # columns

    op.drop_column("service_types_normatives_data", "updated_at")
    op.drop_column("service_types_normatives_data", "created_at")
    op.drop_column("service_types_normatives_data", "year")
    op.drop_column("service_types_normatives_data", "source")
