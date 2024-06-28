# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""fix normatives schema

Revision ID: fb7133dc5105
Revises: 5b2ffe0484de
Create Date: 2024-06-06 11:52:43.225415

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "fb7133dc5105"
down_revision: Union[str, None] = "5b2ffe0484de"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # columns

    op.alter_column("service_types_normatives_data", "service_type_id", existing_type=sa.INTEGER(), nullable=True)
    op.alter_column("service_types_normatives_data", "urban_function_id", existing_type=sa.INTEGER(), nullable=True)

    # constraints

    op.drop_constraint(
        "service_types_normatives_data_service_type_id_urban_fun_df4a", "service_types_normatives_data", type_="unique"
    )
    op.create_unique_constraint(
        "service_types_normatives_data_service_type_territory_key",
        "service_types_normatives_data",
        ["service_type_id", "territory_id"],
    )
    op.create_unique_constraint(
        "service_types_normatives_data_urban_func_territory_key",
        "service_types_normatives_data",
        ["urban_function_id", "territory_id"],
    )


def downgrade() -> None:
    # columns

    op.alter_column("service_types_normatives_data", "urban_function_id", existing_type=sa.INTEGER(), nullable=False)
    op.alter_column("service_types_normatives_data", "service_type_id", existing_type=sa.INTEGER(), nullable=False)

    # constraints

    op.drop_constraint(
        "service_types_normatives_data_urban_func_territory_key", "service_types_normatives_data", type_="unique"
    )
    op.drop_constraint(
        "service_types_normatives_data_service_type_territory_key", "service_types_normatives_data", type_="unique"
    )
    op.create_unique_constraint(
        "service_types_normatives_data_service_type_id_urban_fun_df4a",
        "service_types_normatives_data",
        ["service_type_id", "urban_function_id", "territory_id"],
    )
