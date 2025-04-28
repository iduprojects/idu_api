# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""soc values indicators

Revision ID: 3af33ae762b1
Revises: 20aa94c13267
Create Date: 2025-05-19 14:14:26.065231

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "3af33ae762b1"
down_revision: Union[str, None] = "afce48f863ca"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # add rank, normative_value, decree_value to soc_values_dict
    op.add_column("soc_values_dict", sa.Column("rank", sa.Integer(), nullable=True))
    op.add_column("soc_values_dict", sa.Column("normative_value", sa.Float(10, 2), nullable=True))
    op.add_column(
        "soc_values_dict",
        sa.Column("decree_value", sa.Float(10, 2), nullable=True, comment="meaning of the archetype by decree"),
    )

    # remove pk from soc_group_indicators_data
    op.drop_constraint("soc_group_values_indicators_data_pk", "soc_group_value_indicators_data", type_="primary")

    # remove soc_group_id, value_type columns from soc_group_indicators_data
    op.drop_column("soc_group_value_indicators_data", "soc_group_id")
    op.drop_column("soc_group_value_indicators_data", "value_type")

    # create new pk in soc_group_indicators_data
    op.create_primary_key(
        "soc_group_values_indicators_data_pk",
        "soc_group_value_indicators_data",
        ["soc_value_id", "territory_id", "year"],
    )

    # create soc_values_service_types_dict table
    op.create_table(
        "soc_values_service_types_dict",
        sa.Column("soc_value_id", sa.Integer(), nullable=False),
        sa.Column("service_type_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["service_type_id"],
            ["service_types_dict.service_type_id"],
            name=op.f("soc_values_service_types_dict_fk_service_type_id__service_types_dict"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["soc_value_id"],
            ["soc_values_dict.soc_value_id"],
            name=op.f("soc_values_service_types_dict_fk_soc_value_id__soc_values_dict"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("soc_value_id", "service_type_id", name=op.f("soc_values_service_types_dict_pk")),
    )

    op.rename_table("soc_group_value_indicators_data", "soc_value_indicators_data")


def downgrade() -> None:
    op.rename_table("soc_value_indicators_data", "soc_group_value_indicators_data")

    # drop soc_values_service_types_dict table
    op.drop_table("soc_values_service_types_dict")

    # revert changes for soc_group_value_indicators_data
    op.drop_constraint("soc_group_values_indicators_data_pk", "soc_group_value_indicators_data", type_="primary")

    op.add_column(
        "soc_group_value_indicators_data", sa.Column("soc_group_id", sa.INTEGER(), autoincrement=False, nullable=True)
    )
    op.add_column(
        "soc_group_value_indicators_data",
        sa.Column("value_type", sa.VARCHAR(length=50), autoincrement=False, nullable=True),
    )

    op.create_primary_key(
        "soc_group_values_indicators_data_pk",
        "soc_group_value_indicators_data",
        ["soc_group_id", "soc_value_id", "year", "value_type"],
    )

    # revert adding new columns in soc_values_dict
    op.drop_column("soc_values_dict", "decree_value")
    op.drop_column("soc_values_dict", "normative_value")
    op.drop_column("soc_values_dict", "rank")
