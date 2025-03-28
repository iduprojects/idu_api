# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""social groups

Revision ID: aed1e90268cb
Revises: 0e4fb278f105
Create Date: 2025-03-21 16:48:33.603541

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "aed1e90268cb"
down_revision: Union[str, None] = "0e4fb278f105"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # create `soc_groups_dict` table
    op.execute(sa.schema.CreateSequence(sa.Sequence("soc_groups_dict_id_seq")))
    op.create_table(
        "soc_groups_dict",
        sa.Column(
            "soc_group_id",
            sa.Integer(),
            server_default=sa.text("nextval('soc_groups_dict_id_seq')"),
            nullable=False,
        ),
        sa.Column("name", sa.String(length=200), nullable=False, unique=True),
        sa.PrimaryKeyConstraint("soc_group_id", name=op.f("soc_groups_dict_pk")),
    )

    # create `soc_values_dict` table
    op.execute(sa.schema.CreateSequence(sa.Sequence("soc_values_dict_id_seq")))
    op.create_table(
        "soc_values_dict",
        sa.Column(
            "soc_value_id",
            sa.Integer(),
            server_default=sa.text("nextval('soc_values_dict_id_seq')"),
            nullable=False,
        ),
        sa.Column("name", sa.String(length=200), nullable=False, unique=True),
        sa.PrimaryKeyConstraint("soc_value_id", name=op.f("soc_values_dict_pk")),
    )

    # create association `soc_group_values_data` table
    op.execute(sa.schema.CreateSequence(sa.Sequence("soc_group_values_data_id_seq")))
    op.create_table(
        "soc_group_values_data",
        sa.Column(
            "soc_group_value_id",
            sa.Integer(),
            server_default=sa.text("nextval('soc_group_values_data_id_seq')"),
            nullable=False,
        ),
        sa.Column("soc_group_id", sa.Integer(), nullable=False),
        sa.Column("service_type_id", sa.Integer(), nullable=False),
        sa.Column("soc_value_id", sa.Integer(), nullable=True),
        sa.Column(
            "infrastructure_type",
            postgresql.ENUM(name="infrastructure_type", create_type=False),
            default="basic",
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["soc_group_id"],
            ["soc_groups_dict.soc_group_id"],
            name=op.f("soc_groups_values_data_fk_soc_group_id__soc_groups_dict"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["service_type_id"],
            ["service_types_dict.service_type_id"],
            name=op.f("service_types_dict_fk_service_type_id__service_types_dict"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["soc_value_id"],
            ["soc_values_dict.soc_value_id"],
            name=op.f("soc_groups_values_data_fk_soc_value_id__soc_values_dict"),
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint("soc_group_id", "service_type_id", "soc_value_id", name=op.f("soc_values_data_unique_key")),
        sa.PrimaryKeyConstraint("soc_group_value_id", name=op.f("soc_groups_values_data_pk")),
    )

    # add primary key `indicator_value_id` for `territory_indicators_data` table
    op.execute(sa.schema.CreateSequence(sa.Sequence("territory_indicators_data_id_seq")))
    op.add_column(
        "territory_indicators_data",
        sa.Column(
            "indicator_value_id",
            sa.Integer(),
            server_default=sa.text("nextval('territory_indicators_data_id_seq')"),
            nullable=False,
        ),
    )
    op.drop_constraint("territory_indicators_data_pk", "territory_indicators_data")
    op.create_unique_constraint(
        "territory_indicators_data_unique_key",
        "territory_indicators_data",
        ["indicator_id", "territory_id", "date_type", "date_value", "value_type", "information_source"],
    )
    op.create_primary_key("territory_indicators_data_pk", "territory_indicators_data", ["indicator_value_id"])

    # create `soc_group_value_indicators_data` table
    op.create_table(
        "soc_group_value_indicators_data",
        sa.Column("soc_group_id", sa.Integer(), nullable=False),
        sa.Column("soc_value_id", sa.Integer(), nullable=False),
        sa.Column("territory_id", sa.Integer(), nullable=False),
        sa.Column("year", sa.Integer(), nullable=False),
        sa.Column("value", sa.Float(precision=53), nullable=False),
        sa.Column("value_type", postgresql.ENUM(name="indicator_value_type", create_type=False), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(
            ["soc_group_id"],
            ["soc_groups_dict.soc_group_id"],
            name=op.f("soc_group_values_indicators_data_fk_soc_group_id"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["soc_value_id"],
            ["soc_values_dict.soc_value_id"],
            name=op.f("soc_group_values_indicators_data_fk_soc_value_id"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["territory_id"],
            ["territories_data.territory_id"],
            name=op.f("soc_group_values_indicators_data_fk_territory_id"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint(
            "soc_group_id",
            "soc_value_id",
            "territory_id",
            "value_type",
            "year",
            name=op.f("soc_group_values_indicators_data_pk"),
        ),
    )


def downgrade() -> None:
    # drop all new tables
    op.drop_table("soc_group_value_indicators_data")
    op.drop_table("soc_group_values_data")
    op.drop_table("soc_values_dict")
    op.drop_table("soc_groups_dict")
    op.execute(sa.schema.DropSequence(sa.Sequence("soc_group_values_data_id_seq")))
    op.execute(sa.schema.DropSequence(sa.Sequence("soc_groups_dict_id_seq")))
    op.execute(sa.schema.DropSequence(sa.Sequence("soc_values_dict_id_seq")))

    # revert changes from `territory_indicators_data` table
    op.drop_constraint("territory_indicators_data_pk", "territory_indicators_data")
    op.drop_constraint(
        "territory_indicators_data_unique_key",
        "territory_indicators_data",
    )
    op.create_primary_key(
        "territory_indicators_data_pk",
        "territory_indicators_data",
        ["indicator_id", "territory_id", "date_type", "date_value", "value_type", "information_source"],
    )
    op.drop_column("territory_indicators_data", "indicator_value_id")
    op.execute(sa.schema.DropSequence(sa.Sequence("territory_indicators_data_id_seq")))
