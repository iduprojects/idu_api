# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""target_city_types_dict and city-projects

Revision ID: 538f515c4b43
Revises: a3928e8d60eb
Create Date: 2025-02-07 12:33:53.751432

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "538f515c4b43"
down_revision: Union[str, None] = "a3928e8d60eb"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # create table `public.target_city_types_dict`
    op.execute(sa.schema.CreateSequence(sa.Sequence("target_city_types_dict_id_seq")))
    op.create_table(
        "target_city_types_dict",
        sa.Column(
            "target_city_type_id",
            sa.Integer(),
            server_default=sa.text("nextval('target_city_types_dict_id_seq')"),
            nullable=False,
        ),
        sa.Column("name", sa.String(length=200), nullable=False, unique=True),
        sa.Column("description", sa.String(length=2048), nullable=False),
        sa.PrimaryKeyConstraint("target_city_type_id", name=op.f("target_city_types_dict_pk")),
    )

    # add column `target_city_id` to `public.territories_data` table
    op.add_column("territories_data", sa.Column("target_city_type_id", sa.Integer(), nullable=True))
    op.create_foreign_key(
        "territories_data_fk_target_city_type_id__target_cities_dict",
        "territories_data",
        "target_city_types_dict",
        ["target_city_type_id"],
        ["target_city_type_id"],
    )

    # add column `is_city` to `user_projects.projects_data` table
    op.add_column(
        "projects_data",
        sa.Column("is_city", sa.Boolean(), server_default=sa.false(), nullable=False),
        schema="user_projects",
    )


def downgrade() -> None:
    # drop column `target_city_id` from `public.territories_data` table
    op.drop_constraint(
        "territories_data_fk_target_city_type_id__target_cities_dict",
        "territories_data",
        type_="foreignkey",
    )
    op.drop_column("territories_data", "target_city_type_id")

    # drop table `public.target_city_types_dict`
    op.drop_table("target_city_types_dict")
    op.execute(sa.schema.DropSequence(sa.Sequence("target_city_types_dict_id_seq")))

    # drop column `is_city` from `user_projects.projects_data` table
    op.drop_column(
        "projects_data",
        "is_city",
        schema="user_projects",
    )
