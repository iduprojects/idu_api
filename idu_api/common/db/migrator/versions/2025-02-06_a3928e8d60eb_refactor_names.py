# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""refactor names

Revision ID: a3928e8d60eb
Revises: f634ff858553
Create Date: 2025-02-06 13:54:52.934497

"""
from textwrap import dedent
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "a3928e8d60eb"
down_revision: Union[str, None] = "f634ff858553"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # rename `user_projects.profiles_data` to `user_projects.functional_zones_data`
    op.rename_table("profiles_data", "functional_zones_data", schema="user_projects")
    op.alter_column("functional_zones_data", "profile_id", new_column_name="functional_zone_id", schema="user_projects")
    op.execute(
        sa.text(
            dedent(
                """
                ALTER SEQUENCE user_projects.profiles_data_id_seq RENAME TO functional_zones_data_id_seq
                """
            )
        )
    )

    # rename `public.urban_functions_dict.parent_urban_function_id` to `public.urban_functions_dict.parent_id`
    op.alter_column("urban_functions_dict", "parent_urban_function_id", new_column_name="parent_id")

    # rename `public.territories_data.admin_center` to `public.territories_data.admin_center_id`
    op.alter_column("territories_data", "admin_center", new_column_name="admin_center_id")


def downgrade() -> None:
    # rename `user_projects.functional_zones_data` to `user_projects.profiles_data`
    op.rename_table("functional_zones_data", "profiles_data", schema="user_projects")
    op.alter_column("profiles_data", "functional_zone_id", new_column_name="profile_id", schema="user_projects")
    op.execute(
        sa.text(
            dedent(
                """
                ALTER SEQUENCE user_projects.functional_zones_data_id_seq RENAME TO profiles_data_id_seq
                """
            )
        )
    )

    # rename `public.urban_functions_dict.parent_id` to `public.urban_functions_dict.parent_urban_function_id`
    op.alter_column("urban_functions_dict", "parent_id", new_column_name="parent_urban_function_id")

    # rename `public.territories_data.admin_center_id` to `public.territories_data.admin_center`
    op.alter_column("territories_data", "admin_center_id", new_column_name="admin_center")
