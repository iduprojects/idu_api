# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""fix delete territory rules

Revision ID: 16b17cc8a1c0
Revises: 538f515c4b43
Create Date: 2025-02-14 12:24:28.403969

"""
from textwrap import dedent
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "16b17cc8a1c0"
down_revision: Union[str, None] = "538f515c4b43"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # fix delete trigger
    op.execute(
        sa.text(
            dedent(
                """
                CREATE OR REPLACE FUNCTION public.trigger_update_inner_data_on_delete()
                RETURNS trigger
                LANGUAGE plpgsql
                AS $function$
                BEGIN
                    UPDATE public.territories_data
                    SET parent_id = OLD.parent_id
                    WHERE parent_id = OLD.territory_id;

                    UPDATE public.object_geometries_data
                    SET territory_id = OLD.parent_id
                    WHERE territory_id = OLD.territory_id;
                    
                    UPDATE user_projects.object_geometries_data
                    SET territory_id = OLD.parent_id
                    WHERE territory_id = OLD.territory_id;
                    
                    UPDATE public.functional_zones_data
                    SET territory_id = OLD.parent_id
                    WHERE territory_id = OLD.territory_id;

                    RETURN OLD;
                END;
                $function$;
                """
            )
        )
    )

    # fix constraints
    op.drop_constraint(
        "territory_indicators_data_fk_territory_id__territories_data", "territory_indicators_data", type_="foreignkey"
    )
    op.create_foreign_key(
        "territory_indicators_data_fk_territory_id__territories_data",
        "territory_indicators_data",
        "territories_data",
        ["territory_id"],
        ["territory_id"],
        ondelete="CASCADE",
    )

def downgrade() -> None:
    # revert trigger
    op.execute(
        sa.text(
            dedent(
                """
                CREATE OR REPLACE FUNCTION public.trigger_update_inner_data_on_delete()
                RETURNS trigger
                LANGUAGE plpgsql
                AS $function$
                BEGIN
                    UPDATE territories_data
                    SET parent_id = OLD.parent_id
                    WHERE parent_id = OLD.territory_id;

                    UPDATE object_geometries_data
                    SET territory_id = OLD.parent_id
                    WHERE territory_id = OLD.territory_id;

                    RETURN OLD;
                END;
                $function$;
                """
            )
        )
    )

    # revert constraints
    op.drop_constraint(
        "territory_indicators_data_fk_territory_id__territories_data", "territory_indicators_data", type_="foreignkey"
    )
    op.create_foreign_key(
        "territory_indicators_data_fk_territory_id__territories_data",
        "territory_indicators_data",
        "territories_data",
        ["territory_id"],
        ["territory_id"],
    )
