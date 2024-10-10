# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""fix level autoset for public.territories_data

Revision ID: 74736f898ad6
Revises: dfb61815581e
Create Date: 2024-08-29 12:17:32.562240

"""
from textwrap import dedent
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "74736f898ad6"
down_revision: Union[str, None] = "dfb61815581e"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # create trigger function

    op.execute(
        sa.text(
            dedent(
                """
                CREATE FUNCTION public.trigger_update_inner_data_on_insert()
                RETURNS trigger
                LANGUAGE plpgsql
                AS $function$
                BEGIN
                    NEW.level = (
                        SELECT COALESCE(level, 0) + 1
                        FROM territories_data
                        WHERE territory_id = NEW.parent_id
                    );

                    RETURN NEW;
                END;
                $function$;
                """
            )
        )
    )

    # create triggers

    op.execute(
        sa.text(
            dedent(
                """
                CREATE TRIGGER update_inner_data_on_insert_trigger
                BEFORE INSERT ON public.territories_data
                FOR EACH ROW
                EXECUTE PROCEDURE public.trigger_update_inner_data_on_insert();
                """
            )
        )
    )

    # alter previous trigger

    op.execute(
        sa.text(
            dedent(
                """
                DROP TRIGGER IF EXISTS update_inner_data_on_update_trigger
                ON public.territories_data;
                """
            )
        )
    )

    op.execute(
        sa.text(
            dedent(
                """
                CREATE TRIGGER update_inner_data_on_update_trigger
                BEFORE UPDATE ON public.territories_data
                FOR EACH ROW
                EXECUTE PROCEDURE public.trigger_update_inner_data_on_update();
                """
            )
        )
    )


def downgrade() -> None:
    op.execute(
        sa.text(
            dedent(
                """
                DROP TRIGGER IF EXISTS update_inner_data_on_update_trigger
                ON public.territories_data;
                """
            )
        )
    )
    op.execute(
        sa.text(
            dedent(
                """
                CREATE TRIGGER update_inner_data_on_update_trigger
                BEFORE INSERT OR UPDATE ON public.territories_data
                FOR EACH ROW
                EXECUTE PROCEDURE public.trigger_update_inner_data_on_update();
                """
            )
        )
    )

    op.execute("DROP TRIGGER IF EXISTS update_inner_data_on_insert_trigger ON public.territories_data")
    op.execute("DROP FUNCTION IF EXISTS public.trigger_update_inner_data_on_insert")
