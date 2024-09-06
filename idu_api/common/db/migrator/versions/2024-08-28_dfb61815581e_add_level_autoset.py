# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""add level autoset for public.territories_data

Revision ID: dfb61815581e
Revises: 7c1977523140
Create Date: 2024-08-28 12:54:25.789593

"""
from textwrap import dedent
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "dfb61815581e"
down_revision: Union[str, None] = "7c1977523140"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # create trigger functions

    op.execute(
        sa.text(
            dedent(
                """
                CREATE FUNCTION public.trigger_update_inner_data_on_delete()
                RETURNS trigger
                LANGUAGE plpgsql
                AS $function$
                BEGIN
                    UPDATE territories_data
                    SET
                        parent_id = OLD.parent_id,
                        level = OLD.level
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

    op.execute(
        sa.text(
            dedent(
                """
                CREATE FUNCTION public.trigger_update_inner_data_on_update()
                RETURNS trigger
                LANGUAGE plpgsql
                AS $function$
                BEGIN
                    IF NEW.level <> OLD.level THEN
                        UPDATE territories_data SET level = COALESCE(NEW.level, 0) + 1
                        WHERE parent_id = NEW.territory_id;
                    END IF;

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
                CREATE TRIGGER update_inner_data_on_delete_trigger
                BEFORE DELETE ON public.territories_data
                FOR EACH ROW
                EXECUTE PROCEDURE public.trigger_update_inner_data_on_delete();
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


def downgrade() -> None:
    # delete triggers

    for table_name, trigger_name in (
        ("public.territories_data", "update_inner_data_on_delete_trigger"),
        ("public.territories_data", "update_inner_data_on_update_trigger"),
    ):
        op.execute(f"DROP TRIGGER IF EXISTS {trigger_name} ON {table_name}")

    # delete functions

    for function_name in (
        "public.trigger_update_inner_data_on_delete",
        "public.trigger_update_inner_data_on_update",
    ):
        op.execute(f"DROP FUNCTION IF EXISTS {function_name}")
