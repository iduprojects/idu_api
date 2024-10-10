# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""fix territories level autoset

Revision ID: c15cfb20553f
Revises: a9c7320e2115
Create Date: 2024-10-10 13:42:06.065967

"""
from textwrap import dedent
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "c15cfb20553f"
down_revision: Union[str, None] = "a9c7320e2115"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # delete triggers

    for table_name, trigger_name in (
        ("public.territories_data", "update_inner_data_on_delete_trigger"),
        ("public.territories_data", "update_inner_data_on_update_trigger"),
        ("public.territories_data", "update_inner_data_on_insert_trigger"),
    ):
        op.execute(f"DROP TRIGGER IF EXISTS {trigger_name} ON {table_name}")

    # delete functions

    for function_name in (
        "public.trigger_update_inner_data_on_delete",
        "public.trigger_update_inner_data_on_update",
        "public.trigger_update_inner_data_on_insert",
    ):
        op.execute(f"DROP FUNCTION IF EXISTS {function_name}")

    # create functions to auto-set territories level on insert/update
    op.execute(
        sa.text(
            dedent(
                """
                CREATE FUNCTION public.trigger_update_inner_data_on_insert()
                RETURNS trigger
                LANGUAGE plpgsql
                AS $function$
                BEGIN
                    NEW.level := (
                        SELECT COALESCE(
                            (SELECT level
                            FROM territories_data
                            WHERE territory_id = NEW.parent_id), 0) + 1
                    );
                    RETURN NEW;
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
                        UPDATE territories_data 
                        SET level = NEW.level + 1           
                        WHERE parent_id = NEW.territory_id;
                    END IF;

                    IF NEW.parent_id <> OLD.parent_id OR 
                    NEW.parent_id IS NULL AND OLD.parent_id IS NOT NULL OR
                    NEW.parent_id IS NOT NULL AND OLD.parent_id IS NULL
                    THEN
                        UPDATE territories_data 
                        SET level = (
                            SELECT COALESCE(
                                (SELECT level
                                FROM territories_data
                                WHERE territory_id = NEW.parent_id), 0) + 1
                        )                  
                        WHERE territory_id = NEW.territory_id;
                    END IF;
                    RETURN NEW;
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
                CREATE FUNCTION public.trigger_update_inner_data_on_delete()
                RETURNS trigger
                LANGUAGE plpgsql
                AS $function$
                BEGIN
                    UPDATE territories_data
                    SET
                        parent_id = OLD.parent_id
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

    # create triggers to auto-set territories level on insert/update
    op.execute(
        sa.text(
            dedent(
                """
                CREATE TRIGGER update_inner_data_on_insert_trigger
                BEFORE INSERT ON public.territories_data
                FOR EACH ROW
                EXECUTE FUNCTION public.trigger_update_inner_data_on_insert();
                """
            )
        )
    )
    op.execute(
        sa.text(
            dedent(
                """
                CREATE TRIGGER update_inner_data_on_update_trigger
                AFTER UPDATE ON public.territories_data
                FOR EACH ROW
                EXECUTE FUNCTION public.trigger_update_inner_data_on_update();
                """
            )
        )
    )
    op.execute(
        sa.text(
            dedent(
                """
                CREATE TRIGGER update_inner_data_on_delete_trigger
                BEFORE DELETE ON public.territories_data
                FOR EACH ROW
                EXECUTE FUNCTION public.trigger_update_inner_data_on_delete();
                """
            )
        )
    )


def downgrade() -> None:
    # delete triggers

    for table_name, trigger_name in (
        ("public.territories_data", "update_inner_data_on_delete_trigger"),
        ("public.territories_data", "update_inner_data_on_update_trigger"),
        ("public.territories_data", "update_inner_data_on_insert_trigger"),
    ):
        op.execute(f"DROP TRIGGER IF EXISTS {trigger_name} ON {table_name}")

    # delete functions

    for function_name in (
        "public.trigger_update_inner_data_on_delete",
        "public.trigger_update_inner_data_on_update",
        "public.trigger_update_inner_data_on_insert",
    ):
        op.execute(f"DROP FUNCTION IF EXISTS {function_name}")

    # create functions
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
                BEFORE UPDATE ON public.territories_data
                FOR EACH ROW
                EXECUTE PROCEDURE public.trigger_update_inner_data_on_update();
                """
            )
        )
    )
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
