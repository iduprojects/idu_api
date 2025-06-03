# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""service data null capacity handling

Revision ID: 676ef0e8411b
Revises: e519990b4b4f
Create Date: 2025-06-03 12:29:13.243421

"""
from textwrap import dedent
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "676ef0e8411b"
down_revision: Union[str, None] = "e519990b4b4f"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:

    # create handling null capacity function
    op.execute(
        sa.text(
            dedent(
                """
                CREATE OR REPLACE FUNCTION public.trigger_set_capacity_from_modeled()
                RETURNS trigger
                LANGUAGE plpgsql
                AS $function$
                BEGIN
                    IF NEW.capacity IS NULL THEN
                        NEW.capacity := (
                            SELECT capacity_modeled
                            FROM public.service_types_dict
                            WHERE service_type_id = NEW.service_type_id
                        );
                        NEW.is_capacity_real := FALSE;
                    END IF;
                    RETURN NEW;
                END;
                $function$;
                """
            )
        )
    )

    # create handling null capacity triggers
    for trigger_name, table_name, procedure_name in [
        (
                "handling_null_capacity_trigger",
                "public.services_data",
                "public.trigger_set_capacity_from_modeled",
        ),
        (
                "handling_null_capacity_trigger",
                "user_projects.services_data",
                "public.trigger_set_capacity_from_modeled",
        ),
    ]:
        op.execute(
            sa.text(
                dedent(
                    f"""
                    CREATE TRIGGER {trigger_name}
                    BEFORE INSERT OR UPDATE ON {table_name}
                    FOR EACH ROW
                    EXECUTE PROCEDURE {procedure_name}();
                    """
                )
            )
        )


def downgrade() -> None:

    # delete triggers
    for trigger_name, table_name in [
        ("handling_null_capacity_trigger", "public.services_data"),
        ("handling_null_capacity_trigger", "user_projects.services_data"),
    ]:
        op.execute(sa.text(f"DROP TRIGGER IF EXISTS {trigger_name} ON {table_name}"))

    # delete function
    for function_name in [
        "public.trigger_set_capacity_from_modeled",
    ]:
        op.execute(sa.text(f"DROP FUNCTION IF EXISTS {function_name}"))
