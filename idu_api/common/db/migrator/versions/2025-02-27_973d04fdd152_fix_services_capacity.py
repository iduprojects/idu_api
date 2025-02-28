# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""fix services capacity

Revision ID: 973d04fdd152
Revises: 4b00fa33ec4a
Create Date: 2025-02-27 14:05:40.585814

"""
from textwrap import dedent
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "973d04fdd152"
down_revision: Union[str, None] = "4b00fa33ec4a"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Rename `services_data.capacity_real` to `capacity`, add column `is_capacity_real`
    and trigger to set `is_capacity_real`, if `services_data.capacity_real` was set."""

    for schema in ("public", "user_projects"):
        op.alter_column("services_data", "capacity_real", new_column_name="capacity", schema=schema)
        op.add_column(
            "services_data",
            sa.Column("is_capacity_real", sa.Boolean(), nullable=True),
            schema=schema,
        )
        op.execute(
            sa.text(
                dedent(
                    f"""
                    UPDATE {schema}.services_data
                    SET is_capacity_real = (properties->>'is_capacity_real')::BOOLEAN
                    WHERE properties ? 'is_capacity_real'
                    """
                )
            )
        )
        op.execute(
            sa.text(
                dedent(
                    f"""
                    UPDATE {schema}.services_data
                    SET is_capacity_real = FALSE
                    WHERE capacity IS NOT NULL and NOT properties ? 'is_capacity_real'
                    """
                )
            )
        )
        op.execute(
            sa.text(
                dedent(
                    f"""
                    CREATE OR REPLACE FUNCTION {schema}.trigger_update_service_is_capacity_real()
                    RETURNS trigger
                    LANGUAGE plpgsql
                    AS $function$
                    BEGIN
                        NEW.is_capacity_real := FALSE;
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
                    f"""
                    CREATE TRIGGER insert_service_is_capacity_real_trigger
                    BEFORE INSERT ON {schema}.services_data
                    FOR EACH ROW
                    WHEN (NEW.capacity IS NOT NULL AND NEW.is_capacity_real IS NULL)
                    EXECUTE FUNCTION {schema}.trigger_update_service_is_capacity_real();
                    """
                )
            )
        )
        op.execute(
            sa.text(
                dedent(
                    f"""
                    CREATE TRIGGER update_service_is_capacity_real_trigger
                    BEFORE UPDATE ON {schema}.services_data
                    FOR EACH ROW
                    WHEN (NEW.capacity IS NOT NULL AND OLD.capacity IS NULL AND NEW.is_capacity_real IS NULL)
                    EXECUTE FUNCTION {schema}.trigger_update_service_is_capacity_real();
                    """
                )
            )
        )


def downgrade() -> None:
    """Revert changes to `services_data` table."""

    for schema in ("public", "user_projects"):
        op.execute(f"DROP TRIGGER IF EXISTS insert_service_is_capacity_real_trigger ON {schema}.services_data")
        op.execute(f"DROP TRIGGER IF EXISTS update_service_is_capacity_real_trigger ON {schema}.services_data")
        op.execute(f"DROP FUNCTION IF EXISTS {schema}.trigger_update_service_is_capacity_real")
        op.alter_column("services_data", "capacity", new_column_name="capacity_real", schema=schema)
        op.drop_column("services_data", "is_capacity_real", schema=schema)
