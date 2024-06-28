# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""create_users_schema

Revision ID: 79adbe93b346
Revises: e5183cd68c66
Create Date: 2024-05-25 13:31:09.269838

"""
from textwrap import dedent
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "79adbe93b346"
down_revision: Union[str, None] = "e5183cd68c66"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(sa.schema.CreateSchema("users"))

    op.execute(
        sa.text(
            dedent(
                """
                CREATE TRIGGER set_center_point_trigger_trigger
                BEFORE INSERT OR UPDATE ON public.territories_data
                FOR EACH ROW
                EXECUTE PROCEDURE public.trigger_set_centre_point();
                """
            )
        )
    )


def downgrade() -> None:
    op.execute(sa.text("DROP TRIGGER set_center_point_trigger_trigger ON public.territories_data"))
    op.execute(sa.text("DROP FUNCTION public.trigger_set_centre_point"))

    op.execute(sa.schema.DropSchema("users", cascade=True))
