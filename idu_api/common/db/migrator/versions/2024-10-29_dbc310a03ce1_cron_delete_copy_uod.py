# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""cron delete copy uod

Revision ID: dbc310a03ce1
Revises: 2efcf46bb693
Create Date: 2024-10-29 17:54:16.220414

"""
from textwrap import dedent
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "dbc310a03ce1"
down_revision: Union[str, None] = "21b28a319b43"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_cron;")
    delete_duplicates_sql = sa.text(
        dedent(
            """
            DO $$
            BEGIN
                DELETE FROM urban_objects_data uo
                WHERE uo.urban_object_id NOT IN (
                    SELECT MIN(uo_inner.urban_object_id)
                    FROM urban_objects_data uo_inner
                    WHERE uo_inner.service_id IS NULL
                    GROUP BY uo_inner.physical_object_id, uo_inner.object_geometry_id
                )
                AND uo.service_id IS NULL;
            END $$;
            """
        )
    )
    op.execute(sa.text(dedent(f"SELECT cron.schedule('0 */6 * * *', $$ {delete_duplicates_sql} $$);")))


def downgrade():
    op.execute("SELECT cron.unschedule('0 0 * * *');")
    op.execute("DROP EXTENSION IF EXISTS pg_cron;")
