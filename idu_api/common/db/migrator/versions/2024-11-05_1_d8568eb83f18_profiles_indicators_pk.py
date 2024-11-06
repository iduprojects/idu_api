# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""profiles indicators pk

Revision ID: d8568eb83f18
Revises: 60931341d80d
Create Date: 2024-11-05 12:32:01.754103

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "d8568eb83f18"
down_revision: Union[str, None] = "60931341d80d"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(sa.schema.CreateSequence(sa.Sequence("indicators_data_id_seq", schema="user_projects")))
    op.execute(sa.schema.CreateSequence(sa.Sequence("profiles_data_id_seq", schema="user_projects")))

    op.add_column(
        "profiles_data",
        sa.Column(
            "profile_id",
            sa.Integer(),
            server_default=sa.text("nextval('user_projects.profiles_data_id_seq')"),
            nullable=False,
        ),
        schema="user_projects",
    )
    op.create_primary_key(
        "profiles_data_pk",
        "profiles_data",
        ["profile_id"],
        schema="user_projects",
    )

    op.alter_column(
        "indicators_data",
        "indicator_value_id",
        server_default=sa.text("nextval('user_projects.indicators_data_id_seq')"),
        schema="user_projects",
    )


def downgrade() -> None:
    op.alter_column("indicators_data", "indicator_value_id", server_default=None, schema="user_projects")
    op.drop_constraint("profiles_data_pk", "profiles_data", schema="user_projects")
    op.drop_column("profiles_data", "profile_id", schema="user_projects")

    op.execute(sa.schema.DropSequence(sa.Sequence("indicators_data_id_seq", schema="user_projects")))
    op.execute(sa.schema.DropSequence(sa.Sequence("profiles_data_id_seq", schema="user_projects")))
