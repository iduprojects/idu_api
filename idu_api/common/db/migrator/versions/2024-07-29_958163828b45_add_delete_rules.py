# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""add delete rules

Revision ID: 958163828b45
Revises: f2982fcbb0c6
Create Date: 2024-07-29 12:37:35.105268

"""
from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "958163828b45"
down_revision: Union[str, None] = "f2982fcbb0c6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Remove the existing foreign key constraints
    op.drop_constraint(
        "urban_objects_data_fk_physical_object_id__physical_objects_data", "urban_objects_data", type_="foreignkey"
    )
    op.drop_constraint(
        "urban_objects_data_fk_object_geometry_id__object_geomet_db0b", "urban_objects_data", type_="foreignkey"
    )
    op.drop_constraint("urban_objects_data_fk_service_id__services_data", "urban_objects_data", type_="foreignkey")
    op.drop_constraint(
        "living_buildings_data_fk_physical_object_id__physical_o_2f2e", "living_buildings_data", type_="foreignkey"
    )

    # Add new foreign key constraints
    op.create_foreign_key(
        "urban_objects_data_fk_physical_object_id__physical_objects_data",
        "urban_objects_data",
        "physical_objects_data",
        ["physical_object_id"],
        ["physical_object_id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "urban_objects_data_fk_object_geometry_id__object_geomet_db0b",
        "urban_objects_data",
        "object_geometries_data",
        ["object_geometry_id"],
        ["object_geometry_id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "urban_objects_data_fk_service_id__services_data",
        "urban_objects_data",
        "services_data",
        ["service_id"],
        ["service_id"],
        ondelete="SET NULL",
    )
    op.create_foreign_key(
        "living_buildings_data_fk_physical_object_id__physical_o_2f2e",
        "living_buildings_data",
        "physical_objects_data",
        ["physical_object_id"],
        ["physical_object_id"],
        ondelete="CASCADE",
    )


def downgrade() -> None:
    # Remove the updated foreign key constraints
    op.drop_constraint(
        "urban_objects_data_fk_physical_object_id__physical_objects_data", "urban_objects_data", type_="foreignkey"
    )
    op.drop_constraint(
        "urban_objects_data_fk_object_geometry_id__object_geomet_db0b", "urban_objects_data", type_="foreignkey"
    )
    op.drop_constraint("urban_objects_data_fk_service_id__services_data", "urban_objects_data", type_="foreignkey")
    op.drop_constraint(
        "living_buildings_data_fk_physical_object_id__physical_o_2f2e", "living_buildings_data", type_="foreignkey"
    )

    # Add the original foreign key constraints without ON DELETE CASCADE
    op.create_foreign_key(
        "urban_objects_data_fk_physical_object_id__physical_objects_data",
        "urban_objects_data",
        "physical_objects_data",
        ["physical_object_id"],
        ["physical_object_id"],
    )
    op.create_foreign_key(
        "urban_objects_data_fk_object_geometry_id__object_geomet_db0b",
        "urban_objects_data",
        "object_geometries_data",
        ["object_geometry_id"],
        ["object_geometry_id"],
    )
    op.create_foreign_key(
        "urban_objects_data_fk_service_id__services_data",
        "urban_objects_data",
        "services_data",
        ["service_id"],
        ["service_id"],
    )
    op.create_foreign_key(
        "living_buildings_data_fk_physical_object_id__physical_o_2f2e",
        "living_buildings_data",
        "physical_objects_data",
        ["physical_object_id"],
        ["physical_object_id"],
    )
