# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""empty message

Revision ID: e5183cd68c66
Revises: aa0c57f0df82
Create Date: 2024-05-13 08:10:54.665929

"""
from typing import Sequence, Union

import geoalchemy2
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "e5183cd68c66"
down_revision: Union[str, None] = "aa0c57f0df82"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column("functional_zones_data", "territory_id", existing_type=sa.INTEGER(), nullable=False)

    op.execute("ALTER indicators_dict RENAME name TO name_full")
    op.alter_column("indicators_dict", "name_short", existing_type=sa.VARCHAR(length=200), nullable=True)
    op.execute("UPDATE indicators_dict SET name_short = name_full")
    op.alter_column("indicators_dict", "name_short", nullable=False)


    op.drop_constraint("indicators_dict_name_key", "indicators_dict", type_="unique")
    op.create_unique_constraint(op.f("indicators_dict_name_full_key"), "indicators_dict", ["name_full"])
    op.create_unique_constraint(op.f("indicators_dict_name_short_key"), "indicators_dict", ["name_short"])
    
    op.alter_column("object_geometries_data", "territory_id", existing_type=sa.INTEGER(), nullable=False)
    op.alter_column(
        "object_geometries_data",
        "geometry",
        existing_type=geoalchemy2.types.Geometry(
            spatial_index=False, from_text="ST_GeomFromEWKT", name="geometry", _spatial_index_reflected=True
        ),
        nullable=False,
    )
    op.alter_column(
        "object_geometries_data",
        "centre_point",
        existing_type=geoalchemy2.types.Geometry(
            geometry_type="POINT",
            spatial_index=False,
            from_text="ST_GeomFromEWKT",
            name="geometry",
            _spatial_index_reflected=True,
        ),
        nullable=False,
    )
    op.alter_column("service_types_normatives_data", "service_type_id", existing_type=sa.INTEGER(), nullable=False)
    op.alter_column("service_types_normatives_data", "urban_function_id", existing_type=sa.INTEGER(), nullable=False)
    op.alter_column("service_types_normatives_data", "territory_id", existing_type=sa.INTEGER(), nullable=False)
    op.drop_constraint(
        "service_types_normatives_data_service_type_id_urban_functio_key",
        "service_types_normatives_data",
        type_="unique",
    )
    op.create_unique_constraint(
        op.f("service_types_normatives_data_service_type_id_urban_function_id_territory_id_key"),
        "service_types_normatives_data",
        ["service_type_id", "urban_function_id", "territory_id"],
    )
    op.alter_column("services_data", "territory_type_id", existing_type=sa.INTEGER(), nullable=False)
    op.alter_column("services_data", "name", existing_type=sa.VARCHAR(length=200), nullable=False)
    op.alter_column(
        "territories_data",
        "geometry",
        existing_type=geoalchemy2.types.Geometry(
            spatial_index=False, from_text="ST_GeomFromEWKT", name="geometry", _spatial_index_reflected=True
        ),
        nullable=False,
    )
    op.alter_column(
        "territories_data",
        "centre_point",
        existing_type=geoalchemy2.types.Geometry(
            spatial_index=False, from_text="ST_GeomFromEWKT", name="geometry", _spatial_index_reflected=True
        ),
        type_=geoalchemy2.types.Geometry(
            geometry_type="POINT", spatial_index=False, from_text="ST_GeomFromEWKT", name="geometry", nullable=False
        ),
        nullable=False,
    )
    op.alter_column(
        "territory_indicators_data",
        "information_source",
        existing_type=sa.TEXT(),
        type_=sa.String(length=300),
        existing_nullable=True,
    )
    op.create_unique_constraint(op.f("urban_functions_dict_list_label_key"), "urban_functions_dict", ["list_label"])


def downgrade() -> None:
    op.drop_constraint(op.f("urban_functions_dict_list_label_key"), "urban_functions_dict", type_="unique")
    op.alter_column(
        "territory_indicators_data",
        "information_source",
        existing_type=sa.String(length=300),
        type_=sa.TEXT(),
        existing_nullable=True,
    )
    op.alter_column(
        "territories_data",
        "centre_point",
        existing_type=geoalchemy2.types.Geometry(
            geometry_type="POINT", spatial_index=False, from_text="ST_GeomFromEWKT", name="geometry", nullable=False
        ),
        type_=geoalchemy2.types.Geometry(
            spatial_index=False, from_text="ST_GeomFromEWKT", name="geometry", _spatial_index_reflected=True
        ),
        nullable=True,
    )
    op.alter_column(
        "territories_data",
        "geometry",
        existing_type=geoalchemy2.types.Geometry(
            spatial_index=False, from_text="ST_GeomFromEWKT", name="geometry", _spatial_index_reflected=True
        ),
        nullable=True,
    )
    op.alter_column("services_data", "name", existing_type=sa.VARCHAR(length=200), nullable=True)
    op.alter_column("services_data", "territory_type_id", existing_type=sa.INTEGER(), nullable=True)
    op.drop_constraint(
        op.f("service_types_normatives_data_service_type_id_urban_function_id_territory_id_key"),
        "service_types_normatives_data",
        type_="unique",
    )
    op.create_unique_constraint(
        "service_types_normatives_data_service_type_id_urban_functio_key",
        "service_types_normatives_data",
        ["service_type_id", "urban_function_id", "territory_id"],
    )
    op.alter_column("service_types_normatives_data", "territory_id", existing_type=sa.INTEGER(), nullable=True)
    op.alter_column("service_types_normatives_data", "urban_function_id", existing_type=sa.INTEGER(), nullable=True)
    op.alter_column("service_types_normatives_data", "service_type_id", existing_type=sa.INTEGER(), nullable=True)
    op.alter_column(
        "object_geometries_data",
        "centre_point",
        existing_type=geoalchemy2.types.Geometry(
            geometry_type="POINT",
            spatial_index=False,
            from_text="ST_GeomFromEWKT",
            name="geometry",
            _spatial_index_reflected=True,
        ),
        nullable=True,
    )
    op.alter_column(
        "object_geometries_data",
        "geometry",
        existing_type=geoalchemy2.types.Geometry(
            spatial_index=False, from_text="ST_GeomFromEWKT", name="geometry", _spatial_index_reflected=True
        ),
        nullable=True,
    )
    op.alter_column("object_geometries_data", "territory_id", existing_type=sa.INTEGER(), nullable=True)

    op.drop_constraint(op.f("indicators_dict_name_short_key"), "indicators_dict", type_="unique")
    op.drop_constraint(op.f("indicators_dict_name_full_key"), "indicators_dict", type_="unique")
    op.create_unique_constraint("indicators_dict_name_key", "indicators_dict", ["name_full"])
    op.execute("ALTER indicators_dict RENAME name_full TO name")
    op.drop_column("indicators_dict", "name_short", existing_type=sa.VARCHAR(length=200), nullable=True)

    op.alter_column("functional_zones_data", "territory_id", existing_type=sa.INTEGER(), nullable=True)
