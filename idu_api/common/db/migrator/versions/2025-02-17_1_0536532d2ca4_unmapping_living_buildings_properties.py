# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""unmapping living buildings properties

Revision ID: 0536532d2ca4
Revises: 16b17cc8a1c0
Create Date: 2025-02-17 14:55:39.685822

"""
from collections.abc import Callable
from textwrap import dedent
from typing import Any, Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.sql.type_api import TypeEngine

# revision identifiers, used by Alembic.
revision: str = "0536532d2ca4"
down_revision: Union[str, None] = "16b17cc8a1c0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

new_columns = [
    ("floors", sa.Integer, "floor_count_max", {}),
    ("building_area_modeled", sa.Float, None, {}),
    ("building_area_official", sa.Float, "building_area", {"precision": 53}),
    ("project_type", sa.String, "project_type", {"length": 512}),
    ("floor_type", sa.String, "floor_type", {"length": 128}),
    ("wall_material", sa.String, "wall_material", {"length": 128}),
    ("built_year", sa.Integer, "built_year", {}),
    ("exploitation_start_year", sa.Integer, "exploitation_start_year", {}),
]

types_mapping = {
    sa.Boolean: "BOOLEAN",
    sa.String: "VARCHAR",
    sa.Integer: "FLOAT",
    sa.Float: "FLOAT",
}


def upgrade() -> None:
    """Rename `living_buildings_data` to `buildings_data`, create new columns and fix properties."""

    # rename table
    for schema in ("public", "user_projects"):
        op.rename_table("living_buildings_data", "buildings_data", schema=schema)
        op.alter_column("buildings_data", "living_building_id", new_column_name="building_id", schema=schema)
    op.execute(f"ALTER SEQUENCE living_buildings_data_id_seq RENAME TO buildings_data_id_seq")
    op.execute(f"ALTER SEQUENCE user_projects.living_buildings_id_seq RENAME TO buildings_data_id_seq")

    def add_columns_to_table(
        table: str,
        columns: list[tuple[str, Callable[..., TypeEngine], str, dict[str, Any]]],
    ) -> None:
        """Add columns to a table."""

        for column_name, column_type, _, kwargs in columns:
            new_column = sa.Column(column_name, column_type(**kwargs), nullable=True)
            for schema in ("public", "user_projects"):
                op.add_column(table, new_column, schema=schema)

    # add new columns
    add_columns_to_table("buildings_data", new_columns)

    # fix `properties` and extract data from to columns
    op.execute(
        sa.text(
            dedent(
                f"""
            UPDATE physical_objects_data AS pod
            SET properties = 
                pod.properties 
                || COALESCE((bd.properties->>'physical_object_data')::jsonb, '{{}}'::jsonb)
            FROM buildings_data as bd
            WHERE bd.physical_object_id = pod.physical_object_id;
            """
            )
        )
    )
    op.execute(
        sa.text(
            dedent(
                """
                UPDATE object_geometries_data AS ogd
                SET osm_id = bd.properties->>'osm_id'
                FROM urban_objects_data AS uod
                JOIN buildings_data AS bd ON bd.physical_object_id = uod.physical_object_id
                WHERE uod.object_geometry_id = ogd.object_geometry_id AND bd.properties ? 'osm_id';
                """
            )
        )
    )
    for schema in ("public", "user_projects"):
        op.execute(
            sa.text(
                dedent(
                    f"""
                    UPDATE {schema}.buildings_data 
                    SET properties = 
                        properties - 'physical_object_data'
                        || jsonb_build_object('osm_data', COALESCE((properties->>'osm_data')::jsonb, '{{}}'::jsonb))
                        || jsonb_build_object('building_data', COALESCE((properties->>'building_data')::jsonb, '{{}}'::jsonb))
                        || jsonb_build_object('frt_data', COALESCE((properties->>'frt_data')::jsonb, '{{}}'::jsonb));
                    """
                )
            )
        )
    for schema in ("public", "user_projects"):
        op.execute(
            sa.text(
                dedent(
                    f"""
                    UPDATE {schema}.buildings_data
                    SET properties =
                        properties - 'frt_data' - 'building_data'
                        || 
                        (
                            CASE
                              WHEN properties ? 'frt_data'
                              THEN (properties->'frt_data') || (properties->'building_data')
                              ELSE properties->'building_data'
                            END
                        );
                    """
                )
            )
        )
    for schema in ("public", "user_projects"):
        op.execute(
            sa.text(
                dedent(
                    f"""
                    UPDATE {schema}.buildings_data
                    SET properties = 
                        (properties - 'area_residential' - 'is_living' - 'modeled' - 'properties')
                        ||
                        jsonb_build_object(
                            'living_area_official', 
                            CASE 
                                WHEN properties->>'area_residential' IS NOT NULL 
                                    THEN to_jsonb(REPLACE((properties->>'area_residential'), ',', '.')::float)
                                ELSE 'null'::jsonb
                            END
                        )
                        || jsonb_build_object('living_area_modeled', 'null'::jsonb);
                    """
                )
            )
        )

    def update_query(
        table: str, columns: list[tuple[str, Callable[..., TypeEngine], str, dict[str, Any]]], schema: str = "public"
    ) -> str:
        """Create SQL-query for updating table."""
        header = f"UPDATE {schema}.{table}\nSET"
        assignments = ",\n".join(
            f"\t{col} = ("
            + ("REPLACE(" if sa_type is sa.Float or sa_type is sa.Integer else "")
            + f"(properties->>'{prop_name}')"
            + (", ',', '.')" if sa_type is sa.Float or sa_type is sa.Integer else "")
            + f"::{types_mapping[sa_type]})"
            + ("::INTEGER" if sa_type is sa.Integer else "")
            for col, sa_type, prop_name, _ in columns
            if prop_name is not None
        )
        return f"{header}\n{assignments}"

    for schema in ("public", "user_projects"):
        op.execute(sa.text(dedent(update_query("buildings_data", new_columns, schema=schema))))

    for schema in ("public", "user_projects"):
        op.drop_column("buildings_data", "living_area", schema=schema)

    for schema in ("public", "user_projects"):
        op.execute(
            sa.text(
                dedent(
                    f"UPDATE {schema}.buildings_data SET properties = properties - "
                    + "\n- ".join(
                        [f"'{prop_name}'" for col, sa_type, prop_name, _ in new_columns if prop_name is not None]
                    )
                )
            )
        )


def downgrade() -> None:
    """Return `buildings_data` to the previous state."""

    # rename table
    for schema in ("public", "user_projects"):
        op.rename_table("buildings_data", "living_buildings_data", schema=schema)
        op.alter_column("living_buildings_data", "building_id", new_column_name="living_building_id", schema=schema)
    op.execute(f"ALTER SEQUENCE buildings_data_id_seq RENAME TO living_buildings_data_id_seq")
    op.execute(f"ALTER SEQUENCE user_projects.buildings_data_id_seq RENAME TO living_buildings_id_seq")

    # drop new columns from `living_buildings_data`
    for col, sa_type, prop_name, _ in new_columns:
        if prop_name is not None:
            for schema in ("public", "user_projects"):
                op.execute(
                    sa.text(
                        dedent(
                            f"""
                            UPDATE {schema}.living_buildings_data
                            SET properties = jsonb_set(
                                properties, 
                                '{{{prop_name}}}', 
                                COALESCE(to_jsonb({col}), 'null'::jsonb),
                                true
                            )
                            """
                        )
                    )
                )
        op.drop_column("living_buildings_data", col)
        op.drop_column("living_buildings_data", col, schema="user_projects")

    # return `living_area` column
    for schema in ("public", "user_projects"):
        op.add_column(
            "living_buildings_data",
            sa.Column("living_area", sa.Float(precision=53), nullable=True),
            schema=schema,
        )
        op.execute(
            sa.text(
                dedent(
                    f"""
                    UPDATE {schema}.living_buildings_data
                    SET properties = jsonb_set(
                        properties, 
                        '{{area_residential}}', 
                        COALESCE(to_jsonb(properties->>'living_area_official'), 'null'::jsonb), 
                        true
                    )
                    """
                )
            )
        )
