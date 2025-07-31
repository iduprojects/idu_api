# pylint: disable=no-member,invalid-name,missing-function-docstring,too-many-statements
"""buffers triggers

Revision ID: 01ceb2ef5830
Revises: df9f22c86c5f
Create Date: 2025-07-17 13:06:19.042202

"""
from textwrap import dedent
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "01ceb2ef5830"
down_revision: Union[str, None] = "df9f22c86c5f"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # create triggers on insert/update `buffers_data` (if geometry is not specified)
    for schema in ("public", "user_projects"):
        if schema == "public":
            buffer_type_logic = f"""
                SELECT
                    CASE
                        WHEN uod.service_id IS NULL THEN pod.physical_object_type_id
                        ELSE NULL
                    END,
                    CASE
                        WHEN uod.service_id IS NOT NULL THEN sd.service_type_id
                        ELSE NULL
                    END
                INTO v_physical_object_type_id, v_service_type_id
                FROM {schema}.urban_objects_data uod
                LEFT JOIN public.physical_objects_data pod ON uod.physical_object_id = pod.physical_object_id
                LEFT JOIN public.services_data sd ON uod.service_id = sd.service_id
                WHERE uod.urban_object_id = NEW.urban_object_id;
            """

            buffer_assignment = f"""
                BEGIN
                    SELECT ogd.geometry
                    INTO v_object_geom
                    FROM {schema}.urban_objects_data uod
                    JOIN {schema}.object_geometries_data ogd ON uod.object_geometry_id = ogd.object_geometry_id
                    WHERE uod.urban_object_id = NEW.urban_object_id;
        
                    IF v_object_geom IS NULL THEN
                        RAISE EXCEPTION 'Could not find geometry for urban_object_id=%', NEW.urban_object_id;
                    END IF;
        
                    {buffer_type_logic}
        
                    SELECT buffer_value
                    INTO buffer_radius
                    FROM public.default_buffer_values_dict
                    WHERE buffer_type_id = NEW.buffer_type_id
                      AND (
                          (v_physical_object_type_id IS NOT NULL AND v_physical_object_type_id = default_buffer_values_dict.physical_object_type_id AND default_buffer_values_dict.service_type_id IS NULL)
                       OR (v_service_type_id IS NOT NULL AND v_service_type_id = default_buffer_values_dict.service_type_id AND default_buffer_values_dict.physical_object_type_id IS NULL)
                      )
                    LIMIT 1;
        
                    IF buffer_radius IS NULL THEN
                        RAISE EXCEPTION 'No standard buffer radius found for buffer_type_id=%, physical_object_type_id=%, service_type_id=%',
                            NEW.buffer_type_id, v_physical_object_type_id, v_service_type_id;
                    END IF;
        
                    BEGIN
                        NEW.geometry := ST_Difference(
                            ST_Transform(
                                ST_Buffer(v_object_geom::geography, buffer_radius)::geometry,
                                ST_SRID(v_object_geom)
                            ),
                            v_object_geom
                        );
                    EXCEPTION WHEN OTHERS THEN
                        RAISE NOTICE 'Buffer generation failed for urban_object_id=%', NEW.urban_object_id;
                        RETURN NEW;
                    END;
                END;
            """
        else:
            buffer_type_logic = f"""
                SELECT
                    CASE
                        WHEN uod.service_id IS NULL AND uod.public_service_id IS NULL THEN
                            CASE
                                WHEN uod.physical_object_id IS NOT NULL THEN pod.physical_object_type_id
                                ELSE p_pod.physical_object_type_id
                            END
                        ELSE NULL
                    END,
                    CASE
                        WHEN uod.service_id IS NOT NULL THEN sd.service_type_id
                        WHEN uod.public_service_id IS NOT NULL THEN p_sd.service_type_id
                        ELSE NULL
                    END
                INTO v_physical_object_type_id, v_service_type_id
                FROM {schema}.urban_objects_data uod
                LEFT JOIN public.physical_objects_data pod ON uod.physical_object_id = pod.physical_object_id
                LEFT JOIN public.physical_objects_data p_pod ON uod.public_physical_object_id = p_pod.physical_object_id
                LEFT JOIN public.services_data sd ON uod.service_id = sd.service_id
                LEFT JOIN public.services_data p_sd ON uod.public_service_id = p_sd.service_id
                WHERE uod.urban_object_id = NEW.urban_object_id;
            """

            buffer_assignment = f"""
                BEGIN
                    -- Get object geometry
                    SELECT ogd.geometry
                    INTO v_object_geom
                    FROM {schema}.urban_objects_data uod
                    JOIN {schema}.object_geometries_data ogd ON uod.object_geometry_id = ogd.object_geometry_id
                    WHERE uod.urban_object_id = NEW.urban_object_id;

                    IF v_object_geom IS NULL THEN
                        RAISE EXCEPTION 'Could not find geometry for urban_object_id=%', NEW.urban_object_id;
                    END IF;

                    {buffer_type_logic}

                    SELECT buffer_value
                    INTO buffer_radius
                    FROM public.default_buffer_values_dict
                    WHERE buffer_type_id = NEW.buffer_type_id
                      AND (
                          (v_physical_object_type_id IS NOT NULL AND v_physical_object_type_id = default_buffer_values_dict.physical_object_type_id AND default_buffer_values_dict.service_type_id IS NULL)
                       OR (v_service_type_id IS NOT NULL AND v_service_type_id = default_buffer_values_dict.service_type_id AND default_buffer_values_dict.physical_object_type_id IS NULL)
                      )
                    LIMIT 1;

                    IF buffer_radius IS NULL THEN
                        RAISE EXCEPTION 'No standard buffer radius found for buffer_type_id=%, physical_object_type_id=%, service_type_id=%',
                            NEW.buffer_type_id, v_physical_object_type_id, v_service_type_id;
                    END IF;

                    -- Get project's territory geometry
                    SELECT ptd.geometry, p.is_regional
                    INTO v_project_geom, v_is_regional
                    FROM {schema}.urban_objects_data uod
                    JOIN {schema}.scenarios_data s ON uod.scenario_id = s.scenario_id
                    JOIN {schema}.projects_data p ON s.project_id = p.project_id
                    JOIN {schema}.projects_territory_data ptd ON p.project_id = ptd.project_id
                    WHERE uod.urban_object_id = NEW.urban_object_id;

                    IF v_project_geom IS NULL AND NOT v_is_regional THEN
                        RAISE EXCEPTION 'Could not find project territory geometry for urban_object_id=%', NEW.urban_object_id;
                    END IF;

                    BEGIN
                    
                        IF NOT v_is_regional THEN 
                            result_geom := ST_Intersection(
                                ST_Difference(
                                    ST_Transform(
                                        ST_Buffer(v_object_geom::geography, v_buffer_value)::geometry,
                                        ST_SRID(v_object_geom)
                                    ),
                                    v_object_geom
                                ),
                                v_project_geom
                            );
                        ELSE
                            result_geom := ST_Difference(
                                ST_Transform(
                                    ST_Buffer(v_object_geom::geography, v_buffer_value)::geometry,
                                    ST_SRID(v_object_geom)
                                ),
                                v_object_geom
                            );
                        END IF;
                    
                        NEW.geometry := result_geom;
                        
                    EXCEPTION WHEN OTHERS THEN
                        RAISE NOTICE 'Buffer generation failed for urban_object_id=%', NEW.urban_object_id;
                        RETURN NEW;
                    END;
                END;
            """

        op.execute(
            sa.text(
                dedent(
                    f"""
                    CREATE OR REPLACE FUNCTION {schema}.trigger_set_default_buffer_geometry()
                    RETURNS trigger
                    LANGUAGE plpgsql
                    AS $function$
                    DECLARE
                        v_object_geom geometry;
                        buffer_radius FLOAT;
                        v_physical_object_type_id INT;
                        v_service_type_id INT;
                        v_project_geom GEOMETRY;
                        v_is_regional BOOLEAN;
                        result_geom GEOMETRY;
                    BEGIN
                        IF NEW.geometry IS NULL THEN
                            {buffer_assignment}
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
                    f"""
                    CREATE TRIGGER set_default_buffer_geometry_trigger
                    BEFORE INSERT OR UPDATE ON {schema}.buffers_data
                    FOR EACH ROW
                    EXECUTE PROCEDURE {schema}.trigger_set_default_buffer_geometry();
                    """
                )
            )
        )

    # create triggers on insert/update `urban_objects_data` (if default buffer radius exists)
    for schema in ("public", "user_projects"):
        if schema == "public":
            geometry_logic = """
                SELECT geometry INTO v_object_geom
                FROM public.object_geometries_data
                WHERE object_geometry_id = NEW.object_geometry_id;
            """
            buffer_type_logic = """
                IF NEW.service_id IS NOT NULL THEN
                    SELECT service_type_id INTO v_service_type_id
                    FROM public.services_data
                    WHERE service_id = NEW.service_id;
                    v_physical_object_type_id := NULL;
                ELSE
                    SELECT physical_object_type_id INTO v_physical_object_type_id
                    FROM public.physical_objects_data
                    WHERE physical_object_id = NEW.physical_object_id;
                    v_service_type_id := NULL;
                END IF;
            """

            project_geometry_logic = ""

            result_geom = """
                result_geom := ST_Difference(
                    ST_Transform(
                        ST_Buffer(v_object_geom::geography, v_buffer_value)::geometry,
                        ST_SRID(v_object_geom)
                    ),
                    v_object_geom
                );
            """
        else:
            geometry_logic = """
                IF NEW.public_urban_object_id IS NOT NULL THEN
                    RETURN NULL;
                ELSIF NEW.public_object_geometry_id IS NOT NULL THEN
                    SELECT geometry INTO v_object_geom
                    FROM public.object_geometries_data
                    WHERE object_geometry_id = NEW.public_object_geometry_id;
                ELSE
                    SELECT geometry INTO v_object_geom
                    FROM user_projects.object_geometries_data
                    WHERE object_geometry_id = NEW.object_geometry_id;
                END IF;
            """

            buffer_type_logic = """
                IF NEW.public_service_id IS NOT NULL THEN
                    SELECT service_type_id INTO v_service_type_id
                    FROM public.services_data
                    WHERE service_id = NEW.public_service_id;
                    v_physical_object_type_id := NULL;
                ELSIF NEW.service_id IS NOT NULL THEN
                    SELECT service_type_id INTO v_service_type_id
                    FROM user_projects.services_data
                    WHERE service_id = NEW.service_id;
                    v_physical_object_type_id := NULL;
                ELSIF NEW.public_physical_object_id IS NOT NULL THEN
                    SELECT physical_object_type_id INTO v_physical_object_type_id
                    FROM public.physical_objects_data
                    WHERE physical_object_id = NEW.public_physical_object_id;
                    v_service_type_id := NULL;
                ELSE
                    SELECT physical_object_type_id INTO v_physical_object_type_id
                    FROM user_projects.physical_objects_data
                    WHERE physical_object_id = NEW.physical_object_id;
                    v_service_type_id := NULL;
                END IF;
            """

            project_geometry_logic = """
                SELECT ptd.geometry, p.is_regional
                INTO v_project_geom, v_is_regional
                FROM user_projects.urban_objects_data uod
                JOIN user_projects.scenarios_data s ON uod.scenario_id = s.scenario_id
                JOIN user_projects.projects_data p ON s.project_id = p.project_id
                JOIN user_projects.projects_territory_data ptd ON p.project_id = ptd.project_id
                WHERE uod.urban_object_id = NEW.urban_object_id;
                    
                IF v_project_geom IS NULL AND NOT v_is_regional THEN
                    RAISE EXCEPTION 'Could not find project territory geometry for urban_object_id=%', NEW.urban_object_id;
                END IF;
            """

            result_geom = """
                IF NOT v_is_regional THEN 
                    result_geom := ST_Intersection(
                        ST_Difference(
                            ST_Transform(
                                ST_Buffer(v_object_geom::geography, v_buffer_value)::geometry,
                                ST_SRID(v_object_geom)
                            ),
                            v_object_geom
                        ),
                        v_project_geom
                    );
                ELSE
                    result_geom := ST_Difference(
                        ST_Transform(
                            ST_Buffer(v_object_geom::geography, v_buffer_value)::geometry,
                            ST_SRID(v_object_geom)
                        ),
                        v_object_geom
                    );
                END IF;
            """

        op.execute(
            sa.text(
                dedent(
                    f"""
                    CREATE OR REPLACE FUNCTION {schema}.trigger_update_buffers_for_urban_object()
                    RETURNS TRIGGER AS $$
                    DECLARE
                        v_object_geom GEOMETRY;
                        v_physical_object_type_id INT;
                        v_service_type_id INT;
                        v_buffer_type_id INT;
                        v_buffer_value FLOAT;
                        srid INT;
                        result_geom GEOMETRY;
                        v_project_geom GEOMETRY;
                        v_is_regional BOOLEAN;
                    BEGIN
                        -- 1. Get the geometry
                        {geometry_logic}
                    
                        IF v_object_geom IS NULL THEN
                            RAISE EXCEPTION 'Cannot find geometry for urban_object_id = %, schema = %', NEW.urban_object_id, TG_TABLE_SCHEMA;
                        END IF;
                        
                        -- 1.1 Get the project's territory geometry
                        {project_geometry_logic}
                    
                        -- 2. Get the type of object or service
                        {buffer_type_logic}
                        
                        -- 3. Iterate through buffer values
                        FOR v_buffer_type_id, v_buffer_value IN
                            SELECT buffer_type_id, buffer_value
                            FROM default_buffer_values_dict
                            WHERE
                                (physical_object_type_id = v_physical_object_type_id AND v_physical_object_type_id IS NOT NULL)
                                OR
                                (service_type_id = v_service_type_id AND v_service_type_id IS NOT NULL)
                        LOOP
                            srid := ST_SRID(v_object_geom);
                            
                            {result_geom}
                
                            IF result_geom IS NULL THEN
                                RAISE EXCEPTION 'Resulting geometry is NULL for urban_object_id=%, buffer_type_id=%', NEW.urban_object_id, v_buffer_type_id;
                            END IF;
                
                            PERFORM 1 FROM {schema}.buffers_data
                            WHERE buffer_type_id = v_buffer_type_id AND urban_object_id = NEW.urban_object_id;
                
                            IF FOUND THEN
                                UPDATE {schema}.buffers_data
                                SET geometry = result_geom
                                WHERE buffer_type_id = v_buffer_type_id AND urban_object_id = NEW.urban_object_id;
                            ELSE
                                INSERT INTO {schema}.buffers_data (buffer_type_id, urban_object_id, geometry)
                                VALUES (v_buffer_type_id, NEW.urban_object_id, result_geom);
                            END IF;
                        END LOOP;
                    
                        RETURN NULL;
                    END;
                    $$ LANGUAGE plpgsql;
                    """
                )
            )
        )

        op.execute(
            sa.text(
                dedent(
                    f"""
                    CREATE TRIGGER update_buffers_on_urban_object_trigger
                    AFTER INSERT OR UPDATE ON {schema}.urban_objects_data
                    FOR EACH ROW
                    EXECUTE FUNCTION {schema}.trigger_update_buffers_for_urban_object();
                    """
                )
            )
        )

    # create triggers on update `physical_objects_data` (if physical_object_type were changed)
    for schema in ("public", "user_projects"):
        if schema == "public":
            urban_objects = """
                SELECT uod.urban_object_id, ogd.geometry, ST_SRID(ogd.geometry)
                FROM public.urban_objects_data uod
                JOIN public.object_geometries_data ogd ON uod.object_geometry_id = ogd.object_geometry_id
                WHERE uod.physical_object_id = v_physical_object_id and uod.service_id is NULL 
            """

            project_geometry_logic = ""

            result_geom = """
                result_geom := ST_Difference(
                    ST_Transform(
                        ST_Buffer(v_object_geom::geography, v_buffer_value)::geometry,
                        ST_SRID(v_object_geom)
                    ),
                    v_object_geom
                );
                
                IF result_geom IS NULL THEN
                    RAISE EXCEPTION 'Resulting geometry is NULL for urban_object_id=%, buffer_type_id=%', NEW.urban_object_id, v_buffer_type_id;
                END IF;
            """

            user_projects_logic = f"""
                FOR v_urban_object_id, v_object_geom, v_srid IN
                    SELECT 
                        uod.urban_object_id, 
                        CASE
                            WHEN uod.object_geometry_id IS NULL THEN pub_ogd.geometry
                            ELSE up_ogd.geometry
                        END,
                        CASE
                            WHEN uod.object_geometry_id IS NULL THEN ST_SRID(pub_ogd.geometry)
                            ELSE ST_SRID(up_ogd.geometry)
                        END
                    FROM user_projects.urban_objects_data uod
                    LEFT JOIN user_projects.object_geometries_data up_ogd ON uod.object_geometry_id = up_ogd.object_geometry_id
                    LEFT JOIN public.object_geometries_data pub_ogd ON uod.public_object_geometry_id = pub_ogd.object_geometry_id
                    WHERE uod.public_physical_object_id = v_physical_object_id and uod.service_id is NULL and uod.public_service_id is NULL
                
                LOOP
                
                    {project_geometry_logic}
                
                    FOR v_buffer_type_id, v_buffer_value IN
                        SELECT b.buffer_type_id, d.buffer_value
                        FROM user_projects.buffers_data b
                        JOIN public.default_buffer_values_dict d
                          ON b.buffer_type_id = d.buffer_type_id
                         AND d.physical_object_type_id = v_physical_object_type_id
                        WHERE b.urban_object_id = v_urban_object_id
                    LOOP
                    
                        IF NOT v_is_regional THEN 
                            result_geom := ST_Intersection(
                                ST_Difference(
                                    ST_Transform(
                                        ST_Buffer(v_object_geom::geography, v_buffer_value)::geometry,
                                        ST_SRID(v_object_geom)
                                    ),
                                    v_object_geom
                                ),
                                v_project_geom
                            );
                        ELSE
                            result_geom := ST_Difference(
                                ST_Transform(
                                    ST_Buffer(v_object_geom::geography, v_buffer_value)::geometry,
                                    ST_SRID(v_object_geom)
                                ),
                                v_object_geom
                            );
                        END IF;
        
                        UPDATE user_projects.buffers_data
                        SET geometry = result_geom
                        WHERE urban_object_id = v_urban_object_id AND buffer_type_id = v_buffer_type_id;
                    END LOOP;
                END LOOP;
            """
        else:
            urban_objects = """
                SELECT 
                    uod.urban_object_id, 
                    CASE
                        WHEN uod.object_geometry_id IS NULL THEN pub_ogd.geometry
                        ELSE up_ogd.geometry
                    END,
                    CASE
                        WHEN uod.object_geometry_id IS NULL THEN ST_SRID(pub_ogd.geometry)
                        ELSE ST_SRID(up_ogd.geometry)
                    END
                FROM user_projects.urban_objects_data uod
                LEFT JOIN user_projects.object_geometries_data up_ogd ON uod.object_geometry_id = up_ogd.object_geometry_id
                LEFT JOIN public.object_geometries_data pub_ogd ON uod.public_object_geometry_id = pub_ogd.object_geometry_id
                WHERE uod.physical_object_id = v_physical_object_id and uod.service_id is NULL and uod.public_service_id is NULL
            """

            project_geometry_logic = """
                SELECT ptd.geometry, p.is_regional
                INTO v_project_geom, v_is_regional
                FROM user_projects.urban_objects_data uod
                JOIN user_projects.scenarios_data s ON uod.scenario_id = s.scenario_id
                JOIN user_projects.projects_data p ON s.project_id = p.project_id
                JOIN user_projects.projects_territory_data ptd ON p.project_id = ptd.project_id
                WHERE uod.urban_object_id = v_urban_object_id;
                    
                IF v_project_geom IS NULL AND NOT v_is_regional THEN
                    RAISE EXCEPTION 'Could not find project territory geometry for urban_object_id=%', NEW.urban_object_id;
                END IF;
            """

            result_geom = """
                IF NOT v_is_regional THEN 
                    result_geom := ST_Intersection(
                        ST_Difference(
                            ST_Transform(
                                ST_Buffer(v_object_geom::geography, v_buffer_value)::geometry,
                                ST_SRID(v_object_geom)
                            ),
                            v_object_geom
                        ),
                        v_project_geom
                    );
                ELSE
                    result_geom := ST_Difference(
                        ST_Transform(
                            ST_Buffer(v_object_geom::geography, v_buffer_value)::geometry,
                            ST_SRID(v_object_geom)
                        ),
                        v_object_geom
                    );
                END IF;
                
                IF result_geom IS NULL  THEN
                    RAISE EXCEPTION 'Resulting geometry is NULL for urban_object_id=%, buffer_type_id=%', NEW.urban_object_id, v_buffer_type_id;
                END IF;
            """

            user_projects_logic = ""

        op.execute(
            sa.text(
                dedent(
                    f"""
                    CREATE OR REPLACE FUNCTION {schema}.trigger_update_buffer_on_update_physical_object()
                    RETURNS TRIGGER AS $$
                    DECLARE
                        v_physical_object_id INT := NEW.physical_object_id;
                        v_physical_object_type_id INT := NEW.physical_object_type_id;
                    
                        v_urban_object_id INT;
                        v_buffer_type_id INT;
                        v_buffer_value FLOAT;
                        v_object_geom GEOMETRY;
                        v_srid INT;
                        result_geom GEOMETRY;
                        v_project_geom GEOMETRY;
                        v_is_regional BOOLEAN;
                    BEGIN
                        -- Go through all urban_object_ids associated with the changed physical_object_id and not having a service_id.
                        FOR v_urban_object_id, v_object_geom, v_srid IN
                            {urban_objects}
                        LOOP
                            
                            {project_geometry_logic}
                            
                            -- Updating buffers associated with this urban_object_id and non-custom ones
                            FOR v_buffer_type_id, v_buffer_value IN
                                SELECT b.buffer_type_id, d.buffer_value
                                FROM {schema}.buffers_data b
                                JOIN public.default_buffer_values_dict d
                                  ON b.buffer_type_id = d.buffer_type_id AND d.physical_object_type_id = v_physical_object_type_id
                                WHERE b.urban_object_id = v_urban_object_id AND b.is_custom = false
                            LOOP                    
                                
                                {result_geom}
                            
                                -- Update buffer geometry
                                UPDATE public.buffers_data
                                SET geometry = result_geom
                                WHERE urban_object_id = v_urban_object_id AND buffer_type_id = v_buffer_type_id;
                            END LOOP;
                        END LOOP;
    
                        {user_projects_logic}
                        
                    RETURN NEW;
                    END;
                    $$ LANGUAGE plpgsql;
                    """
                )
            )
        )

        op.execute(
            sa.text(
                dedent(
                    f"""
                    CREATE TRIGGER update_buffer_on_update_physical_object_trigger
                    AFTER UPDATE OF physical_object_type_id ON {schema}.physical_objects_data
                    FOR EACH ROW
                    WHEN (OLD.physical_object_type_id IS DISTINCT FROM NEW.physical_object_type_id)
                    EXECUTE FUNCTION {schema}.trigger_update_buffer_on_update_physical_object();
                    """
                )
            )
        )

    # create triggers on update `services_data` (if service_type were changed)
    for schema in ("public", "user_projects"):
        if schema == "public":
            urban_objects = """
                SELECT uod.urban_object_id, ogd.geometry, ST_SRID(ogd.geometry) 
                FROM public.urban_objects_data uod 
                JOIN public.object_geometries_data ogd ON uod.object_geometry_id = ogd.object_geometry_id 
                WHERE uod.service_id = v_service_id 
             """

            project_geometry_logic = ""

            result_geom = """
                result_geom := ST_Difference(
                    ST_Transform(
                        ST_Buffer(v_object_geom::geography, v_buffer_value)::geometry,
                        ST_SRID(v_object_geom)
                    ),
                    v_object_geom
                );

                IF result_geom IS NULL THEN
                    RAISE EXCEPTION 'Resulting geometry is NULL for urban_object_id=%, buffer_type_id=%', NEW.urban_object_id, v_buffer_type_id;
                END IF;
            """

            user_projects_logic = f"""
                FOR v_urban_object_id, v_object_geom, v_srid IN
                    SELECT 
                        uod.urban_object_id, 
                        CASE
                            WHEN uod.object_geometry_id IS NULL THEN pub_ogd.geometry
                            ELSE up_ogd.geometry
                        END,
                        CASE
                            WHEN uod.object_geometry_id IS NULL THEN ST_SRID(pub_ogd.geometry)
                            ELSE ST_SRID(up_ogd.geometry)
                        END
                    FROM user_projects.urban_objects_data uod
                    LEFT JOIN user_projects.object_geometries_data up_ogd ON uod.object_geometry_id = up_ogd.object_geometry_id
                    LEFT JOIN public.object_geometries_data pub_ogd ON uod.public_object_geometry_id = pub_ogd.object_geometry_id
                    WHERE uod.public_service_id = v_service_id

                LOOP

                    {project_geometry_logic}

                    FOR v_buffer_type_id, v_buffer_value IN
                        SELECT b.buffer_type_id, d.buffer_value
                        FROM user_projects.buffers_data b
                        JOIN public.default_buffer_values_dict d
                          ON b.buffer_type_id = d.buffer_type_id
                         AND d.service_type_id = v_service_type_id
                        WHERE b.urban_object_id = v_urban_object_id
                    LOOP

                        IF NOT v_is_regional THEN 
                            result_geom := ST_Intersection(
                                ST_Difference(
                                    ST_Transform(
                                        ST_Buffer(v_object_geom::geography, v_buffer_value)::geometry,
                                        ST_SRID(v_object_geom)
                                    ),
                                    v_object_geom
                                ),
                                v_project_geom
                            );
                        ELSE
                            result_geom := ST_Difference(
                                ST_Transform(
                                    ST_Buffer(v_object_geom::geography, v_buffer_value)::geometry,
                                    ST_SRID(v_object_geom)
                                ),
                                v_object_geom
                            );
                        END IF;
        
                        IF result_geom IS NULL  THEN
                            RAISE EXCEPTION 'Resulting geometry is NULL for urban_object_id=%, buffer_type_id=%', NEW.urban_object_id, v_buffer_type_id;
                        END IF;

                        UPDATE user_projects.buffers_data
                        SET geometry = result_geom
                        WHERE urban_object_id = v_urban_object_id AND buffer_type_id = v_buffer_type_id;
                    END LOOP;
                END LOOP;
            """
        else:
            urban_objects = """
            SELECT uod.urban_object_id, 
                   CASE 
                       WHEN uod.object_geometry_id IS NULL THEN pub_ogd.geometry 
                       ELSE up_ogd.geometry 
                    END, 
                   CASE 
                       WHEN uod.object_geometry_id IS NULL THEN ST_SRID(pub_ogd.geometry) 
                       ELSE ST_SRID(up_ogd.geometry) 
                    END
            FROM user_projects.urban_objects_data uod
                     LEFT JOIN user_projects.object_geometries_data up_ogd 
                               ON uod.object_geometry_id = up_ogd.object_geometry_id
                     LEFT JOIN public.object_geometries_data pub_ogd 
                               ON uod.public_object_geometry_id = pub_ogd.object_geometry_id
            WHERE uod.service_id = v_service_id 
            """

            project_geometry_logic = """
                SELECT ptd.geometry, p.is_regional
                INTO v_project_geom, v_is_regional
                FROM user_projects.urban_objects_data uod
                JOIN user_projects.scenarios_data s ON uod.scenario_id = s.scenario_id
                JOIN user_projects.projects_data p ON s.project_id = p.project_id
                JOIN user_projects.projects_territory_data ptd ON p.project_id = ptd.project_id
                WHERE uod.urban_object_id = v_urban_object_id;
    
                IF v_project_geom IS NULL AND NOT v_is_regional THEN
                    RAISE EXCEPTION 'Could not find project territory geometry for urban_object_id=%', NEW.urban_object_id;
                END IF; 
             """

            result_geom = """
                IF NOT v_is_regional THEN 
                    result_geom := ST_Intersection(
                        ST_Difference(
                            ST_Transform(
                                ST_Buffer(v_object_geom::geography, v_buffer_value)::geometry,
                                ST_SRID(v_object_geom)
                            ),
                            v_object_geom
                        ),
                        v_project_geom
                    );
                ELSE
                    result_geom := ST_Difference(
                        ST_Transform(
                            ST_Buffer(v_object_geom::geography, v_buffer_value)::geometry,
                            ST_SRID(v_object_geom)
                        ),
                        v_object_geom
                    );
                END IF;

                IF result_geom IS NULL  THEN
                    RAISE EXCEPTION 'Resulting geometry is NULL for urban_object_id=%, buffer_type_id=%', NEW.urban_object_id, v_buffer_type_id;
                END IF;
            """

            user_projects_logic = ""

        op.execute(
            sa.text(
                dedent(
                    f"""
                    CREATE OR REPLACE FUNCTION {schema}.trigger_update_buffer_on_update_service()
                    RETURNS TRIGGER AS $$
                    DECLARE
                        v_service_id INT := NEW.service_id;
                        v_service_type_id INT := NEW.service_type_id;

                        v_urban_object_id INT;
                        v_buffer_type_id INT;
                        v_buffer_value FLOAT;
                        v_object_geom GEOMETRY;
                        v_srid INT;
                        result_geom GEOMETRY;
                        v_project_geom GEOMETRY;
                        v_is_regional BOOLEAN;
                    BEGIN
                        -- Go through all urban_object_ids associated with the changed service_id and not having a service_id.
                        FOR v_urban_object_id, v_object_geom, v_srid IN
                            {urban_objects}
                        LOOP

                            {project_geometry_logic}

                            -- Updating buffers associated with this urban_object_id and non-custom ones
                            FOR v_buffer_type_id, v_buffer_value IN
                                SELECT b.buffer_type_id, d.buffer_value
                                FROM {schema}.buffers_data b
                                JOIN public.default_buffer_values_dict d
                                  ON b.buffer_type_id = d.buffer_type_id AND d.service_type_id = v_service_type_id
                                WHERE b.urban_object_id = v_urban_object_id AND b.is_custom = false
                            LOOP                    

                                {result_geom}

                                -- Update buffer geometry
                                UPDATE public.buffers_data
                                SET geometry = result_geom
                                WHERE urban_object_id = v_urban_object_id AND buffer_type_id = v_buffer_type_id;
                            END LOOP;
                        END LOOP;

                        {user_projects_logic}

                    RETURN NEW;
                    END;
                    $$ LANGUAGE plpgsql;
                    """
                )
            )
        )

        op.execute(
            sa.text(
                dedent(
                    f"""
                    CREATE TRIGGER update_buffer_on_update_service_trigger
                    AFTER UPDATE OF service_type_id ON {schema}.services_data
                    FOR EACH ROW
                    WHEN (OLD.service_type_id IS DISTINCT FROM NEW.service_type_id)
                    EXECUTE FUNCTION {schema}.trigger_update_buffer_on_update_service();
                    """
                )
            )
        )


def downgrade() -> None:

    # drop triggers
    for schema in ("public", "user_projects"):
        op.execute(
            sa.text(
                dedent(
                    f"""
                    DROP TRIGGER IF EXISTS set_default_buffer_geometry_trigger
                    ON {schema}.buffers_data;
                    """
                )
            )
        )
        op.execute(sa.text(dedent(f"DROP FUNCTION IF EXISTS {schema}.trigger_set_default_buffer_geometry();")))

        op.execute(
            sa.text(
                dedent(
                    f"""
                    DROP TRIGGER IF EXISTS update_buffers_on_urban_object_trigger
                    ON {schema}.urban_objects_data;
                    """
                )
            )
        )
        op.execute(sa.text(dedent(f"DROP FUNCTION IF EXISTS {schema}.trigger_update_buffers_for_urban_object();")))

        op.execute(
            sa.text(
                dedent(
                    f"""
                    DROP TRIGGER IF EXISTS update_buffer_on_update_physical_object_trigger
                    ON {schema}.physical_objects_data;
                    """
                )
            )
        )
        op.execute(
            sa.text(dedent(f"DROP FUNCTION IF EXISTS {schema}.trigger_update_buffer_on_update_physical_object();"))
        )

        op.execute(
            sa.text(
                dedent(
                    f"""
                    DROP TRIGGER IF EXISTS update_buffer_on_update_service_trigger
                    ON {schema}.services_data;
                    """
                )
            )
        )
        op.execute(sa.text(dedent(f"DROP FUNCTION IF EXISTS {schema}.trigger_update_buffer_on_update_service();")))
