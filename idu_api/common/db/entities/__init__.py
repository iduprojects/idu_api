"""
Module to store all the database tables.
"""

from idu_api.common.db.entities.buffers import buffer_types_dict, buffers_data
from idu_api.common.db.entities.functional_zones import functional_zone_types_dict, functional_zones_data
from idu_api.common.db.entities.indicators_dict import indicators_dict, measurement_units_dict
from idu_api.common.db.entities.indicators_groups import indicators_groups_data, indicators_groups_dict
from idu_api.common.db.entities.living_buildings import buildings_data
from idu_api.common.db.entities.object_geometries import object_geometries_data
from idu_api.common.db.entities.physical_object_types import (
    object_service_types_dict,
    physical_object_functions_dict,
    physical_object_types_dict,
)
from idu_api.common.db.entities.physical_objects import physical_objects_data
from idu_api.common.db.entities.profiles_reclamation import profiles_reclamation_data
from idu_api.common.db.entities.projects.functional_zones import projects_functional_zones
from idu_api.common.db.entities.projects.hexagons import hexagons_data
from idu_api.common.db.entities.projects.indicators import projects_indicators_data
from idu_api.common.db.entities.projects.living_buildings import projects_buildings_data
from idu_api.common.db.entities.projects.object_geometries import projects_object_geometries_data
from idu_api.common.db.entities.projects.physical_objects import projects_physical_objects_data
from idu_api.common.db.entities.projects.projects import projects_data, projects_phases_data
from idu_api.common.db.entities.projects.projects_territory import projects_territory_data
from idu_api.common.db.entities.projects.scenarios import scenarios_data
from idu_api.common.db.entities.projects.services import projects_services_data
from idu_api.common.db.entities.projects.urban_objects import projects_urban_objects_data
from idu_api.common.db.entities.service_types import service_types_dict, urban_functions_dict
from idu_api.common.db.entities.service_types_normatives import service_types_normatives_data
from idu_api.common.db.entities.services import services_data
from idu_api.common.db.entities.soc_groups import (
    soc_group_values_data,
    soc_groups_dict,
    soc_value_indicators_data,
    soc_values_dict,
    soc_values_service_types_dict,
)
from idu_api.common.db.entities.territories import target_city_types_dict, territories_data, territory_types_dict
from idu_api.common.db.entities.territory_indicators import territory_indicators_data
from idu_api.common.db.entities.urban_objects import urban_objects_data
