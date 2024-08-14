"""
Module to store all the database tables.
"""

from idu_api.common.db.entities.functional_zones import functional_zones_data
from idu_api.common.db.entities.indicators_dict import indicators_dict, measurement_units_dict
from idu_api.common.db.entities.living_buildings import living_buildings_data
from idu_api.common.db.entities.object_geometries import object_geometries_data
from idu_api.common.db.entities.physical_objects import physical_objects_data
from idu_api.common.db.entities.projects_data import projects_data
from idu_api.common.db.entities.projects_territory_data import projects_territory_data
from idu_api.common.db.entities.service_types_normatives import service_types_normatives_data
from idu_api.common.db.entities.services import services_data
from idu_api.common.db.entities.territories import territories_data
from idu_api.common.db.entities.territory_indicators import territory_indicators_data
from idu_api.common.db.entities.urban_functions import urban_functions_dict
from idu_api.common.db.entities.urban_objects import urban_objects_data
from idu_api.common.db.entities.urban_types_dicts import (
    functional_zone_types_dict,
    physical_object_types_dict,
    service_types_dict,
    territory_types_dict,
)
