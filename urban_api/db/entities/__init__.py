"""
Module to store all the database tables.
"""

from urban_api.db.entities.functional_zones import functional_zones_data
from urban_api.db.entities.indicators_dict import indicators_dict, measurement_units_dict
from urban_api.db.entities.living_buildings import living_buildings_data
from urban_api.db.entities.object_geometries import object_geometries_data
from urban_api.db.entities.physical_objects import physical_objects_data
from urban_api.db.entities.service_types_normatives import service_types_normatives_data
from urban_api.db.entities.services import services_data
from urban_api.db.entities.territories import territories_data
from urban_api.db.entities.territory_indicators import territory_indicators_data
from urban_api.db.entities.urban_functions import urban_functions_dict
from urban_api.db.entities.urban_objects import urban_objects_data
from urban_api.db.entities.urban_types_dicts import (
    functional_zone_types_dict,
    physical_object_types_dict,
    service_types_dict,
    territory_types_dict,
)
