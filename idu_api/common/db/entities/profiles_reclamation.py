from sqlalchemy import Column, Float, ForeignKey, Integer, PrimaryKeyConstraint, Table

from idu_api.common.db import metadata
from idu_api.common.db.entities.urban_types_dicts import functional_zone_types_dict

profiles_reclamation_data = Table(
    "profiles_reclamation_data",
    metadata,
    Column(
        "source_profile_id", Integer, ForeignKey(functional_zone_types_dict.c.functional_zone_type_id), nullable=False
    ),
    Column(
        "target_profile_id", Integer, ForeignKey(functional_zone_types_dict.c.functional_zone_type_id), nullable=False
    ),
    Column("technical_price", Float, nullable=False, unique=False),
    Column("technical_time", Float, nullable=False, unique=False),
    Column("biological_price", Float, nullable=False, unique=False),
    Column("biological_time", Float, nullable=False, unique=False),
    PrimaryKeyConstraint("source_profile_id", "target_profile_id"),
)

"""
Profiles reclamation:
- source_profile_id foreign key int
- target_profile_id foreign key int
- technical_price float 
- technical_time float 
- biological_price float 
- biological_time float 
"""
