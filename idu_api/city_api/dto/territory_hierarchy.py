from dataclasses import dataclass

from idu_api.city_api.dto.base import Base


@dataclass()
class TerritoryHierarchyDTO(Base):
    territory_type_id: int
    territory_type_name: str
    level: int
