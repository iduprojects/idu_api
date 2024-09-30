from dataclasses import dataclass
from typing import Optional

from idu_api.urban_api.dto import TerritoryDTO, TerritoryWithoutGeometryDTO


@dataclass()
class CATerritoryDTO(TerritoryDTO):
    population: Optional[int]


@dataclass(frozen=True)
class CATerritoryWithoutGeometryDTO(TerritoryWithoutGeometryDTO):
    population: Optional[int]
