from dataclasses import dataclass
from typing import Optional

from idu_api.city_api.dto.base import Base


@dataclass()
class ServiceCountDTO(Base):
    service_type_id: int
    name: Optional[str]
    code: Optional[str]
    urban_function_id: int
    count: int
