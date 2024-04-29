"""
Data Transfer Objects (much like entities from database) are defined in this module.
"""
from .territories import TerritoryTypeDTO, TerritoryDTO
from .users import UserDTO, TokensTuple

__all__ = [
    "TerritoryTypeDTO",
    "TerritoryDTO",
    "UserDTO",
    "TokensTuple"
]
