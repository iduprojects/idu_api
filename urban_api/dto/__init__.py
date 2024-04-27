"""
Data Transfer Objects (much like entities from database) are defined in this module.
"""
from .notes import NoteDto
from .users import UserDTO, TokensTuple

__all__ = [
    "NoteDto",
    "UserDTO",
    "TokensTuple"
]
