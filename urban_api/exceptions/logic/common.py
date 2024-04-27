"""
Exceptions connected with geometry are defined here.
"""
from fastapi import status

from urban_api.exceptions import NotesApiError


class EntityNotFoundById(NotesApiError):
    """
    Exception to raise when requested entity was not found in the database by the identifier.
    """

    def __init__(self, requested_id: int, entity: str):
        """
        Construct from requested identifier and entity (table) name.
        """
        self.requested_id = requested_id
        self.entity = entity
        super().__init__()

    def __str__(self) -> str:
        return f"Entity '{self.entity}' with id={self.requested_id} is not found"

    def get_status_code(self) -> int:
        """
        Return '404 Not found' status code.
        """
        return status.HTTP_404_NOT_FOUND


class EntityOwnerError(NotesApiError):
    """
    Exception to raise when requested entity didn't belong to the user.
    """

    def __init__(self, requested_id: int, entity: str):
        """
        Construct from requested identifier and entity (table) name.
        """
        self.requested_id = requested_id
        self.entity = entity
        super().__init__()

    def get_status_code(self) -> int:
        """
        Return 400 Bad Request http code.
        """
        return status.HTTP_400_BAD_REQUEST

    def __str__(self) -> str:
        return f"Entity '{self.entity}' with id={self.requested_id} don't belong to you"
