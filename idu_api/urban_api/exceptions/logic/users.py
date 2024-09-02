"""
Exceptions connected with user's logic are defined here.
"""

from fastapi import status

from idu_api.urban_api.exceptions import IduApiError


class AccessDeniedError(IduApiError):
    """
    Exception to raise when you do not have access rights to a resource.
    """

    def __init__(self, requested_id: int, entity: str):
        """
        Construct from requested identifier and entity (table) name.
        """
        self.requested_id = requested_id
        self.entity = entity
        super().__init__()

    def __str__(self) -> str:
        return f"Access for entity '{self.entity}' with id={self.requested_id} is denied"

    def get_status_code(self) -> int:
        """
        Return '403 Forbidden' status code.
        """
        return status.HTTP_403_FORBIDDEN
