"""
Exceptions connected with user's logic are defined here.
"""

from fastapi import status

from idu_api.urban_api.exceptions import IduApiError


class InvalidImageError(IduApiError):
    """
    Exception to raise when you do not have access rights to a resource.
    """

    def __init__(self, project_id: int):
        """
        Construct from requested identifier and entity (table) name.
        """
        self.project_id = project_id
        super().__init__()

    def __str__(self) -> str:
        return f"You uploaded invalid image for project with id = {self.project_id}."

    def get_status_code(self) -> int:
        """
        Return '400 Forbidden' status code.
        """
        return status.HTTP_400_BAD_REQUEST
