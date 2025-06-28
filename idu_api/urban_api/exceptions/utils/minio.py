"""
Exceptions connected with image and Pillow library are defined here.
"""

from fastapi import status

from idu_api.urban_api.exceptions import IduApiError


class InvalidImageError(IduApiError):
    """
    Exception to raise when user upload invalid image.
    """

    def __init__(self, project_id: int):
        """
        Construct from requested project identifier.
        """
        self.project_id = project_id
        super().__init__()

    def __str__(self) -> str:
        return f"Invalid image for project with id = {self.project_id} was uploaded."

    def get_status_code(self) -> int:
        """
        Return '400 Bad Request' status code.
        """
        return status.HTTP_400_BAD_REQUEST


class FileNotFound(IduApiError):
    """
    Exception to raise when file with given name was not found for specified project.
    """

    def __init__(self, project_id: int, filename: str):
        """
        Construct from requested project and image identifiers.
        """
        self.project_id = project_id
        self.filename = filename
        super().__init__()

    def __str__(self) -> str:
        return f"File `{self.filename}` was not found for project with id = {self.project_id}."

    def get_status_code(self) -> int:
        """
        Return '404 Not Found' status code.
        """
        return status.HTTP_404_NOT_FOUND
