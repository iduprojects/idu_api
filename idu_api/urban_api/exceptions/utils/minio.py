"""
Exceptions connected with minio client are defined here.
"""

from fastapi import status

from idu_api.urban_api.exceptions import IduApiError


class UploadFileError(IduApiError):
    """
    Exception to raise when uploading file to server was failed.
    """

    def __init__(self, exc: str):
        """
        Construct from requested identifier and entity (table) name.
        """
        self.exc = exc
        super().__init__()

    def __str__(self) -> str:
        return f"Failed to upload file to bucket: {self.exc}."

    def get_status_code(self) -> int:
        """
        Return '503 SERVICE UNAVAILABLE' status code.
        """
        return status.HTTP_503_SERVICE_UNAVAILABLE


class DownloadFileError(IduApiError):
    """
    Exception to raise when downloading file from server was failed.
    """

    def __init__(self, exc: str):
        """
        Construct from requested identifier and entity (table) name.
        """
        self.exc = exc
        super().__init__()

    def __str__(self) -> str:
        return f"Failed to download file from bucket: {self.exc}."

    def get_status_code(self) -> int:
        """
        Return '503 SERVICE UNAVAILABLE' status code.
        """
        return status.HTTP_503_SERVICE_UNAVAILABLE


class GetPresignedURLError(IduApiError):
    """
    Exception to raise when generating presigned url for requested file from server was failed.
    """

    def __init__(self, exc: str):
        """
        Construct from requested identifier and entity (table) name.
        """
        self.exc = exc
        super().__init__()

    def __str__(self) -> str:
        return f"Failed to generate presigned URL: {self.exc}."

    def get_status_code(self) -> int:
        """
        Return '503 SERVICE UNAVAILABLE' status code.
        """
        return status.HTTP_503_SERVICE_UNAVAILABLE


class DeleteFileError(IduApiError):
    """
    Exception to raise when deleting file from server was failed.
    """

    def __init__(self, exc: str):
        """
        Construct from requested identifier and entity (table) name.
        """
        self.exc = exc
        super().__init__()

    def __str__(self) -> str:
        return f"Failed to delete file from bucket: {self.exc}."

    def get_status_code(self) -> int:
        """
        Return '503 SERVICE UNAVAILABLE' status code.
        """
        return status.HTTP_503_SERVICE_UNAVAILABLE
