import io

import pytest


class MockAsyncMinioClient:
    """Mock implementation of AsyncMinioClient for testing purposes."""

    def __init__(self):
        """Initialize the mock with an in-memory store."""
        self._store = {}
        self._bucket_name = "mock_bucket"

    async def upload_file(self, file_data: bytes, object_name: str) -> str:
        """Mock the upload_file method."""
        self._store[object_name] = file_data
        return object_name

    async def get_file(self, object_name: str) -> io.BytesIO:
        """Mock the get_file method."""
        if object_name in self._store:
            return io.BytesIO(self._store[object_name])
        else:
            raise FileNotFoundError(f"Object '{object_name}' not found in mock store.")

    async def object_exists(self, bucket_name: str, object_name: str) -> bool:
        """Mock the object_exists method."""
        return object_name in self._store

    async def get_presigned_url(self, object_name: str, expires_in: int = 3600) -> str:
        """Mock the get_presigned_url method."""
        if object_name not in self._store:
            raise FileNotFoundError(f"Object '{object_name}' not found in mock store.")
        return f"http://mockstorage/{self._bucket_name}/{object_name}"

    async def delete_file(self, object_name: str) -> None:
        """Mock the delete_file method."""
        if object_name in self._store:
            del self._store[object_name]
        else:
            raise FileNotFoundError(f"Object '{object_name}' not found in mock store.")


@pytest.fixture
def mock_minio_client():
    return MockAsyncMinioClient()
