import io

import pytest

from idu_api.urban_api.exceptions.utils.minio import GetPresignedURLError


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

    async def get_files(self, object_names: list[str]) -> list[io.BytesIO]:
        """Mock the get_file method."""
        if await self.objects_exist(object_names):
            return [io.BytesIO(self._store[name]) for name in object_names]
        raise FileNotFoundError("At least one object not found in mock store.")

    async def objects_exist(self, object_names: list[str]) -> bool:
        """Mock the object_exists method."""
        return all(name in self._store for name in object_names)

    async def generate_presigned_urls(self, object_names: list[str]) -> list[str]:
        """Mock the generate_presigned_url method."""
        if await self.objects_exist(object_names):
            return [f"http://mockstorage/{self._bucket_name}/{name}" for name in object_names]
        raise GetPresignedURLError("At least one object not found in mock store.")

    async def delete_file(self, object_name: str) -> None:
        """Mock the delete_file method."""
        if object_name in self._store:
            del self._store[object_name]
        else:
            raise FileNotFoundError(f"Object '{object_name}' not found in mock store.")


@pytest.fixture
def mock_minio_client():
    return MockAsyncMinioClient()
