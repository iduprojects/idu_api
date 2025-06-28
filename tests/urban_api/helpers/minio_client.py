"""Mock AsyncMinioClient implementation is defined here."""

import io
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest
import structlog

__all__ = ["MockAsyncMinioClient", "mock_minio_client"]


class MockAsyncMinioClient:
    """Mock implementation of AsyncMinioClient for testing purposes."""

    def __init__(self):
        """Initialize the mock with an in-memory store."""
        self._store = {}
        self._bucket_name = "mock_bucket"
        self.upload_file_mock = AsyncMock()
        self.get_files_mock = AsyncMock()
        self.list_objects_mock = AsyncMock()
        self.copy_object_mock = AsyncMock()
        self.generate_presigned_urls_mock = AsyncMock()
        self.delete_file_mock = AsyncMock()

    @asynccontextmanager
    async def get_session_client(self):
        yield AsyncMock()

    async def upload_file(
        self, session, file_data: bytes, object_name: str, logger: structlog.stdlib.BoundLogger
    ) -> str:
        """Mock the upload_file method."""
        await self.upload_file_mock(object_name)
        self._store[object_name] = file_data
        return object_name

    async def get_files(
        self, session, object_names: list[str], logger: structlog.stdlib.BoundLogger
    ) -> list[io.BytesIO]:
        """Mock the get_file method."""
        await self.get_files_mock(object_names)
        if all(name in self.list_objects(session, object_names, logger) for name in object_names):
            return [io.BytesIO(self._store[name]) for name in object_names]
        raise FileNotFoundError("At least one object not found in mock store")

    async def list_objects(self, session, object_names: list[str], logger: structlog.stdlib.BoundLogger) -> bool:
        """Mock the object_exists method."""
        await self.list_objects_mock(object_names)
        return self._store

    async def generate_presigned_urls(
        self, session, object_names: list[str], logger: structlog.stdlib.BoundLogger, expires_in: int = 3600
    ) -> list[str]:
        """Mock the generate_presigned_url method."""
        await self.generate_presigned_urls_mock(object_names)
        if all(name in self.list_objects(session, object_names, logger) for name in object_names):
            return [f"http://mockstorage/{self._bucket_name}/{name}?expires_in={expires_in}" for name in object_names]
        raise FileNotFoundError("At least one object not found in mock store")

    async def delete_file(self, session, object_name: str, logger: structlog.stdlib.BoundLogger) -> None:
        """Mock the delete_file method."""
        await self.delete_file_mock(object_name)
        if object_name in self._store:
            del self._store[object_name]
        else:
            raise FileNotFoundError(f"Object '{object_name}' not found in mock store")


@pytest.fixture
def mock_minio_client():
    return MockAsyncMinioClient()
