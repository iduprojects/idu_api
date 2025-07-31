"""Asynchronous MinIO client is defined here."""

import asyncio
import io
from contextlib import asynccontextmanager

import aioboto3
import structlog
from botocore.config import Config
from botocore.exceptions import EndpointConnectionError

from idu_api.urban_api.config import UrbanAPIConfig
from idu_api.urban_api.exceptions.utils.external import ExternalServiceUnavailable


class AsyncMinioClient:
    """Asynchronous client for interacting with Minio file server."""

    def __init__(
        self,
        url: str,
        access_key: str,
        secret_key: str,
        bucket_name: str,
        region_name: str,
        connect_timeout: int,
        read_timeout: int,
    ):
        """Initialize the Minio client with the configuration parameters."""
        self._bucket_name = bucket_name
        self._endpoint_url = url
        self._access_key = access_key
        self._secret_key = secret_key
        self._region_name = region_name
        self._connect_timeout = connect_timeout
        self._read_timeout = read_timeout

    @asynccontextmanager
    async def get_session(self):
        """Create and return an aioboto3 session client."""
        session = aioboto3.Session()
        async with session.client(
            "s3",
            endpoint_url=self._endpoint_url,
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._secret_key,
            region_name=self._region_name,
            config=self._get_config(),
        ) as client:
            yield client

    def _get_config(self) -> Config:
        """Create and return a botocore Config object."""
        return Config(
            connect_timeout=self._connect_timeout,
            read_timeout=self._read_timeout,
        )

    async def upload_file(
        self,
        session,
        file_data: bytes,
        object_name: str,
        logger: structlog.stdlib.BoundLogger,
    ) -> str:
        """Upload a file from bytes data to the specified bucket asynchronously."""
        try:
            file_stream = io.BytesIO(file_data)
            await session.upload_fileobj(file_stream, self._bucket_name, object_name)
            return object_name
        except EndpointConnectionError as exc:
            await logger.aerror("could not connect to MinIO fileserver")
            raise ExternalServiceUnavailable("fileserver") from exc
        except Exception as exc:
            await logger.aexception("unexpected error in AsyncMinioClient")
            raise exc

    async def list_objects(
        self,
        session,
        logger: structlog.stdlib.BoundLogger,
        prefix: str = "",
    ) -> list[str]:
        try:
            response = await session.list_objects_v2(Bucket=self._bucket_name, Prefix=prefix)
            existing_objects = {obj["Key"] for obj in response.get("Contents", [])}
            return list(existing_objects)
        except EndpointConnectionError as exc:
            await logger.aerror("could not connect to MinIO fileserver")
            raise ExternalServiceUnavailable("fileserver") from exc
        except Exception as exc:
            await logger.aexception("unexpected error in AsyncMinioClient")
            raise exc

    async def get_files(
        self, session, object_names: list[str], logger: structlog.stdlib.BoundLogger
    ) -> list[io.BytesIO]:
        """Retrieve a file from the bucket asynchronously and return its content as bytes."""
        try:
            tasks, file_data_map = [], {}
            for name in object_names:
                file_data = io.BytesIO()
                task = session.download_fileobj(self._bucket_name, name, file_data)
                tasks.append(task)
                file_data_map[name] = file_data
            await asyncio.gather(*tasks)
            for name, file_data in file_data_map.items():
                file_data.seek(0)
            return [file_data_map[name] for name in object_names]
        except EndpointConnectionError as exc:
            await logger.aerror("could not connect to MinIO fileserver")
            raise ExternalServiceUnavailable("fileserver") from exc
        except Exception as exc:
            await logger.aexception("unexpected error in AsyncMinioClient")
            raise exc

    async def generate_presigned_urls(
        self,
        session,
        object_names: list[str],
        logger: structlog.stdlib.BoundLogger,
        expires_in: int = 3600,
    ) -> list[str]:
        try:
            tasks = [
                session.generate_presigned_url(
                    "get_object", Params={"Bucket": self._bucket_name, "Key": name}, ExpiresIn=expires_in
                )
                for name in object_names
            ]
            return await asyncio.gather(*tasks)
        except EndpointConnectionError as exc:
            await logger.aerror("could not connect to MinIO fileserver")
            raise ExternalServiceUnavailable("fileserver") from exc
        except Exception as exc:
            await logger.aexception("unexpected error in AsyncMinioClient")
            raise exc

    async def copy_object(
        self,
        session,
        old_key: str,
        new_key: str,
        logger: structlog.stdlib.BoundLogger,
    ):
        try:
            copy_source = {"Bucket": self._bucket_name, "Key": old_key}
            await session.copy_object(Bucket=self._bucket_name, CopySource=copy_source, Key=new_key)
        except EndpointConnectionError as exc:
            await logger.aerror("could not connect to MinIO fileserver")
            raise ExternalServiceUnavailable("fileserver") from exc
        except Exception as exc:
            await logger.aexception("unexpected error in AsyncMinioClient")
            raise exc

    async def delete_file(
        self,
        session,
        object_name: str,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        """Delete a file from the specified bucket asynchronously."""
        try:
            await session.delete_object(Bucket=self._bucket_name, Key=object_name)
        except EndpointConnectionError as exc:
            await logger.aerror("could not connect to MinIO fileserver")
            raise ExternalServiceUnavailable("fileserver") from exc
        except Exception as exc:
            await logger.aexception("unexpected error in AsyncMinioClient")
            raise exc


def get_minio_client_from_config(app_config: UrbanAPIConfig) -> AsyncMinioClient:
    minio_client = AsyncMinioClient(
        url=app_config.fileserver.url,
        access_key=app_config.fileserver.access_key,
        secret_key=app_config.fileserver.secret_key,
        bucket_name=app_config.fileserver.projects_bucket,
        region_name=app_config.fileserver.region_name,
        connect_timeout=app_config.fileserver.connect_timeout,
        read_timeout=app_config.fileserver.read_timeout,
    )
    return minio_client


def get_minio_client() -> AsyncMinioClient:
    return get_minio_client_from_config(UrbanAPIConfig.from_file_or_default())
