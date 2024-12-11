import asyncio
import io

import aioboto3
from botocore.client import Config
from botocore.exceptions import ClientError
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed

from idu_api.urban_api.config import UrbanAPIConfig
from idu_api.urban_api.exceptions.utils.minio import (
    DeleteFileError,
    DownloadFileError,
    GetPresignedURLError,
    UploadFileError,
)


class AsyncMinioClient:
    """Asynchronous client for interacting with Minio file server."""

    RETRIES = 3

    def __init__(
        self,
        host: str,
        port: int,
        access_key: str,
        secret_key: str,
        bucket_name: str,
        region_name: str,
        connect_timeout: int,
        read_timeout: int,
        retries: int,
    ):
        """Initialize the Minio client with the configuration parameters."""
        self._bucket_name = bucket_name
        self._endpoint_url = f"http://{host}:{port}"
        self._access_key = access_key
        self._secret_key = secret_key
        self._region_name = region_name
        self._connect_timeout = connect_timeout
        self._read_timeout = read_timeout
        self.RETRIES = retries

    @retry(stop=stop_after_attempt(RETRIES), wait=wait_fixed(1), retry=retry_if_exception_type(ClientError))
    async def upload_file(self, file_data: bytes, object_name: str) -> str:
        """Upload a file from bytes data to the specified bucket asynchronously."""
        async with aioboto3.Session().client(
            "s3",
            endpoint_url=self._endpoint_url,
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._secret_key,
            region_name=self._region_name,
            config=Config(connect_timeout=self._connect_timeout, read_timeout=self._read_timeout),
        ) as client:
            try:
                file_stream = io.BytesIO(file_data)
                await client.upload_fileobj(file_stream, self._bucket_name, object_name)
                return object_name
            except ClientError as exc:
                raise UploadFileError(str(exc)) from exc

    @retry(stop=stop_after_attempt(RETRIES), wait=wait_fixed(1), retry=retry_if_exception_type(ClientError))
    async def get_files(self, object_names: list[str]) -> list[io.BytesIO]:
        """Retrieve a file from the bucket asynchronously and return its content as bytes."""
        async with aioboto3.Session().client(
            "s3",
            endpoint_url=self._endpoint_url,
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._secret_key,
            region_name=self._region_name,
            config=Config(connect_timeout=self._connect_timeout, read_timeout=self._read_timeout),
        ) as client:
            existence_map = await self.objects_exist(object_names)
            final_names = [
                name if existence_map.get(name, False) else "defaultImg.png" if "preview" in name else "defaultImg.jpg"
                for name in object_names
            ]
            try:
                tasks = []
                file_data_map = {}
                for name in final_names:
                    file_data = io.BytesIO()
                    task = client.download_fileobj(self._bucket_name, name, file_data)
                    tasks.append(task)
                    file_data_map[name] = file_data
                await asyncio.gather(*tasks)
                for name, file_data in file_data_map.items():
                    file_data.seek(0)
                return [file_data_map[name] for name in final_names]
            except ClientError as exc:
                raise DownloadFileError(str(exc)) from exc

    @retry(stop=stop_after_attempt(RETRIES), wait=wait_fixed(1), retry=retry_if_exception_type(ClientError))
    async def objects_exist(self, object_names: list[str]) -> dict[str, bool]:
        async with aioboto3.Session().client(
            "s3",
            endpoint_url=self._endpoint_url,
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._secret_key,
            region_name=self._region_name,
            config=Config(connect_timeout=self._connect_timeout, read_timeout=self._read_timeout),
        ) as client:
            response = await client.list_objects_v2(Bucket=self._bucket_name, Prefix="")
            existing_objects = {obj["Key"] for obj in response.get("Contents", [])}
            return {name: name in existing_objects for name in object_names}

    @retry(stop=stop_after_attempt(RETRIES), wait=wait_fixed(1), retry=retry_if_exception_type(ClientError))
    async def generate_presigned_urls(self, object_names: list[str], expires_in: int = 3600) -> list[str]:
        async with aioboto3.Session().client(
            "s3",
            endpoint_url=self._endpoint_url,
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._secret_key,
            region_name=self._region_name,
            config=Config(connect_timeout=self._connect_timeout, read_timeout=self._read_timeout),
        ) as client:
            existence_map = await self.objects_exist(object_names)
            final_names = [
                name if existence_map.get(name, False) else "defaultImg.png" if "preview" in name else "defaultImg.jpg"
                for name in object_names
            ]
            try:
                tasks = [
                    client.generate_presigned_url(
                        "get_object", Params={"Bucket": self._bucket_name, "Key": name}, ExpiresIn=expires_in
                    )
                    for name in final_names
                ]
                return await asyncio.gather(*tasks)
            except ClientError as exc:
                raise GetPresignedURLError(str(exc)) from exc

    @retry(stop=stop_after_attempt(RETRIES), wait=wait_fixed(1), retry=retry_if_exception_type(ClientError))
    async def delete_file(self, object_name: str) -> None:
        """
        Delete a file from the specified bucket asynchronously.

        :param object_name: Name of the file to delete from the bucket.
        :raises DeleteFileError: If the delete operation fails.
        """
        async with aioboto3.Session().resource(
            "s3",
            endpoint_url=self._endpoint_url,
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._secret_key,
            region_name=self._region_name,
            config=Config(connect_timeout=self._connect_timeout, read_timeout=self._read_timeout),
        ) as s3:
            try:
                await (await s3.Bucket(self._bucket_name)).objects.filter(Prefix=object_name).delete()
            except ClientError as exc:
                raise DeleteFileError(str(exc)) from exc


async def get_minio_client() -> AsyncMinioClient:
    app_config = UrbanAPIConfig.from_file_or_default()
    minio_client = AsyncMinioClient(
        host=app_config.fileserver.host,
        port=app_config.fileserver.port,
        access_key=app_config.fileserver.access_key,
        secret_key=app_config.fileserver.secret_key,
        bucket_name=app_config.fileserver.projects_bucket,
        region_name=app_config.fileserver.region_name,
        connect_timeout=app_config.fileserver.connect_timeout,
        read_timeout=app_config.fileserver.read_timeout,
        retries=app_config.fileserver.retries,
    )
    yield minio_client
