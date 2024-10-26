import io
from sys import prefix

import aioboto3
from botocore.exceptions import ClientError

from idu_api.urban_api.config import UrbanAPIConfig
from idu_api.urban_api.exceptions.utils.minio import (
    DeleteFileError,
    DownloadFileError,
    GetPresignedURLError,
    UploadFileError,
)


class AsyncMinioClient:
    """Asynchronous client for interacting with Minio file server."""

    def __init__(self, host: str, port: int, access_key: str, secret_key: str, bucket_name: str):
        """Initialize the Minio client with the configuration parameters."""
        self._bucket_name = bucket_name
        self._endpoint_url = f"http://{host}:{port}"
        self._access_key = access_key
        self._secret_key = secret_key

    async def upload_file(self, file_data: bytes, object_name: str) -> str:
        """
        Upload a file from bytes data to the specified bucket asynchronously.

        :param file_data: The file data in bytes to be uploaded.
        :param object_name: Name for the object in the bucket.
        :return: The name of the uploaded object.
        """
        async with aioboto3.Session().client(
            "s3",
            endpoint_url=self._endpoint_url,
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._secret_key,
        ) as client:
            try:
                # Create a BytesIO stream from the bytes data
                file_stream = io.BytesIO(file_data)

                # Upload the file to the bucket
                await client.upload_fileobj(file_stream, self._bucket_name, object_name)
                return object_name
            except ClientError as exc:
                raise UploadFileError(str(exc)) from exc

    async def get_file(self, object_name: str) -> io.BytesIO:
        """
        Retrieve a file from the bucket asynchronously and return its content as bytes.

        :param object_name: Name of the file to retrieve from the bucket.
        :return: The file data in bytes.
        """
        async with aioboto3.Session().client(
            "s3",
            endpoint_url=self._endpoint_url,
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._secret_key,
        ) as client:
            try:
                # Create a BytesIO stream to store the downloaded data
                file_stream = io.BytesIO()
                try:
                    # Download the file from the bucket
                    await client.download_fileobj(self._bucket_name, object_name, file_stream)
                except ClientError:
                    default_name = "defaultImg.png" if "preview" in object_name else "defaultImg.jpg"
                    await client.download_fileobj(self._bucket_name, default_name, file_stream)

                # Get the bytes data from the stream
                file_stream.seek(0)
                return file_stream
            except ClientError as exc:
                raise DownloadFileError(str(exc)) from exc

    async def get_presigned_url(self, object_name: str, expires_in: int = 3600) -> str:
        """
        Generate a presigned URL for downloading the file asynchronously.

        :param object_name: Name of the file in the bucket.
        :param expires_in: Time in seconds for which the URL should be valid.
        :return: Presigned URL for the file.
        """
        async with aioboto3.Session().client(
            "s3",
            endpoint_url=self._endpoint_url,
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._secret_key,
        ) as client:
            try:
                # Generate a presigned URL
                return await client.generate_presigned_url(
                    "get_object", Params={"Bucket": self._bucket_name, "Key": object_name}, ExpiresIn=expires_in
                )
            except ClientError as exc:
                raise GetPresignedURLError(str(exc)) from exc

    async def delete_file(self, object_name: str) -> None:
        """
        Delete a file from the specified bucket asynchronously.

        :param object_name: Name of the file to delete from the bucket.
        :raises ClientError: If the delete operation fails.
        """
        async with aioboto3.Session().resource(
            "s3",
            endpoint_url=self._endpoint_url,
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._secret_key,
        ) as s3:
            try:
                # Delete the file from the bucket
                await (await s3.Bucket(self._bucket_name)).objects.filter(Prefix=object_name).delete()
            except ClientError as exc:
                raise DeleteFileError(str(exc)) from exc


async def get_minio_client() -> AsyncMinioClient:
    app_config = UrbanAPIConfig.try_from_env()
    minio_client = AsyncMinioClient(
        host=app_config.fileserver.host,
        port=app_config.fileserver.port,
        access_key=app_config.fileserver.access_key,
        secret_key=app_config.fileserver.secret_key,
        bucket_name=app_config.fileserver.projects_bucket,
    )
    yield minio_client
