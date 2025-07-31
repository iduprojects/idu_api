import asyncio
import io
import json
import uuid
from io import BytesIO
from typing import Literal

import aioboto3
from PIL import Image
from structlog.stdlib import BoundLogger

from idu_api.urban_api.config import UrbanAPIConfig
from idu_api.urban_api.exceptions.utils.minio import FileNotFound, InvalidImageError
from idu_api.urban_api.minio.client import AsyncMinioClient, get_minio_client_from_config
from idu_api.urban_api.schemas.enums import ProjectPhase

# ========== Image Utilities ==========


def make_image(file: bytes) -> Image.Image:
    """Convert a byte stream into a PIL Image, converting RGBA to RGB if necessary."""
    image = Image.open(io.BytesIO(file))
    if image.mode == "RGBA":
        image = image.convert("RGB")
    return image


def make_preview(image: Image.Image, max_dimension: int = 1600) -> bytes:
    """Resize image proportionally to fit within max dimensions. Return JPEG bytes."""
    width, height = image.size
    if width > max_dimension or height > max_dimension:
        ratio = min(max_dimension / width, max_dimension / height)
        new_size = (int(width * ratio), int(height * ratio))
        image = image.resize(new_size, Image.Resampling.LANCZOS)

    preview_stream = io.BytesIO()
    image.save(preview_stream, format="JPEG", quality=85)
    preview_stream.seek(0)
    return preview_stream.getvalue()


# ========== Storage Manager ==========


class ProjectStorageManager:
    """
    Manages the file storage for urban projects in MinIO.

    This includes:
    - Gallery images (originals and previews)
    - Main project image
    - Project logo
    - Scenario phase documents
    - Project-level metadata stored as JSON
    """

    def __init__(self, app_config: UrbanAPIConfig):
        """
        Initialize storage manager with application configuration.

        Args:
            app_config: Instance of UrbanAPIConfig containing MinIO settings.
        """
        self._client: AsyncMinioClient = get_minio_client_from_config(app_config)

    # ========== Prefix Helpers ==========

    @staticmethod
    def _project_prefix(project_id: int) -> str:
        return f"{project_id}/"

    @staticmethod
    def _gallery_original_prefix(project_id: int) -> str:
        return f"{project_id}/gallery/original/"

    @staticmethod
    def _gallery_preview_prefix(project_id: int) -> str:
        return f"{project_id}/gallery/preview/"

    @staticmethod
    def _logo_prefix(project_id: int) -> str:
        return f"{project_id}/logo/"

    @staticmethod
    def _phase_prefix(project_id: int, phase: str) -> str:
        return f"{project_id}/phases/{phase}/"

    @staticmethod
    def _metadata_path(project_id: int) -> str:
        return f"{project_id}/metadata.json"

    # ========== Metadata ==========

    async def load_list_of_metadata(self, session: aioboto3.Session, ids: list[int], logger: BoundLogger) -> list[dict]:
        """
        Load metadata JSON for multiple projects.

        Args:
            session: aioboto3.Session instance.
            ids: list of project identifiers.
            logger: Structlog logger.

        Returns:
            A list of metadata dictionaries.
        """
        try:
            paths = [self._metadata_path(project_id) for project_id in ids]
            files = await self._client.get_files(session, paths, logger)
            return [json.loads(f.read().decode("utf-8")) for f in files]
        except Exception as exc:
            await logger.aexception("Failed to load metadata", project_ids=ids, error=repr(exc))
            raise FileNotFound(ids, "metadata.json") from exc

    async def load_metadata(self, session, project_id: int, logger: BoundLogger) -> dict:
        """
        Load metadata JSON for a single project.

        Args:
            session: aioboto3 base client instance.
            project_id: The project identifier.
            logger: Structlog logger.

        Returns:
            The project metadata dictionary.
        """
        return (await self.load_list_of_metadata(session, [project_id], logger))[0]

    async def save_metadata(self, session, project_id: int, metadata: dict, logger: BoundLogger) -> None:
        """
        Save metadata JSON for a given project.

        Args:
            session: aioboto3 base client instance.
            project_id: The project identifier.
            metadata: The metadata to save.
            logger: Structlog logger.
        """
        try:
            if session is ...:
                session = self._client.get_session()
            content = json.dumps(metadata).encode("utf-8")
            await self._client.upload_file(session, content, self._metadata_path(project_id), logger)
        except Exception:
            await logger.aexception("Failed to save metadata", project_id=project_id)
            raise

    # ========== Project Management ==========

    async def init_project(self, project_id: int, logger: BoundLogger):
        """
        Create metadata JSON for a given project and upload it to file server.

        Args:
            project_id: Identifier of the project.
            logger: Structlog logger.
        """
        metadata = {"main_image_id": None, "gallery_images": []}
        async with self._client.get_session() as session:
            await self.save_metadata(session, project_id, metadata, logger)

    async def delete_project(self, project_id: int, logger: BoundLogger):
        """
        Remove all files for a given project from file server.

        Args:
            project_id: Identifier of the project.
            logger: Structlog logger.
        """
        async with self._client.get_session() as session:
            existing_objects = await self._client.list_objects(session, logger, prefix=f"{project_id}/")
            tasks = [self._client.delete_file(session, name, logger) for name in existing_objects]
            await asyncio.gather(*tasks)

    # ========== Gallery Management ==========

    async def upload_gallery_image(
        self,
        project_id: int,
        file: bytes,
        logger: BoundLogger,
        set_main: bool = False,
    ) -> str:
        """
        Upload a gallery image (original + preview) to the project.

        Args:
            project_id: Identifier of the project.
            file: Raw bytes of the uploaded image.
            logger: Structlog logger.
            set_main: If true, also set image as the main one.

        Returns:
            A presigned URL to access the preview image.
        """
        image_id = str(uuid.uuid4())
        original_name = f"{self._gallery_original_prefix(project_id)}{image_id}.jpg"
        preview_name = f"{self._gallery_preview_prefix(project_id)}{image_id}.jpg"

        try:
            image = make_image(file)
        except Exception as exc:
            raise InvalidImageError(project_id) from exc

        original_stream = io.BytesIO()
        image.save(original_stream, format="JPEG")
        original_bytes = original_stream.getvalue()
        preview_bytes = make_preview(image)

        async with self._client.get_session() as session:
            await asyncio.gather(
                self._client.upload_file(session, original_bytes, original_name, logger),
                self._client.upload_file(session, preview_bytes, preview_name, logger),
            )

            metadata = await self.load_metadata(session, project_id, logger)
            metadata.setdefault("gallery_images", []).append(image_id)
            if set_main:
                metadata["main_image_id"] = image_id
            await self.save_metadata(session, project_id, metadata, logger)

        return (await self._client.generate_presigned_urls(session, [preview_name], logger))[0]

    async def get_list_gallery_images_urls(self, project_id: int, logger: BoundLogger) -> list[str]:
        """
        list all valid gallery image preview URLs for the project.

        Args:
            project_id: Identifier of the project.
            logger: Structlog logger.

        Returns:
            A list of presigned preview URLs.
        """
        gallery_prefix = self._gallery_preview_prefix(project_id)
        async with self._client.get_session() as session:
            metadata = await self.load_metadata(session, project_id, logger)
            main_image_id = metadata.get("main_image_id")
            if metadata.get("gallery_images"):
                object_names = sorted(metadata.get("gallery_images", []), key=lambda i: i != main_image_id)
                final_names = [gallery_prefix + name + ".jpg" for name in object_names]
            else:
                final_names = ["defaultImg.jpg"]

            return await self._client.generate_presigned_urls(session, final_names, logger)

    async def get_gallery_image(
        self,
        project_id: int,
        image_id: str | None,
        logger: BoundLogger,
        image_type: Literal["original", "preview"] = "preview",
    ) -> BytesIO:
        """
        Get the image from project's gallery by given identifier.

        Args:
            project_id: Project identifier.
            image_id: Image identifier.
            logger: Structlog logger.
            image_type: To get original or preview image.

        Raises:
            FileNotFound: If the image identifier is not in gallery.

        Returns:
            Bytes-like object of the image.
        """
        prefix = (
            self._gallery_preview_prefix(project_id)
            if image_type == "preview"
            else self._gallery_original_prefix(project_id)
        )
        async with self._client.get_session() as session:
            metadata = await self.load_metadata(session, project_id, logger)
            image_id = image_id or metadata.get("main_image_id")
            object_name = f"{prefix}{image_id}.jpg"
            if image_id is None:
                object_name = "defaultImg.jpg"
            elif image_id not in metadata["gallery_images"]:
                raise FileNotFound(project_id, f"{image_id}.jpg")
            return (await self._client.get_files(session, [object_name], logger))[0]

    async def get_gallery_image_url(
        self,
        project_id: int,
        image_id: str | None,
        logger: BoundLogger,
        image_type: Literal["original", "preview"] = "preview",
    ) -> str | None:
        """
        Get the presigned URL of the project's gallery image by given identifier.

        Args:
            project_id: Project identifier.
            image_id: Image identifier.
            logger: Structlog logger.
            image_type: To get original or preview image URL.

        Raises:
            FileNotFound: If the image identifier is not in gallery.

        Returns:
            URL to the logo image, or None if it doesn't exist.
        """
        prefix = (
            self._gallery_preview_prefix(project_id)
            if image_type == "preview"
            else self._gallery_original_prefix(project_id)
        )
        async with self._client.get_session() as session:
            metadata = await self.load_metadata(session, project_id, logger)
            image_id = image_id or metadata.get("main_image_id")
            object_name = f"{prefix}{image_id}.jpg"
            if image_id is None:
                object_name = "defaultImg.jpg"
            elif image_id not in metadata["gallery_images"]:
                raise FileNotFound(project_id, f"{image_id}.jpg")
            return (await self._client.generate_presigned_urls(session, [object_name], logger))[0]

    async def set_main_image(self, project_id: int, image_id: str, logger: BoundLogger) -> None:
        """
        Set one of the gallery images as the main project image.

        Args:
            project_id: Project identifier.
            image_id: identifier of the gallery image to use as main.
            logger: Structlog logger.

        Raises:
            FileNotFound: If the image identifier is not in gallery.
        """
        async with self._client.get_session() as session:
            metadata = await self.load_metadata(session, project_id, logger)
            prefix = self._gallery_preview_prefix(project_id)
            existing_objects = await self._client.list_objects(session, logger, prefix=prefix)
            if not any(image_id in image for image in existing_objects):
                raise FileNotFound(project_id, f"{image_id}.jpg")

            metadata["main_image_id"] = image_id
            await self.save_metadata(session, project_id, metadata, logger)

    async def get_main_images_urls(self, ids: list[int], logger: BoundLogger) -> list[dict[str, str]]:
        """
        Get main image URLs for a list of project identifiers.

        Args:
            ids: list of project identifiers.
            logger: Structlog logger.

        Returns:
            A list of dictionaries with "project_id" and "url".
        """
        async with self._client.get_session() as session:
            list_of_metadata = await self.load_list_of_metadata(session, ids, logger)
            object_names = []
            for project_id, metadata in zip(ids, list_of_metadata):
                image_id = metadata.get("main_image_id")
                object_names.append(f"{self._gallery_preview_prefix(project_id)}{image_id}.jpg" if image_id else None)

            existing_objects = await self._client.list_objects(session, logger)
            valid_names = [name if name in existing_objects else "defaultImg.jpg" for name in object_names]
            urls = await self._client.generate_presigned_urls(session, valid_names, logger)

        return [{"project_id": pid, "url": url} for pid, url in zip(ids, urls) if url is not None]

    async def delete_gallery_image(self, project_id: int, image_id: str, logger: BoundLogger) -> None:
        """
        Delete a gallery image and update metadata.

        Args:
            project_id: identifier of the project.
            image_id: identifier of the image to delete.
            logger: Structlog logger.
        """
        original_name = f"{self._gallery_original_prefix(project_id)}original/{image_id}.jpg"
        preview_name = f"{self._gallery_preview_prefix(project_id)}{image_id}.jpg"

        async with self._client.get_session() as session:
            metadata = await self.load_metadata(session, project_id, logger)
            if image_id not in metadata["gallery_images"]:
                raise FileNotFound(project_id, f"{image_id}.jpg")

            await asyncio.gather(
                self._client.delete_file(session, original_name, logger),
                self._client.delete_file(session, preview_name, logger),
            )

            if metadata.get("main_image_id") == image_id:
                metadata["main_image_id"] = metadata["gallery_images"][0] if metadata["gallery_images"] else None
            await self.save_metadata(session, project_id, metadata, logger)

    # ========== Logo Management ==========

    async def upload_logo(self, project_id: int, file_data: bytes, file_ext: str, logger: BoundLogger) -> str:
        """
        Upload a logo image for the project.

        Args:
            project_id: Project identifier.
            file_data: Raw bytes of the image.
            file_ext: File extension.
            logger: Structlog logger.

        Returns:
            A presigned URL to access the uploaded logo.
        """
        logo_path = f"{self._logo_prefix(project_id)}image.{file_ext}"
        async with self._client.get_session() as session:
            existing_objects = await self._client.list_objects(session, logger, prefix=self._logo_prefix(project_id))
            if existing_objects:
                await self._client.delete_file(session, existing_objects[0], logger)

            await self._client.upload_file(session, file_data, logo_path, logger)

            return (await self._client.generate_presigned_urls(session, [logo_path], logger))[0]

    async def get_logo_url(self, project_id: int, logger: BoundLogger) -> str | None:
        """
        Get the presigned URL of the project logo.

        Args:
            project_id: Project identifier.
            logger: Structlog logger.

        Returns:
            URL to the logo image, or None if it doesn't exist.
        """
        async with self._client.get_session() as session:
            existing_objects = await self._client.list_objects(session, logger, prefix=self._logo_prefix(project_id))
            if not existing_objects:
                existing_objects = ["defaultLogo.jpg"]
            return (await self._client.generate_presigned_urls(session, existing_objects, logger))[0]

    async def delete_logo(self, project_id: int, logger: BoundLogger) -> None:
        """
        Delete the logo image of the project.

        Args:
            project_id: Project identifier.
            logger: Structlog logger.
        """
        async with self._client.get_session() as session:
            logos = await self._client.list_objects(session, logger, prefix=self._logo_prefix(project_id))
            if not logos:
                raise FileNotFound(project_id, "logo")
            await self._client.delete_file(session, logos[0], logger)

    # ========== Phase Documents ==========

    async def upload_phase_document(
        self,
        project_id: int,
        phase: ProjectPhase,
        file_data: bytes,
        file_name: str,
        file_ext: str,
        logger: BoundLogger,
    ) -> str:
        """
        Upload a document to the specified phase. Adds suffix if name already exists.

        Args:
            project_id: Project identifier.
            phase: Project phase (enum).
            file_data: File content in bytes.
            file_name: Base filename (without extension).
            file_ext: File extension (e.g. 'pdf', 'docx').
            logger: Structlog logger.

        Returns:
            A presigned URL to the uploaded file.
        """
        base_name = f"{file_name}.{file_ext}"
        object_name = f"{self._phase_prefix(project_id, phase)}{base_name}"
        async with self._client.get_session() as session:
            existing_objects = await self._client.list_objects(session, logger, prefix=self._logo_prefix(project_id))
            i = 1
            while object_name in existing_objects:
                object_name = f"{self._phase_prefix(project_id, phase)}{file_name} ({i}).{file_ext}"
                i += 1

            await self._client.upload_file(session, file_data, object_name, logger)
            return (await self._client.generate_presigned_urls(session, [object_name], logger))[0]

    async def get_phase_document_urls(self, project_id: int, phase: ProjectPhase, logger: BoundLogger) -> list[str]:
        """
        list all presigned URLs for documents under a specific project phase.

        Args:
            project_id: Project identifier.
            phase: Project phase (enum).
            logger: Structlog logger.

        Returns:
            A list of presigned URLs to documents.
        """
        async with self._client.get_session() as session:
            keys = await self._client.list_objects(session, logger, prefix=self._phase_prefix(project_id, phase))
            return await self._client.generate_presigned_urls(session, keys, logger)

    async def rename_phase_document(
        self,
        project_id: int,
        phase: ProjectPhase,
        old_key: str,
        new_key: str,
        logger: BoundLogger,
    ) -> str:
        """
        Rename a document in a project phase.

        Args:
            project_id: Project identifier.
            phase: Project phase (enum).
            old_key: Original file name.
            new_key: New file name.
            logger: Structlog logger.

        Raises:
            FileNotFound: If the original file does not exist.
        """
        prefix = self._phase_prefix(project_id, phase)
        old_object = f"{prefix}{old_key}"
        new_object = f"{prefix}{new_key}"
        async with self._client.get_session() as session:
            if old_object not in await self._client.list_objects(session, logger, prefix=prefix):
                raise FileNotFound(project_id, old_key)

            await self._client.copy_object(session, old_object, new_object, logger)
            await self._client.delete_file(session, old_object, logger)

            return (await self._client.generate_presigned_urls(session, [new_object], logger))[0]

    async def delete_phase_document(
        self,
        project_id: int,
        phase: ProjectPhase,
        file_name: str,
        logger: BoundLogger,
    ) -> None:
        """
        Delete a document in a specific project phase.

        Args:
            project_id: Project identifier.
            phase: Project phase (enum).
            file_name: Name of the file to delete.
            logger: Structlog logger.
        """
        prefix = self._phase_prefix(project_id, phase)
        object_name = f"{prefix}{file_name}"
        async with self._client.get_session() as session:
            if object_name not in await self._client.list_objects(session, logger, prefix=prefix):
                raise FileNotFound(project_id, file_name)
            await self._client.delete_file(session, object_name, logger)


def get_project_storage_manager_from_config(app_config: UrbanAPIConfig) -> ProjectStorageManager:
    return ProjectStorageManager(app_config)


def get_project_storage_manager():
    return get_project_storage_manager_from_config(UrbanAPIConfig.from_file_or_default())
