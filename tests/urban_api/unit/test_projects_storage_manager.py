import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from structlog.stdlib import BoundLogger

from idu_api.urban_api.exceptions.utils.minio import FileNotFound
from idu_api.urban_api.minio.client import AsyncMinioClient
from idu_api.urban_api.minio.services import ProjectStorageManager


@pytest.fixture
def storage_manager():
    mock_config = MagicMock()
    with patch(
        "idu_api.urban_api.minio.services.projects_storage.get_minio_client_from_config",
        new=MagicMock(return_value=MagicMock(spec=AsyncMinioClient)),
    ):
        return ProjectStorageManager(mock_config)


@pytest.fixture
def fake_logger():
    return AsyncMock(spec=BoundLogger)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "metadata, expected_result",
    [
        ([b'{"name": "Project1"}'], [{"name": "Project1"}]),
        ([b'{"main_image_id": "img1"}'], [{"main_image_id": "img1"}]),
    ],
)
async def test_load_list_of_metadata(storage_manager, fake_logger, metadata, expected_result):
    session = AsyncMock()
    storage_manager._client.get_files.return_value = [MagicMock(read=MagicMock(return_value=metadata[0]))]
    result = await storage_manager.load_list_of_metadata(session, [1], fake_logger)
    assert result == expected_result


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "project_id, metadata",
    [
        (123, {"title": "Urban Project"}),
        (456, {"gallery_images": ["img1"], "main_image_id": "img1"}),
    ],
)
async def test_save_metadata(storage_manager, fake_logger, project_id, metadata):
    session = AsyncMock()
    await storage_manager.save_metadata(session, project_id, metadata, fake_logger)
    storage_manager._client.upload_file.assert_awaited_once()
    args, kwargs = storage_manager._client.upload_file.call_args
    assert json.loads(args[1].decode()) == metadata
    assert project_id == project_id


@pytest.mark.asyncio
async def test_init_project(storage_manager, fake_logger):
    session = AsyncMock()
    storage_manager._client.get_session.return_value.__aenter__.return_value = session

    await storage_manager.init_project(1, fake_logger)
    storage_manager._client.upload_file.assert_awaited_once()


@pytest.mark.asyncio
async def test_delete_project(storage_manager, fake_logger):
    session = AsyncMock()
    storage_manager._client.get_session.return_value.__aenter__.return_value = session
    storage_manager._client.list_objects.return_value = ["1/test.jpg"]

    await storage_manager.delete_project(1, fake_logger)
    storage_manager._client.list_objects.assert_awaited_once_with(session, fake_logger, prefix="1/")
    storage_manager._client.delete_file.assert_any_call(session, "1/test.jpg", fake_logger)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "gallery_images, main_image_id, expected_order",
    [
        (["img1", "img2"], "img1", ["img1", "img2"]),
        (["img2", "img1"], "img1", ["img1", "img2"]),
        (["img3"], None, ["img3"]),
    ],
)
async def test_get_list_gallery_images_urls(
    storage_manager, fake_logger, gallery_images, main_image_id, expected_order
):
    session = AsyncMock()
    storage_manager._client.get_session.return_value.__aenter__.return_value = session
    storage_manager.load_metadata = AsyncMock(
        return_value={"gallery_images": gallery_images, "main_image_id": main_image_id}
    )
    storage_manager._client.generate_presigned_urls.return_value = [f"url_for_{img}" for img in expected_order]

    result = await storage_manager.get_list_gallery_images_urls(1, fake_logger)
    assert result == [f"url_for_{img}" for img in expected_order]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "existing_objects, image_type, should_raise",
    [
        (["img1"], "preview", False),
        (["img1"], "original", False),
        ([], "preview", True),
    ],
)
async def test_get_gallery_image(storage_manager, fake_logger, existing_objects, image_type, should_raise):
    session = AsyncMock()
    storage_manager._client.get_session.return_value.__aenter__.return_value = session
    storage_manager.load_metadata = AsyncMock(
        return_value={
            "gallery_images": existing_objects,
            "main_image_id": existing_objects[0] if existing_objects else None,
        }
    )
    storage_manager._client.get_files.return_value = [b"image-bytes"]

    if should_raise:
        with pytest.raises(FileNotFound):
            await storage_manager.get_gallery_image(1, "img1", fake_logger, image_type)
    else:
        result = await storage_manager.get_gallery_image(1, "img1", fake_logger, image_type)
        assert result == b"image-bytes"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "main_image_id, image_id, should_raise",
    [
        ("img1", "img1", False),
        ("img2", "img1", True),
    ],
)
async def test_set_main_image(storage_manager, fake_logger, main_image_id, image_id, should_raise):
    session = AsyncMock()
    storage_manager._client.get_session.return_value.__aenter__.return_value = session
    storage_manager.load_metadata = AsyncMock(return_value={"main_image_id": main_image_id})
    storage_manager._client.list_objects.return_value = [f"1/gallery/preview/{image_id}.jpg"]

    if should_raise:
        storage_manager._client.list_objects.return_value = []
        with pytest.raises(FileNotFound):
            await storage_manager.set_main_image(1, image_id, fake_logger)
    else:
        await storage_manager.set_main_image(1, image_id, fake_logger)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "object_names, existing_objects, expected_urls",
    [
        (
            ["1/gallery/preview/img1.jpg", "2/gallery/preview/img2.jpg"],
            ["1/gallery/preview/img1.jpg"],
            ["url1", "default"],
        ),
    ],
)
async def test_get_main_images_urls(storage_manager, fake_logger, object_names, existing_objects, expected_urls):
    session = AsyncMock()
    storage_manager._client.get_session.return_value.__aenter__.return_value = session
    storage_manager.load_list_of_metadata = AsyncMock(
        return_value=[
            {"main_image_id": "img1"},
            {"main_image_id": "img2"},
        ]
    )
    storage_manager._client.list_objects.return_value = existing_objects
    storage_manager._client.generate_presigned_urls.return_value = ["url1", "default"]

    result = await storage_manager.get_main_images_urls([1, 2], fake_logger)
    assert len(result) == 2
    assert result[0]["url"] == "url1"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "image_id, gallery_images, main_image_id",
    [
        ("img1", ["img1", "img2"], "img1"),
        ("img3", ["img3"], "img3"),
    ],
)
async def test_delete_gallery_image(storage_manager, fake_logger, image_id, gallery_images, main_image_id):
    session = AsyncMock()
    storage_manager._client.get_session.return_value.__aenter__.return_value = session
    storage_manager.load_metadata = AsyncMock(
        return_value={"gallery_images": gallery_images, "main_image_id": main_image_id}
    )

    await storage_manager.delete_gallery_image(1, image_id, fake_logger)
    assert storage_manager._client.delete_file.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "file_ext, expected_path",
    [
        ("png", "1/logo/image.png"),
        ("jpg", "1/logo/image.jpg"),
    ],
)
async def test_upload_logo(storage_manager, fake_logger, file_ext, expected_path):
    session = AsyncMock()
    storage_manager._client.get_session.return_value.__aenter__.return_value = session
    storage_manager._client.generate_presigned_urls.return_value = ["http://logo.url"]

    result = await storage_manager.upload_logo(1, b"logo-bytes", file_ext, fake_logger)
    storage_manager._client.upload_file.assert_awaited_once_with(session, b"logo-bytes", expected_path, fake_logger)
    assert result == "http://logo.url"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "existing_objects, expected_result",
    [
        (["1/logo/image.jpg"], "http://logo.url"),
        ([], None),
    ],
)
async def test_get_logo_url(storage_manager, fake_logger, existing_objects, expected_result):
    session = AsyncMock()
    storage_manager._client.get_session.return_value.__aenter__.return_value = session
    storage_manager._client.list_objects.return_value = existing_objects
    storage_manager._client.generate_presigned_urls.return_value = ["http://logo.url"]

    result = await storage_manager.get_logo_url(1, fake_logger)
    assert result == expected_result


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "existing_objects, should_raise",
    [
        (["1/logo/image.jpg"], False),
        ([], True),
    ],
)
async def test_delete_logo(storage_manager, fake_logger, existing_objects, should_raise):
    session = AsyncMock()
    storage_manager._client.get_session.return_value.__aenter__.return_value = session
    storage_manager._client.list_objects.return_value = existing_objects

    if should_raise:
        with pytest.raises(FileNotFound):
            await storage_manager.delete_logo(1, fake_logger)
    else:
        await storage_manager.delete_logo(1, fake_logger)
        storage_manager._client.delete_file.assert_awaited_once()


from enum import Enum


class ScenarioPhase(str, Enum):
    planning = "planning"
    implementation = "implementation"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "existing, expected_name",
    [
        ([], "1/phases/planning/plan.pdf"),
        (["1/phases/planning/plan.pdf"], "1/phases/planning/plan (1).pdf"),
        (["1/phases/planning/plan.pdf", "1/phases/planning/plan (1).pdf"], "1/phases/planning/plan (2).pdf"),
    ],
)
async def test_upload_phase_document(storage_manager, fake_logger, existing, expected_name):
    session = AsyncMock()
    storage_manager._client.get_session.return_value.__aenter__.return_value = session
    storage_manager._client.list_objects.return_value = existing
    storage_manager._client.generate_presigned_urls.return_value = [f"http://url/{expected_name}"]

    result = await storage_manager.upload_phase_document(
        project_id=1,
        phase=ScenarioPhase.planning,
        file_data=b"doc-bytes",
        file_name="plan",
        file_ext="pdf",
        logger=fake_logger,
    )
    assert result == f"http://url/{expected_name}"
    storage_manager._client.upload_file.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_phase_document_urls(storage_manager, fake_logger):
    session = AsyncMock()
    storage_manager._client.get_session.return_value.__aenter__.return_value = session
    storage_manager._client.list_objects.return_value = ["1/phases/planning/file1.pdf", "1/phases/planning/file2.pdf"]
    storage_manager._client.generate_presigned_urls.return_value = ["url1", "url2"]

    result = await storage_manager.get_phase_document_urls(1, ScenarioPhase.planning.value, fake_logger)
    assert result == ["url1", "url2"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "existing_objects, should_raise",
    [
        (["1/phases/planning/old.pdf"], False),
        ([], True),
    ],
)
async def test_rename_phase_document(storage_manager, fake_logger, existing_objects, should_raise):
    session = AsyncMock()
    storage_manager._client.get_session.return_value.__aenter__.return_value = session
    storage_manager._client.list_objects.return_value = existing_objects
    storage_manager._client.generate_presigned_urls.return_value = ["url-renamed"]

    if should_raise:
        with pytest.raises(FileNotFound):
            await storage_manager.rename_phase_document(1, ScenarioPhase.planning, "old.pdf", "new.pdf", fake_logger)
    else:
        result = await storage_manager.rename_phase_document(
            1, ScenarioPhase.planning.value, "old.pdf", "new.pdf", fake_logger
        )
        storage_manager._client.copy_object.assert_awaited_once()
        storage_manager._client.delete_file.assert_awaited_once()
        assert result == "url-renamed"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "existing_objects, should_raise",
    [
        (["1/phases/implementation/report.pdf"], False),
        ([], True),
    ],
)
async def test_delete_phase_document(storage_manager, fake_logger, existing_objects, should_raise):
    session = AsyncMock()
    storage_manager._client.get_session.return_value.__aenter__.return_value = session
    storage_manager._client.list_objects.return_value = existing_objects

    if should_raise:
        with pytest.raises(FileNotFound):
            await storage_manager.delete_phase_document(
                project_id=1, phase=ScenarioPhase.implementation.value, file_name="report.pdf", logger=fake_logger
            )
    else:
        await storage_manager.delete_phase_document(
            project_id=1, phase=ScenarioPhase.implementation.value, file_name="report.pdf", logger=fake_logger
        )
        storage_manager._client.delete_file.assert_awaited_once_with(
            session, "1/phases/implementation/report.pdf", fake_logger
        )
