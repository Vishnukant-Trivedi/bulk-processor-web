from __future__ import annotations

import io

import pytest
from starlette.datastructures import UploadFile

from app.services.bulk import BulkJobService
from app.services.store import STORE


class FakeUpstreamClient:
    def __init__(self) -> None:
        self.created = []
        self.activated = []
        self.deleted = []

    async def create_hospital(self, payload):
        self.created.append(payload)
        return {"id": len(self.created)}

    async def activate_batch(self, batch_id):
        self.activated.append(batch_id)
        return {"status": "activated", "batch_id": batch_id}

    async def delete_batch(self, batch_id):
        self.deleted.append(batch_id)
        return {"status": "deleted", "batch_id": batch_id}


class FlakyUpstreamClient(FakeUpstreamClient):
    def __init__(self, fail_once_for_name: str) -> None:
        super().__init__()
        self.fail_once_for_name = fail_once_for_name
        self._failed_names = set()

    async def create_hospital(self, payload):
        name = payload["name"]
        if name == self.fail_once_for_name and name not in self._failed_names:
            self._failed_names.add(name)
            raise RuntimeError("transient upstream failure")
        return await super().create_hospital(payload)


class ActivationFlakyClient(FakeUpstreamClient):
    def __init__(self) -> None:
        super().__init__()
        self.fail_once = True

    async def activate_batch(self, batch_id):
        if self.fail_once:
            self.fail_once = False
            raise RuntimeError("transient activation failure")
        return await super().activate_batch(batch_id)


@pytest.fixture(autouse=True)
def clean_store():
    STORE.reset()
    yield
    STORE.reset()


def make_upload(content: str, filename: str = "hospitals.csv") -> UploadFile:
    return UploadFile(filename=filename, file=io.BytesIO(content.encode("utf-8")))


@pytest.mark.asyncio
async def test_validation_flags_missing_required_columns():
    service = BulkJobService(upstream_client=FakeUpstreamClient())
    upload = make_upload("name,phone\nHospital A,111\n", filename="bad.csv")

    job, validation, response = await service.ingest_upload(upload)

    assert job is None
    assert response is None
    assert validation.valid is False
    assert any(issue.field == "address" for issue in validation.issues)


@pytest.mark.asyncio
async def test_bulk_ingest_dedupes_and_activates():
    client = FakeUpstreamClient()
    service = BulkJobService(upstream_client=client)
    upload = make_upload(
        "name,address,phone\n"
        "General Hospital,123 Main St,555-1111\n"
        "General Hospital,123 Main St,555-1111\n"
        "City Hospital,77 Oak Ave,\n"
    )

    job, validation, response = await service.ingest_upload(upload, wait_for_completion=True)

    assert validation.valid is True
    assert job is not None
    assert response is not None
    assert response.batch_id == job.batch_id
    assert response.total_hospitals == 3
    assert response.processed_hospitals == 3
    assert response.duplicate_hospitals == 1
    assert response.failed_hospitals == 0
    assert response.batch_activated is True
    assert response.processing_time_seconds >= 0
    assert service.get_job_status(job.batch_id).progress_percent == 66.67
    assert len(client.created) == 2
    assert client.activated == [job.batch_id]
    assert {row.status for row in response.hospitals} <= {
        "created_and_activated",
        "duplicate_in_job",
    }


@pytest.mark.asyncio
async def test_bulk_ingest_activates_when_some_rows_fail_but_creates_exist():
    client = FlakyUpstreamClient(fail_once_for_name="Retry Hospital")
    service = BulkJobService(upstream_client=client)
    upload = make_upload(
        "name,address,phone\n"
        "Good Hospital,123 Main St,555-1111\n"
        "Retry Hospital,77 Oak Ave,555-2222\n"
    )

    job, validation, response = await service.ingest_upload(upload, wait_for_completion=True)

    assert validation.valid is True
    assert job is not None
    assert response is not None
    assert response.batch_activated is True
    assert response.status == "completed_with_errors"
    assert response.failed_hospitals == 1
    assert response.total_hospitals == 2
    assert service.get_job_status(job.batch_id).progress_percent == 50.0
    assert client.activated == [job.batch_id]
    assert any(row.status == "failed" for row in response.hospitals)
    assert any(row.status == "created_and_activated" for row in response.hospitals)


@pytest.mark.asyncio
async def test_resume_moves_job_back_to_queue():
    service = BulkJobService(upstream_client=FakeUpstreamClient())
    job = STORE.create_job(
        batch_id="batch-1",
        source_filename="a.csv",
        source_sha256="abc",
        raw_rows=[
            {
                "name": "A",
                "address": "B",
                "phone": "",
                "__row_number__": 1,
                "__dedupe_key__": "k1",
            }
        ],
        chunk_size=1,
    )
    job.status = "completed_with_errors"
    job.rows[1].status = "failed"

    result = service.resume_job("batch-1")
    assert result.resumed is True
    assert result.status == "queued"


@pytest.mark.asyncio
async def test_resume_retries_failed_rows():
    client = FlakyUpstreamClient(fail_once_for_name="Retry Hospital")
    service = BulkJobService(upstream_client=client)
    upload = make_upload(
        "name,address,phone\n"
        "Good Hospital,123 Main St,555-1111\n"
        "Retry Hospital,77 Oak Ave,555-2222\n"
    )

    job, validation, response = await service.ingest_upload(upload, wait_for_completion=True)

    assert validation.valid is True
    assert job is not None
    assert response is not None
    assert service.get_job_status(job.batch_id).status == "completed_with_errors"

    rows_before = service.list_job_rows(job.batch_id)
    assert any(row.status == "failed" and row.name == "Retry Hospital" for row in rows_before)

    resume_result = service.resume_job(job.batch_id)
    assert resume_result.resumed is True
    assert resume_result.status == "queued"

    rows_after_resume = service.list_job_rows(job.batch_id)
    assert any(row.status == "pending" and row.name == "Retry Hospital" for row in rows_after_resume)

    retry_response = await service.process_job(job.batch_id)

    assert retry_response.batch_activated is True
    assert service.get_job_status(job.batch_id).status == "completed"
    assert len(client.created) == 2


@pytest.mark.asyncio
async def test_invalid_only_batch_does_not_resume():
    service = BulkJobService(upstream_client=FakeUpstreamClient())
    job = STORE.create_job(
        batch_id="batch-invalid",
        source_filename="invalid.csv",
        source_sha256="abc",
        raw_rows=[
            {
                "name": "",
                "address": "123 Main St",
                "phone": "",
                "__row_number__": 1,
                "__dedupe_key__": "k1",
            }
        ],
        chunk_size=1,
    )
    job.status = "completed_with_errors"
    job.rows[1].status = "invalid"

    result = service.resume_job("batch-invalid")

    assert result.resumed is False
    assert result.status == "completed_with_errors"


@pytest.mark.asyncio
async def test_duplicate_existing_only_batch_does_not_activate():
    client = FakeUpstreamClient()
    service = BulkJobService(upstream_client=client)

    first_upload = make_upload(
        "name,address,phone\n"
        "General Hospital,123 Main St,555-1111\n"
    )
    job1, validation1, response1 = await service.ingest_upload(first_upload, wait_for_completion=True)
    assert validation1.valid is True
    assert response1 is not None
    assert response1.batch_activated is True

    second_upload = make_upload(
        "name,address,phone\n"
        "General Hospital,123 Main St,555-1111\n"
    )
    job2, validation2, response2 = await service.ingest_upload(second_upload, wait_for_completion=True)

    assert validation2.valid is True
    assert job2 is not None
    assert response2 is not None
    assert response2.batch_activated is False
    assert response2.status == "completed"
    assert response2.failed_hospitals == 0
    assert response2.duplicate_hospitals == 1
    assert service.get_job_status(job2.batch_id).progress_percent == 0.0
    assert client.activated == [job1.batch_id]


@pytest.mark.asyncio
async def test_resume_retries_activation_failure():
    client = ActivationFlakyClient()
    service = BulkJobService(upstream_client=client)
    upload = make_upload(
        "name,address,phone\n"
        "General Hospital,123 Main St,555-1111\n"
    )

    job, validation, response = await service.ingest_upload(upload, wait_for_completion=True)

    assert validation.valid is True
    assert job is not None
    assert response is not None
    assert response.batch_activated is False
    assert response.status == "completed_with_errors"
    assert client.activated == []

    resume_result = service.resume_job(job.batch_id)
    assert resume_result.resumed is True
    assert resume_result.status == "queued"

    retry_response = await service.process_job(job.batch_id)

    assert retry_response.batch_activated is True
    assert service.get_job_status(job.batch_id).status == "completed"
    assert client.activated == [job.batch_id]
