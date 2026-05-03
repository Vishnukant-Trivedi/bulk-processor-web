from __future__ import annotations

import io

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import get_service, router
from app.services.bulk import BulkJobService
from app.services.store import STORE


class FakeUpstreamClient:
    def __init__(self) -> None:
        self.created = []
        self.activated = []

    async def create_hospital(self, payload):
        self.created.append(payload)
        return {"id": len(self.created)}

    async def activate_batch(self, batch_id):
        self.activated.append(batch_id)
        return {"status": "activated", "batch_id": batch_id}


class FlakyCreateClient(FakeUpstreamClient):
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


@pytest.fixture(autouse=True)
def clean_store():
    STORE.reset()
    yield
    STORE.reset()


def build_client(upstream_client):
    service = BulkJobService(upstream_client=upstream_client)
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_service] = lambda: service
    return TestClient(app), service


def file_payload(content: str, filename: str = "hospitals.csv"):
    return {"file": (filename, io.BytesIO(content.encode("utf-8")), "text/csv")}


def test_bulk_api_happy_path_and_events():
    client, _service = build_client(FakeUpstreamClient())
    csv_data = (
        "name,address,phone\n"
        "General Hospital,123 Main St,555-1111\n"
        "General Hospital,123 Main St,555-1111\n"
        "City Hospital,77 Oak Ave,\n"
    )

    validate_response = client.post("/hospitals/bulk/validate", files=file_payload(csv_data))
    assert validate_response.status_code == 200
    assert validate_response.json()["valid"] is True

    bulk_response = client.post("/hospitals/bulk", params={"wait": "true"}, files=file_payload(csv_data))
    assert bulk_response.status_code == 200
    payload = bulk_response.json()
    assert payload["batch_activated"] is True
    assert payload["status"] == "completed"
    assert len(payload["hospitals"]) == 3

    batch_id = payload["batch_id"]

    status_response = client.get(f"/hospitals/bulk/{batch_id}")
    assert status_response.status_code == 200
    status_payload = status_response.json()
    assert status_payload["batch_activated"] is True
    assert status_payload["progress_percent"] == 66.67

    rows_response = client.get(f"/hospitals/bulk/{batch_id}/rows")
    assert rows_response.status_code == 200
    rows_payload = rows_response.json()
    assert rows_payload["batch_id"] == batch_id
    assert len(rows_payload["rows"]) == 3

    events_response = client.get(f"/hospitals/bulk/{batch_id}/events")
    assert events_response.status_code == 200
    assert "event: done" in events_response.text
    assert '"status":"completed"' in events_response.text


@pytest.mark.asyncio
async def test_resume_api_retries_failed_rows():
    client, service = build_client(FlakyCreateClient(fail_once_for_name="Retry Hospital"))
    csv_data = (
        "name,address,phone\n"
        "Good Hospital,123 Main St,555-1111\n"
        "Retry Hospital,77 Oak Ave,555-2222\n"
    )

    bulk_response = client.post("/hospitals/bulk", params={"wait": "true"}, files=file_payload(csv_data))
    assert bulk_response.status_code == 200
    payload = bulk_response.json()
    assert payload["status"] == "completed_with_errors"
    assert payload["batch_activated"] is True

    batch_id = payload["batch_id"]
    status_payload = client.get(f"/hospitals/bulk/{batch_id}").json()
    assert status_payload["status"] == "completed_with_errors"
    assert status_payload["batch_activated"] is True

    rows_payload = client.get(f"/hospitals/bulk/{batch_id}/rows").json()
    assert any(row["status"] == "failed" for row in rows_payload["rows"])

    resume_response = client.post(f"/hospitals/bulk/{batch_id}/resume")
    assert resume_response.status_code == 200
    assert resume_response.json()["resumed"] is True

    await service.process_job(batch_id)

    final_status = client.get(f"/hospitals/bulk/{batch_id}").json()
    assert final_status["status"] == "completed"
    assert final_status["batch_activated"] is True
    assert final_status["progress_percent"] == 100.0
