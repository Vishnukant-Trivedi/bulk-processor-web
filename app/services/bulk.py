from __future__ import annotations

import asyncio
import csv
import hashlib
import io
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from fastapi import UploadFile

from app.core.settings import settings
from app.schemas import (
    BulkJobStatusResponse,
    BulkProcessingResponse,
    CSVValidationResponse,
    HospitalRowResult,
    ResumeResponse,
    ValidationIssue,
)
from app.services.normalization import build_dedupe_key, normalize_row, row_to_payload
from app.services.store import STORE, JobRecord, RowRecord, new_batch_id
from app.services.upstream import HospitalDirectoryClient, UpstreamAPIError


logger = logging.getLogger(__name__)


MAX_HOSPITALS_PER_CSV = 20
TERMINAL_STATUSES = {
    "created",
    "created_and_activated",
    "failed",
    "invalid",
    "duplicate_existing",
    "duplicate_in_job",
}


class BulkJobService:
    def __init__(
        self, store=STORE, upstream_client: Optional[HospitalDirectoryClient] = None
    ) -> None:
        self.store = store
        self.upstream_client = upstream_client or HospitalDirectoryClient()

    async def ingest_upload(
        self, upload: UploadFile, wait_for_completion: bool = True
    ) -> Tuple[Optional[JobRecord], CSVValidationResponse, Optional[BulkProcessingResponse]]:
        data = await upload.read()
        validation, parsed_rows = self._parse_csv_bytes(data, strict_rows=False)
        logger.info(
            "bulk_upload_validated",
            extra={
                "event": "bulk_upload_validated",
                "upload_filename": upload.filename or "upload.csv",
                "total_rows": validation.total_rows,
                "valid": validation.valid,
                "issue_count": len(validation.issues),
            },
        )
        if not validation.valid:
            return None, validation, None

        batch_id = new_batch_id()
        source_sha256 = hashlib.sha256(data).hexdigest()
        with self.store.lock:
            self.store.create_job(
                batch_id=batch_id,
                source_filename=upload.filename or "upload.csv",
                source_sha256=source_sha256,
                raw_rows=parsed_rows,
                chunk_size=max(1, settings.bulk_chunk_size),
            )
        logger.info(
            "bulk_job_created",
            extra={
                "event": "bulk_job_created",
                "batch_id": batch_id,
                "upload_filename": upload.filename or "upload.csv",
                "total_rows": validation.total_rows,
            },
        )

        job = self.store.get_job(batch_id)
        if job is None:
            raise RuntimeError("job creation failed")

        if wait_for_completion and validation.total_rows <= settings.bulk_sync_row_limit:
            response = await self.process_job(batch_id)
            return job, validation, response

        return job, validation, None

    def validate_csv_file(self, path: Path, strict_rows: bool = False) -> CSVValidationResponse:
        with path.open("rb") as handle:
            data = handle.read()
        validation, _ = self._parse_csv_bytes(data, strict_rows=strict_rows)
        return validation

    def validate_csv_bytes(
        self, data: bytes, strict_rows: bool = False
    ) -> CSVValidationResponse:
        validation, _ = self._parse_csv_bytes(data, strict_rows=strict_rows)
        return validation

    async def process_job(
        self, batch_id: str, collect_results: bool = True
    ) -> BulkProcessingResponse:
        started = time.perf_counter()
        job = self.store.get_job(batch_id)
        if job is None:
            raise ValueError(f"Unknown batch_id: {batch_id}")

        with self.store.lock:
            if job.status == "completed":
                return self._response_for_job(batch_id, [], started)
            job.status = "processing"
            job.touch()
        logger.info("bulk_job_processing_started", extra={"event": "bulk_job_processing_started", "batch_id": batch_id})

        raw_rows = list(job.raw_rows)
        pending_rows = [
            row for row in raw_rows if job.rows[row["__row_number__"]].status not in TERMINAL_STATUSES
        ]

        row_results: List[HospitalRowResult] = []
        if pending_rows:
            semaphore = asyncio.Semaphore(max(1, settings.bulk_concurrency))
            tasks = [
                self._process_row(batch_id, row["__row_number__"], row, semaphore)
                for row in pending_rows
            ]
            processed = await asyncio.gather(*tasks)
            if collect_results:
                row_results.extend(
                    HospitalRowResult(
                        row=item.row,
                        hospital_id=item.hospital_id,
                        name=item.name,
                        status=item.status,
                )
                for item in processed
            )

        job_summary = self.store.summarize_job(batch_id)
        if job_summary is None:
            raise ValueError(f"Unknown batch_id: {batch_id}")

        should_activate = job_summary["created_rows"] > 0
        if should_activate:
            try:
                await self.upstream_client.activate_batch(batch_id)
                logger.info(
                    "bulk_batch_activation_succeeded",
                    extra={
                        "event": "bulk_batch_activation_succeeded",
                        "batch_id": batch_id,
                        "created_rows": job_summary["created_rows"],
                        "failed_rows": job_summary["failed_rows"],
                    },
                )
                with self.store.lock:
                    current = self.store.get_job(batch_id)
                    if current is not None:
                        current.batch_activated = True
                        current.status = "completed_with_errors" if job_summary["failed_rows"] else "completed"
                        current.activated_at = datetime.utcnow()
                        current.touch()
                        for row in current.rows.values():
                            if row.status == "created":
                                row.status = "created_and_activated"
                        for entry in self.store.ledger.values():
                            if entry.batch_id == batch_id:
                                entry.active = True
                if collect_results:
                    for result in row_results:
                        if result.status == "created":
                            result.status = "created_and_activated"
            except Exception as exc:
                logger.exception(
                    "bulk_batch_activation_failed",
                    extra={
                        "event": "bulk_batch_activation_failed",
                        "batch_id": batch_id,
                    },
                )
                with self.store.lock:
                    current = self.store.get_job(batch_id)
                    if current is not None:
                        current.status = "completed_with_errors"
                        current.error_message = f"Activation failed: {exc}"
        else:
            with self.store.lock:
                current = self.store.get_job(batch_id)
                if current is not None:
                    current.status = "completed_with_errors" if job_summary["failed_rows"] else "completed"

        return self._response_for_job(batch_id, row_results, started)

    def get_job_status(self, batch_id: str) -> BulkJobStatusResponse:
        summary = self.store.summarize_job(batch_id)
        if summary is None:
            raise ValueError(f"Unknown batch_id: {batch_id}")
        return BulkJobStatusResponse(**summary)

    def resume_job(self, batch_id: str) -> ResumeResponse:
        with self.store.lock:
            job = self.store.get_job(batch_id)
            if job is None:
                raise ValueError(f"Unknown batch_id: {batch_id}")
            if job.status == "completed":
                return ResumeResponse(batch_id=batch_id, status=job.status, resumed=False)
            has_retryable_failures = any(row.status == "failed" for row in job.rows.values())
            needs_activation_retry = (
                job.batch_activated is False
                and any(row.status == "created" for row in job.rows.values())
            )
            if not has_retryable_failures and not needs_activation_retry:
                return ResumeResponse(batch_id=batch_id, status=job.status, resumed=False)
            for row in job.rows.values():
                if row.status == "failed":
                    row.status = "pending"
                    row.message = None
                    row.hospital_id = None
            job.in_progress_dedupe_keys.clear()
            job.status = "queued"
            job.error_message = None
            job.touch()
        logger.info(
            "bulk_job_resumed",
            extra={
                "event": "bulk_job_resumed",
                "batch_id": batch_id,
                "resumed": True,
                "retryable_failures": has_retryable_failures,
                "needs_activation_retry": needs_activation_retry,
            },
        )
        return ResumeResponse(batch_id=batch_id, status="queued", resumed=True)

    def list_job_rows(
        self, batch_id: str, limit: int = 100, offset: int = 0
    ) -> List[HospitalRowResult]:
        job = self.store.get_job(batch_id)
        if job is None:
            raise ValueError(f"Unknown batch_id: {batch_id}")
        ordered_rows = sorted(job.rows.values(), key=lambda item: item.row)
        sliced = ordered_rows[offset : offset + limit]
        return [
            HospitalRowResult(
                row=row.row,
                hospital_id=row.hospital_id,
                name=row.name,
                status=row.status,
            )
            for row in sliced
        ]

    async def _process_row(
        self,
        batch_id: str,
        row_number: int,
        raw_row: Dict[str, str],
        semaphore: asyncio.Semaphore,
    ) -> RowRecord:
        async with semaphore:
            return await self._process_row_inner(batch_id, row_number, raw_row)

    async def _process_row_inner(
        self, batch_id: str, row_number: int, raw_row: Dict[str, str]
    ) -> RowRecord:
        job = self.store.get_job(batch_id)
        if job is None:
            raise ValueError(f"Unknown batch_id: {batch_id}")

        name, address, phone, dedupe_key = normalize_row(raw_row)
        row = job.rows[row_number]

        validation_errors = self._validate_row(row_number, raw_row)
        if validation_errors:
            message = "; ".join(issue.message for issue in validation_errors)
            with self.store.lock:
                row.status = "invalid"
                row.message = message
                job.touch()
            logger.info(
                "bulk_row_invalid",
                extra={
                    "event": "bulk_row_invalid",
                    "batch_id": batch_id,
                    "row": row_number,
                    "error_message": message,
                },
            )
            return row

        with self.store.lock:
            if dedupe_key in job.in_progress_dedupe_keys:
                row.status = "duplicate_in_job"
                row.message = "Duplicate row in the same upload"
                logger.info(
                    "bulk_row_duplicate_in_job",
                    extra={
                        "event": "bulk_row_duplicate_in_job",
                        "batch_id": batch_id,
                        "row": row_number,
                    },
                )
                return row

            existing = self.store.ledger.get(dedupe_key)
            if existing is not None:
                row.status = "duplicate_in_job" if existing.batch_id == batch_id else "duplicate_existing"
                row.hospital_id = existing.hospital_id
                row.message = (
                    "Duplicate row in the same upload"
                    if existing.batch_id == batch_id
                    else "Duplicate of an already created hospital"
                )
                logger.info(
                    "bulk_row_duplicate_existing",
                    extra={
                        "event": "bulk_row_duplicate_existing",
                        "batch_id": batch_id,
                        "row": row_number,
                        "existing_batch_id": existing.batch_id,
                    },
                )
                return row

            job.in_progress_dedupe_keys.add(dedupe_key)
            row.dedupe_key = dedupe_key

        payload = row_to_payload(raw_row, batch_id)
        try:
            response = await self.upstream_client.create_hospital(payload)
            hospital_id = response.get("id") or response.get("hospital_id") or self.store.next_hospital_id()
            if isinstance(hospital_id, str) and hospital_id.isdigit():
                hospital_id = int(hospital_id)
            with self.store.lock:
                row.status = "created"
                row.hospital_id = int(hospital_id)
                row.message = None
                self.store.ledger[dedupe_key] = self.store.ledger.get(
                    dedupe_key,
                    None,
                ) or self._make_ledger_entry(batch_id, row_number, name, address, phone, int(hospital_id))
                self.store.ledger[dedupe_key].hospital_id = int(hospital_id)
                self.store.ledger[dedupe_key].active = False
                job.in_progress_dedupe_keys.discard(dedupe_key)
                job.touch()
            logger.info(
                "bulk_row_created",
                extra={
                    "event": "bulk_row_created",
                    "batch_id": batch_id,
                    "row": row_number,
                    "hospital_id": int(hospital_id),
                },
            )
            return row
        except Exception as exc:
            with self.store.lock:
                row.status = "failed"
                row.message = str(exc)
                job.in_progress_dedupe_keys.discard(dedupe_key)
                job.touch()
            logger.exception(
                "bulk_row_create_failed",
                extra={
                    "event": "bulk_row_create_failed",
                    "batch_id": batch_id,
                    "row": row_number,
                },
            )
            return row

    def _make_ledger_entry(
        self,
        batch_id: str,
        row_number: int,
        name: str,
        address: str,
        phone: Optional[str],
        hospital_id: int,
    ):
        from app.services.store import LedgerEntry

        return LedgerEntry(
            batch_id=batch_id,
            row_number=row_number,
            hospital_id=hospital_id,
            name=name,
            address=address,
            phone=phone,
            active=False,
        )

    def _validate_row(
        self, row_number: int, row: Dict[str, str]
    ) -> List[ValidationIssue]:
        issues: List[ValidationIssue] = []
        name = (row.get("name") or "").strip()
        address = (row.get("address") or "").strip()
        if not name:
            issues.append(ValidationIssue(row=row_number, field="name", message="name is required"))
        if not address:
            issues.append(
                ValidationIssue(row=row_number, field="address", message="address is required")
            )
        return issues

    def _parse_csv_bytes(
        self, data: bytes, strict_rows: bool = False
    ) -> Tuple[CSVValidationResponse, List[Dict[str, str]]]:
        issues: List[ValidationIssue] = []
        rows: List[Dict[str, str]] = []
        try:
            text = data.decode("utf-8-sig")
        except UnicodeDecodeError:
            return (
                CSVValidationResponse(
                    valid=False,
                    total_rows=0,
                    issues=[ValidationIssue(row=0, message="CSV must be UTF-8 encoded")],
                ),
                [],
            )

        reader = csv.DictReader(io.StringIO(text))
        headers = [header.strip().lower() for header in (reader.fieldnames or [])]
        required = {"name", "address"}
        missing = sorted(required - set(headers))
        if missing:
            for field in missing:
                issues.append(
                    ValidationIssue(row=0, field=field, message=f"Missing required column: {field}")
                )
            return (
                CSVValidationResponse(valid=False, total_rows=0, issues=issues),
                [],
            )

        for row_number, raw in enumerate(reader, start=1):
            if row_number > MAX_HOSPITALS_PER_CSV:
                issues.append(
                    ValidationIssue(
                        row=row_number,
                        message=f"CSV exceeds maximum allowed rows ({MAX_HOSPITALS_PER_CSV})",
                    )
                )
                break
            normalized = self._normalize_csv_row(row_number, raw)
            rows.append(normalized)
            if strict_rows:
                issues.extend(self._validate_row(row_number, normalized))

        valid = len(issues) == 0 and 1 <= len(rows) <= MAX_HOSPITALS_PER_CSV
        return CSVValidationResponse(valid=valid, total_rows=len(rows), issues=issues), rows

    def _normalize_csv_row(self, row_number: int, raw: Dict[str, str]) -> Dict[str, str]:
        normalized = {
            "name": (raw.get("name") or "").strip(),
            "address": (raw.get("address") or "").strip(),
            "phone": (raw.get("phone") or "").strip(),
            "__row_number__": row_number,
        }
        normalized["__dedupe_key__"] = build_dedupe_key(
            normalized["name"], normalized["address"], normalized["phone"] or None
        )
        return normalized

    def _response_for_job(
        self, batch_id: str, row_results: Sequence[HospitalRowResult], started_at: float
    ) -> BulkProcessingResponse:
        job = self.store.get_job(batch_id)
        if job is None:
            raise ValueError(f"Unknown batch_id: {batch_id}")
        summary = self.store.summarize_job(batch_id)
        if summary is None:
            raise ValueError(f"Unknown batch_id: {batch_id}")
        if job.batch_activated:
            for result in row_results:
                if result.status == "created":
                    result.status = "created_and_activated"
        return BulkProcessingResponse(
            batch_id=batch_id,
            total_hospitals=summary["total_rows"],
            processed_hospitals=summary["processed_rows"],
            failed_hospitals=summary["failed_rows"],
            duplicate_hospitals=summary["duplicate_rows"],
            processing_time_seconds=round(max(time.perf_counter() - started_at, 0.0), 3),
            batch_activated=job.batch_activated,
            status=job.status,
            hospitals=list(row_results),
            progress_url=f"/hospitals/bulk/{batch_id}",
        )

    def find_next_queued_job(self) -> Optional[str]:
        return self.store.claim_next_queued_job()
