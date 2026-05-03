from __future__ import annotations

import asyncio
import json
import logging
from typing import AsyncGenerator, Optional

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from fastapi.responses import JSONResponse, StreamingResponse

from app.schemas import (
    BulkJobStatusResponse,
    BulkProcessingResponse,
    CSVValidationResponse,
    ResumeResponse,
)
from app.services.bulk import BulkJobService


router = APIRouter()
logger = logging.getLogger(__name__)


def get_service() -> BulkJobService:
    return BulkJobService()


@router.get("/healthz")
def healthz() -> dict:
    return {"status": "ok"}


@router.post("/hospitals/bulk", response_model=BulkProcessingResponse)
async def bulk_create_hospitals(
    file: UploadFile = File(...),
    wait: bool = Query(True, description="Wait for completion for small uploads"),
    service: BulkJobService = Depends(get_service),
):
    job, validation, response = await service.ingest_upload(file, wait_for_completion=wait)
    if not validation.valid:
        logger.info(
            "bulk_request_validation_failed",
            extra={
                "event": "bulk_request_validation_failed",
                "upload_filename": file.filename or "upload.csv",
                "issue_count": len(validation.issues),
            },
        )
        raise HTTPException(
            status_code=422,
            detail={
                "message": "CSV validation failed",
                "issues": [issue.model_dump() for issue in validation.issues],
            },
        )

    if response is not None:
        logger.info(
            "bulk_request_completed",
            extra={
                "event": "bulk_request_completed",
                "batch_id": response.batch_id,
                "status": response.status,
                "batch_activated": response.batch_activated,
            },
        )
        return response

    status = service.get_job_status(job.batch_id)
    logger.info(
        "bulk_request_accepted",
        extra={
            "event": "bulk_request_accepted",
            "batch_id": job.batch_id,
            "status": status.status,
        },
    )
    return JSONResponse(
        status_code=202,
        content={
            "batch_id": job.batch_id,
            "total_hospitals": status.total_rows,
            "processed_hospitals": status.processed_rows,
            "failed_hospitals": status.failed_rows,
            "duplicate_hospitals": status.duplicate_rows,
            "processing_time_seconds": 0.0,
            "batch_activated": status.batch_activated,
            "status": status.status,
            "hospitals": [],
            "progress_url": f"/hospitals/bulk/{job.batch_id}",
        },
    )


@router.post("/hospitals/bulk/validate", response_model=CSVValidationResponse)
async def validate_csv(
    file: UploadFile = File(...),
    service: BulkJobService = Depends(get_service),
):
    data = await file.read()
    validation = service.validate_csv_bytes(data, strict_rows=True)
    logger.info(
        "bulk_csv_validated",
        extra={
            "event": "bulk_csv_validated",
            "upload_filename": file.filename or "upload.csv",
            "valid": validation.valid,
            "total_rows": validation.total_rows,
            "issue_count": len(validation.issues),
        },
    )
    return validation


@router.get("/hospitals/bulk/{batch_id}", response_model=BulkJobStatusResponse)
def bulk_status(batch_id: str, service: BulkJobService = Depends(get_service)):
    try:
        status = service.get_job_status(batch_id)
        logger.info(
            "bulk_status_read",
            extra={
                "event": "bulk_status_read",
                "batch_id": batch_id,
                "status": status.status,
            },
        )
        return status
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@router.post("/hospitals/bulk/{batch_id}/resume", response_model=ResumeResponse)
async def resume_bulk(batch_id: str, service: BulkJobService = Depends(get_service)):
    try:
        result = service.resume_job(batch_id)
        logger.info(
            "bulk_resume_requested",
            extra={
                "event": "bulk_resume_requested",
                "batch_id": batch_id,
                "resumed": result.resumed,
                "status": result.status,
            },
        )
        return result
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@router.get("/hospitals/bulk/{batch_id}/rows")
def bulk_rows(
    batch_id: str,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    service: BulkJobService = Depends(get_service),
):
    try:
        rows = service.list_job_rows(batch_id, limit=limit, offset=offset)
        logger.info(
            "bulk_rows_read",
            extra={
                "event": "bulk_rows_read",
                "batch_id": batch_id,
                "row_count": len(rows),
                "limit": limit,
                "offset": offset,
            },
        )
        return {"batch_id": batch_id, "rows": [row.model_dump() for row in rows]}
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@router.get("/hospitals/bulk/{batch_id}/events")
async def bulk_events(batch_id: str, service: BulkJobService = Depends(get_service)):
    async def event_stream() -> AsyncGenerator[str, None]:
        last_payload: Optional[str] = None
        while True:
            try:
                payload = service.get_job_status(batch_id).model_dump()
            except ValueError:
                yield "event: error\ndata: {\"message\":\"batch not found\"}\n\n"
                return
            serialized = json.dumps(payload, separators=(",", ":"))
            if serialized != last_payload:
                last_payload = serialized
                yield f"event: progress\ndata: {serialized}\n\n"
            if payload["status"] in {"completed", "completed_with_errors", "failed", "invalid"}:
                yield f"event: done\ndata: {serialized}\n\n"
                return
            await asyncio.sleep(1.0)

    return StreamingResponse(event_stream(), media_type="text/event-stream")
