# Hospital Bulk Processor

Bulk-processing service for the hospital directory API.

## What it does

- Accepts CSV uploads for bulk hospital creation.
- Keeps job state fully in memory.
- Dedupe based on normalized hospital identity.
- Processes jobs asynchronously with bounded concurrency.
- Activates the upstream batch only after all unique valid rows succeed.
- Exposes validation, progress, SSE progress streaming, and resume endpoints.

## Architecture

- Web API accepts uploads, validates structure, and keeps parsed rows in memory.
- A background loop in the same web process handles queued batches.
- Chunk summaries and row status live in process memory, which fits the 20-row cap.
- Hospital identity is deduped with an in-memory ledger keyed by normalized name, address, and phone.
- The batch is activated only after every unique valid row succeeds.

## Endpoints

- `POST /hospitals/bulk`
- `POST /hospitals/bulk/validate`
- `GET /hospitals/bulk/{batch_id}`
- `GET /hospitals/bulk/{batch_id}/rows`
- `GET /hospitals/bulk/{batch_id}/events`
- `POST /hospitals/bulk/{batch_id}/resume`
- `GET /healthz`

## Bonus items included

- CSV validation endpoint
- Progress tracking via polling and SSE
- Resume support for failed jobs
- Comprehensive tests
- Dockerfile and docker-compose
- Render deployment manifest

### Progress semantics

- `progress_percent` means the share of rows that have been fully created and activated in the upstream batch.
- It is calculated as `created_and_activated_rows / total_rows * 100`.
- Failed, invalid, and duplicate rows do not count toward `progress_percent`.
- A batch can still be `completed_with_errors` while `progress_percent` is below `100.0` if some rows never became `created_and_activated`.

## Run locally

```bash
python3 -m pip install -r requirements.txt
python3 -m uvicorn app.main:app --reload
```

## Environment

- `HOSPITAL_API_BASE_URL`: upstream hospital API base URL
- `BULK_SYNC_ROW_LIMIT`: max rows to process synchronously in request path
- `BULK_CHUNK_SIZE`: rows per processing chunk
- `BULK_CONCURRENCY`: concurrent upstream requests per chunk
- `RUN_BACKGROUND_WORKER`: start the in-process polling worker from the web app
