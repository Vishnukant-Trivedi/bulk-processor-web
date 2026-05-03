# Hospital Bulk Processor

Backend service for ingesting small hospital CSV batches into the hospital directory API.

## Overview

This service turns a manual, row-by-row hospital creation workflow into a controlled batch pipeline. It accepts a CSV upload, validates the file, deduplicates repeated hospitals, creates new hospitals through the upstream directory API, activates the resulting batch, and exposes status and retry controls.

The system exists to make bulk ingestion safer, faster, and easier to operate than ad hoc manual creation. It is intentionally lightweight because the batch size is bounded and the operational surface is small.

## Target Users

- Operations and support teams that need to ingest hospital records in controlled batches.
- Product or internal platform users who need visibility into upload outcome, duplicates, and failures.
- Engineering and QA reviewers who need deterministic behavior, clear status transitions, and retry semantics.

## Key Capabilities

- CSV upload and schema validation before any upstream writes.
- In-memory batch state with per-row outcomes.
- Dedupe using normalized hospital identity: `name + address + phone`.
- Asynchronous row processing with bounded concurrency.
- Batch activation after successful creation of hospitals in the batch.
- Polling and SSE progress tracking.
- Resume support for failed rows.
- Row-level inspection for debugging and review.

## User Flow

1. Upload
   - A client submits a multipart CSV file to create a batch.
   - The service generates a batch ID and keeps the parsed rows in memory.

2. Validate
   - The service checks UTF-8 encoding, required headers, required fields, and the 20-row limit.
   - Validation failures return immediately and do not trigger upstream writes.

3. Process
   - Valid rows are processed with bounded concurrency.
   - Duplicate rows are marked and skipped.
   - Successful rows are created in the upstream hospital directory API.
   - After processing, the service attempts batch activation if at least one hospital was created.

4. Track
   - Clients can poll batch status or subscribe to SSE updates.
   - `progress_percent` reflects activated rows only: `created_and_activated_rows / total_rows * 100`.

5. Retry
   - Failed rows can be resumed.
   - On resume, only failed rows are reset to `pending` and retried.
   - Invalid and duplicate rows remain unchanged.

## Batch Lifecycle

### Job states

- `queued`: batch accepted and waiting for worker execution.
- `processing`: worker is actively creating rows upstream.
- `completed`: all rows were resolved successfully or skipped as duplicates, and activation succeeded.
- `completed_with_errors`: at least one row failed validation or upstream create, or activation failed after successful creation.

### Row states

- `pending`: not yet processed, or reset for retry.
- `created`: upstream create succeeded, activation not yet reflected in the row.
- `created_and_activated`: row was created and the batch activation succeeded.
- `failed`: upstream create failed.
- `invalid`: local CSV validation failed for the row.
- `duplicate_in_job`: duplicate within the same upload.
- `duplicate_existing`: duplicate of a hospital already created in the current runtime state.

### Transition rules

- Only `failed` rows are reset to `pending` during resume.
- `invalid` and duplicate rows are terminal and are never retried.
- Batch activation is attempted when at least one row is created.
- If activation succeeds, created rows are marked `created_and_activated`.
- If no rows are created, activation is skipped.

### All-negative batch outcomes

- All rows invalid: batch finishes as `completed_with_errors`.
- All rows failed upstream: batch finishes as `completed_with_errors`.
- All rows duplicate and no creates occur: batch finishes as `completed`.
- Mixed results: created rows still trigger activation; the batch may still end as `completed_with_errors` if failures remain.

## System Architecture

### API Service

- Accepts upload, validation, status, resume, and row-detail requests.
- Returns either a completed response or a queued response for deferred processing.
- Exposes SSE progress events for clients that want live updates.

### In-Memory State Store

- Stores batch metadata, row state, dedupe ledger, and progress information.
- Tracks row lifecycle transitions such as `pending`, `created`, `failed`, `invalid`, and duplicate states.
- Holds all runtime state in process memory.

### Worker Loop

- Polls for queued batches in the same web process.
- Processes rows with bounded concurrency.
- Calls the upstream create and activate endpoints.

### Upstream Hospital Directory API

- Serves as the source of truth for individual hospital creation.
- Receives batch activation after successful row creation.
- Remains authoritative for the final hospital records.

## Design Decisions & Tradeoffs

### In-memory state

The implementation keeps state in memory to reduce complexity and fit the bounded batch size. The tradeoff is that a process restart loses runtime batch state, so this design is not appropriate for large or long-lived jobs.

### Dedupe strategy

The service deduplicates on a normalized business key built from `name`, `address`, and `phone`. This prevents duplicate upstream creates for repeated rows in the same upload and for already-created rows observed during the current process lifetime.

### Async processing

Row processing is asynchronous with bounded concurrency so the request path stays responsive and upstream calls do not run serially. This reduces end-to-end latency while still protecting the upstream API from uncontrolled fan-out.

### Activation semantics

The system attempts batch activation after row processing whenever the batch produced at least one created hospital. Failed, invalid, and duplicate rows remain visible as separate outcomes. This keeps successful work from being blocked by unrelated row-level issues.

## Success Metrics

| Metric | Definition | Initial target |
| --- | --- | --- |
| Validation latency | Time from upload request to validation response | p95 < 100ms for a 20-row CSV |
| Submission latency | Time to accept a batch when `wait=false` | p95 < 250ms |
| Batch completion latency | Time from accepted upload to terminal batch state | p95 < 5s on a healthy upstream |
| Activation success rate | Batches with at least one created row that successfully activate | > 99% |
| Retry recovery rate | Failed rows that become created after resume | > 80% for transient failures |

These metrics should be measured per batch and aggregated over time to detect upstream instability, validation drift, and retry regressions.

## Failure Handling

- Validation failures: malformed files, missing headers, blank required fields, UTF-8 issues, and oversized CSVs are rejected before any upstream writes.
- Row-level failures: individual rows can fail validation or upstream creation without failing the entire batch.
- Partial failures: the batch may finish as `completed_with_errors` even when activation succeeds for created rows.
- Upstream transient failures: the HTTP client retries transient upstream errors with backoff.
- Activation failures: if activation fails after row creation, the batch remains visible as `completed_with_errors` and the created hospitals are not rolled back automatically.
- Resume behavior: `POST /hospitals/bulk/{batch_id}/resume` resets failed rows to `pending` and reprocesses them; completed batches are not resumed.
- Idempotency behavior: already-created rows are not recreated on resume, and duplicate detection prevents repeated work within the current runtime state.

## Testing & Verification

The codebase includes async tests that exercise the key flows:

- CSV validation rejects missing required columns.
- Bulk ingestion deduplicates repeated rows and activates the batch.
- Activation still occurs when some rows fail, as long as there is at least one created row.
- Resume moves failed rows back to `pending`.
- Resume retries failed rows successfully when the upstream failure is transient.

These tests provide coverage for validation, dedupe, partial failure handling, activation semantics, and retry behavior.

## Scalability Considerations

Current limits:

- State is process-local and disappears on restart.
- One web process owns the in-memory batch store.
- SSE progress is suitable for small-scale operational visibility, not for very high fan-out.
- Upstream rate limits and latency directly affect completion time.

Evolution path:

- Move batch state into a durable database.
- Persist row-level outcome and dedupe ledger records.
- Add a queue to decouple request acceptance from processing.
- Run distributed workers for horizontal scaling.
- Store large uploads outside the app process if batch size or file size grows.
- Add stronger idempotency keys and replay protection for external callers.

## API Overview

### `GET /healthz`

Health check.

```json
{"status":"ok"}
```

### `POST /hospitals/bulk/validate`

Validates a CSV before processing.

```json
{
  "valid": true,
  "total_rows": 20,
  "issues": []
}
```

### `POST /hospitals/bulk`

Accepts a CSV upload and processes the batch.

```json
{
  "batch_id": "f2260c10-f0d8-49a7-ab67-776936fd14ff",
  "total_hospitals": 20,
  "processed_hospitals": 20,
  "failed_hospitals": 13,
  "duplicate_hospitals": 0,
  "processing_time_seconds": 1.245,
  "batch_activated": true,
  "status": "completed_with_errors",
  "hospitals": [
    {
      "row": 1,
      "hospital_id": 1001,
      "name": "General Hospital",
      "status": "created_and_activated"
    }
  ],
  "progress_url": "/hospitals/bulk/f2260c10-f0d8-49a7-ab67-776936fd14ff"
}
```

### `GET /hospitals/bulk/{batch_id}`

Returns batch summary, row counts, activation state, and progress.

```json
{
  "batch_id": "f2260c10-f0d8-49a7-ab67-776936fd14ff",
  "status": "completed_with_errors",
  "total_rows": 20,
  "processed_rows": 20,
  "valid_rows": 7,
  "duplicate_rows": 0,
  "failed_rows": 13,
  "created_rows": 7,
  "batch_activated": true,
  "progress_percent": 35.0,
  "source_filename": "sample_hospitals_with_missing.csv",
  "error_message": null
}
```

### `GET /hospitals/bulk/{batch_id}/rows`

Returns row-level status for debugging and review.

### `GET /hospitals/bulk/{batch_id}/events`

Streams batch progress as SSE events: `progress` while work is ongoing and `done` when the batch reaches a terminal state.

### `POST /hospitals/bulk/{batch_id}/resume`

Resets failed rows to `pending` and retries the batch work.

## Observability

- Logs should include `batch_id`, row number, row status, upstream latency, and failure cause.
- Metrics should track upload acceptance, validation failures, row-level success and failure counts, duplicate rate, activation success rate, queue depth, retry recovery, and terminal batch latency.
- Tracing should connect upload request, worker execution, and upstream API calls using `batch_id` as the primary correlation key.

## Security

- Validate file encoding, headers, row count, and required fields before processing.
- Apply rate limiting at the edge or gateway if the service is exposed outside a trusted network.
- Require authentication and authorization for real production use.
- Keep secrets and upstream credentials out of request payloads and logs.
- Restrict CORS if the API is ever called from a browser client.
