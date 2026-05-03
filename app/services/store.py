from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Set


@dataclass
class LedgerEntry:
    batch_id: str
    row_number: int
    hospital_id: int
    name: str
    address: str
    phone: Optional[str]
    active: bool = False
    created_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class RowRecord:
    row: int
    name: str
    address: str
    phone: Optional[str]
    dedupe_key: str
    status: str = "pending"
    hospital_id: Optional[int] = None
    message: Optional[str] = None


@dataclass
class ChunkRecord:
    chunk_number: int
    start_row: int
    end_row: int
    status: str = "pending"


@dataclass
class JobRecord:
    batch_id: str
    source_filename: str
    source_sha256: str
    raw_rows: List[Dict[str, str]]
    status: str = "queued"
    batch_activated: bool = False
    error_message: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    activated_at: Optional[datetime] = None
    rows: Dict[int, RowRecord] = field(default_factory=dict)
    chunks: Dict[int, ChunkRecord] = field(default_factory=dict)
    in_progress_dedupe_keys: Set[str] = field(default_factory=set)

    def touch(self) -> None:
        self.updated_at = datetime.utcnow()


class InMemoryBulkStore:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.jobs: Dict[str, JobRecord] = {}
        self.ledger: Dict[str, LedgerEntry] = {}
        self._created_counter = 1000

    @property
    def lock(self) -> threading.RLock:
        return self._lock

    def reset(self) -> None:
        with self._lock:
            self.jobs.clear()
            self.ledger.clear()
            self._created_counter = 1000

    def next_hospital_id(self) -> int:
        with self._lock:
            self._created_counter += 1
            return self._created_counter

    def create_job(
        self,
        batch_id: str,
        source_filename: str,
        source_sha256: str,
        raw_rows: List[Dict[str, str]],
        chunk_size: int,
    ) -> JobRecord:
        job = JobRecord(
            batch_id=batch_id,
            source_filename=source_filename,
            source_sha256=source_sha256,
            raw_rows=raw_rows,
        )
        for row_index in range(1, len(raw_rows) + 1):
            row = raw_rows[row_index - 1]
            job.rows[row_index] = RowRecord(
                row=row_index,
                name=(row.get("name") or "").strip(),
                address=(row.get("address") or "").strip(),
                phone=(row.get("phone") or "").strip() or None,
                dedupe_key=row.get("__dedupe_key__", ""),
            )
        chunk_number = 1
        for start in range(1, len(raw_rows) + 1, chunk_size):
            end = min(len(raw_rows), start + chunk_size - 1)
            job.chunks[chunk_number] = ChunkRecord(
                chunk_number=chunk_number, start_row=start, end_row=end
            )
            chunk_number += 1
        with self._lock:
            self.jobs[batch_id] = job
        return job

    def get_job(self, batch_id: str) -> Optional[JobRecord]:
        with self._lock:
            return self.jobs.get(batch_id)

    def claim_next_queued_job(self) -> Optional[str]:
        with self._lock:
            queued = [job for job in self.jobs.values() if job.status == "queued"]
            queued.sort(key=lambda item: item.created_at)
            if not queued:
                return None
            job = queued[0]
            job.status = "processing"
            job.touch()
            return job.batch_id

    def summarize_job(self, batch_id: str) -> Optional[dict]:
        with self._lock:
            job = self.jobs.get(batch_id)
            if job is None:
                return None
            rows = list(job.rows.values())
            terminal = {
                "created",
                "created_and_activated",
                "failed",
                "invalid",
                "duplicate_existing",
                "duplicate_in_job",
            }
            processed = sum(1 for row in rows if row.status in terminal)
            created = sum(1 for row in rows if row.status in {"created", "created_and_activated"})
            activated = sum(1 for row in rows if row.status == "created_and_activated")
            failed = sum(1 for row in rows if row.status in {"failed", "invalid"})
            duplicates = sum(1 for row in rows if row.status in {"duplicate_existing", "duplicate_in_job"})
            progress = 0.0 if not rows else round((activated / len(rows)) * 100.0, 2)
            return {
                "batch_id": job.batch_id,
                "status": job.status,
                "total_rows": len(rows),
                "processed_rows": processed,
                "valid_rows": processed - failed,
                "duplicate_rows": duplicates,
                "failed_rows": failed,
                "created_rows": created,
                "batch_activated": job.batch_activated,
                "progress_percent": progress,
                "source_filename": job.source_filename,
                "error_message": job.error_message,
            }


STORE = InMemoryBulkStore()


def new_batch_id() -> str:
    return str(uuid.uuid4())
