from __future__ import annotations

import os
from dataclasses import dataclass


def _bool_env(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    hospital_api_base_url: str = os.getenv(
        "HOSPITAL_API_BASE_URL", "https://hospital-directory.onrender.com"
    ).rstrip("/")
    bulk_sync_row_limit: int = int(os.getenv("BULK_SYNC_ROW_LIMIT", "20"))
    bulk_chunk_size: int = int(os.getenv("BULK_CHUNK_SIZE", "1000"))
    bulk_concurrency: int = int(os.getenv("BULK_CONCURRENCY", "32"))
    request_timeout_seconds: float = float(os.getenv("REQUEST_TIMEOUT_SECONDS", "15"))
    request_max_attempts: int = int(os.getenv("REQUEST_MAX_ATTEMPTS", "5"))
    worker_poll_seconds: float = float(os.getenv("WORKER_POLL_SECONDS", "1.5"))
    run_background_worker: bool = _bool_env("RUN_BACKGROUND_WORKER", True)


settings = Settings()
