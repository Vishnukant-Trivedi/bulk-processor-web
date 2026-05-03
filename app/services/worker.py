from __future__ import annotations

import asyncio
import logging
from typing import Optional

from app.core.settings import settings
from app.services.bulk import BulkJobService


logger = logging.getLogger(__name__)


async def run_worker(service: Optional[BulkJobService] = None) -> None:
    service = service or BulkJobService()
    while True:
        try:
            batch_id = service.find_next_queued_job()
            if not batch_id:
                await asyncio.sleep(settings.worker_poll_seconds)
                continue
            logger.info("Processing batch %s", batch_id)
            await service.process_job(batch_id, collect_results=False)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Worker loop error")
            await asyncio.sleep(settings.worker_poll_seconds)
