from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api import router
from app.core.logging import configure_logging
from app.core.settings import settings
from app.services.bulk import BulkJobService
from app.services.worker import run_worker


configure_logging()


@asynccontextmanager
async def lifespan(app: FastAPI):
    worker_task = None
    if settings.run_background_worker:
        worker_task = asyncio.create_task(run_worker(BulkJobService()))
    try:
        yield
    finally:
        if worker_task:
            worker_task.cancel()
            try:
                await worker_task
            except asyncio.CancelledError:
                pass


app = FastAPI(title="Hospital Bulk Processor", version="0.1.0", lifespan=lifespan)
app.include_router(router)
