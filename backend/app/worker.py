"""
Queue + Worker — all in one file.

The queue is a simple asyncio.Queue (in-memory).
In production this would be replaced by AWS SQS — the worker would
poll SQS instead of a local queue, but the process_job logic stays the same.

The worker runs as a background task inside the same FastAPI process.
"""
import asyncio
import logging
from datetime import datetime

from app.db import SessionLocal, IngestionJob
from app.pipeline import run_pipeline

logger = logging.getLogger(__name__)

# Global in-memory queue — shared between the API (puts jobs) and the worker (gets jobs)
# In production: replace this with an SQS client
queue: asyncio.Queue = asyncio.Queue()


async def worker_loop():
    """
    Runs forever in the background.
    Picks jobs off the queue one at a time and processes them.
    """
    logger.info("Worker ready")
    while True:
        job_data = await queue.get()  # waits until a job arrives
        await process_job(job_data)


async def process_job(job_data: dict):
    """
    Runs the ingestion pipeline for one uploaded file.
    Updates the job status in the DB when done.
    """
    job_id   = job_data["job_id"]
    db = SessionLocal()

    try:
        result = run_pipeline(
            file_type = job_data["file_type"],
            content   = job_data["content"],
            filename  = job_data["filename"],
            db        = db,
            source    = job_data.get("source"),
        )

        # Mark job as done and store result summary
        job = db.query(IngestionJob).filter_by(id=job_id).first()
        job.status            = "done"
        job.records_processed = result["processed"]
        job.warning_count     = result["warnings"]
        job.error_count       = result["errors"]
        job.error_detail      = result["detail"][:100]  # cap at 100 rows
        job.completed_at      = datetime.utcnow()
        db.commit()

        logger.info("Job %s done — %d processed, %d warnings, %d errors",
                    job_id, result["processed"], result["warnings"], result["errors"])

    except Exception as e:
        logger.exception("Job %s failed: %s", job_id, e)
        db.rollback()
        job = db.query(IngestionJob).filter_by(id=job_id).first()
        job.status       = "failed"
        job.error_detail = [{"error": str(e)}]
        job.completed_at = datetime.utcnow()
        db.commit()

    finally:
        db.close()
