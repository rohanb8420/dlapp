"""Threaded job manager coordinating filesystem crawls for the audit app."""

from __future__ import annotations

import threading
import time
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, Optional

import logging

from . import DEFAULT_DB_PATH
from . import audit_db
from .audit_indexer import IndexResult, IndexStats, index_path


@dataclass
class JobStatus:
    """In-memory representation of the crawler state."""

    job_id: str
    run_id: int
    root_path: str
    status: str = "idle"
    message: str = ""
    processed_files: int = 0
    error_count: int = 0
    current_path: Optional[str] = None
    started_at: Optional[float] = None
    finished_at: Optional[float] = None
    duration_seconds: Optional[float] = None

    def to_dict(self) -> Dict[str, Optional[str]]:
        payload = asdict(self)
        if self.started_at is not None and self.duration_seconds is None and self.finished_at:
            payload["duration_seconds"] = max(self.finished_at - self.started_at, 0.0)
        return payload


class JobAlreadyRunningError(RuntimeError):
    """Raised when a new job is requested while another is running."""


LOGGER = logging.getLogger(__name__)


class JobManager:
    """Simple single-worker manager used by the Dash app."""

    def __init__(self, db_path: Path = DEFAULT_DB_PATH) -> None:
        self.db_path = Path(db_path)
        self._lock = threading.Lock()
        self._job: Optional[JobStatus] = None
        self._thread: Optional[threading.Thread] = None
        audit_db.ensure_database(self.db_path)
        LOGGER.debug("JobManager initialized with DB at %s", self.db_path)

    def start(self, root_path: str) -> JobStatus:
        """Queue a new crawl job."""
        root = Path(root_path).expanduser()
        with self._lock:
            if self._job and self._job.status == "running":
                raise JobAlreadyRunningError("A crawl is already in progress.")
            run_id = audit_db.prepare_run(root, db_path=self.db_path)
            job = JobStatus(
                job_id=str(uuid.uuid4()),
                run_id=run_id,
                root_path=str(root.resolve()),
                status="queued",
                message="Queued (preparing to index)",
            )
            self._job = job
            self._thread = threading.Thread(target=self._worker, args=(job,), daemon=True)
            self._thread.start()
            LOGGER.info("Started crawl job %s for %s (run_id=%s)", job.job_id, job.root_path, run_id)
            return job

    def get_status(self) -> Optional[JobStatus]:
        """Return a snapshot of the current job."""
        with self._lock:
            if self._job is None:
                return None
            return JobStatus(**self._job.to_dict())

    def has_active_job(self) -> bool:
        with self._lock:
            return bool(self._job and self._job.status in {"queued", "running"})

    def _worker(self, job: JobStatus) -> None:
        with self._lock:
            job.status = "running"
            job.message = "Indexing…"
            job.started_at = time.time()
        LOGGER.info("Job %s running against %s", job.job_id, job.root_path)

        try:
            result = index_path(
                Path(job.root_path),
                job.run_id,
                db_path=self.db_path,
                progress_callback=self._on_progress,
            )
            self._on_complete(job, result, status="completed", message="Scan complete.")
            LOGGER.info(
                "Job %s completed: files=%s errors=%s duration=%.2fs",
                job.job_id,
                result.total_files,
                result.error_count,
                result.duration_seconds,
            )
        except Exception as exc:  # pragma: no cover - defensive logging
            LOGGER.exception("Job %s failed", job.job_id)
            self._on_complete(
                job,
                IndexResult(
                    run_id=job.run_id,
                    total_files=job.processed_files,
                    error_count=job.error_count + 1,
                    duration_seconds=0.0,
                ),
                status="failed",
                message=f"Scan failed: {exc}",
            )

    def _on_progress(self, stats: IndexStats) -> None:
        with self._lock:
            if not self._job or self._job.status != "running":
                return
            self._job.processed_files = stats.processed_files
            self._job.error_count = stats.error_count
            self._job.current_path = stats.current_path
            self._job.message = (
                f"Indexed {stats.processed_files:,} files"
                if stats.processed_files
                else "Indexing…"
            )
        LOGGER.debug(
            "Job %s progress: files=%s errors=%s last=%s",
            self._job.job_id if self._job else "n/a",
            stats.processed_files,
            stats.error_count,
            stats.current_path,
        )

    def _on_complete(
        self,
        job: JobStatus,
        result: IndexResult,
        status: str,
        message: str,
    ) -> None:
        finished = time.time()
        with self._lock:
            job.status = status
            job.message = message
            job.finished_at = finished
            job.processed_files = result.total_files
            job.error_count = result.error_count
            job.duration_seconds = result.duration_seconds or (
                finished - (job.started_at or finished)
            )
            self._thread = None
        LOGGER.info(
            "Job %s final state=%s files=%s errors=%s duration=%.2fs",
            job.job_id,
            status,
            result.total_files,
            result.error_count,
            job.duration_seconds or 0.0,
        )


manager = JobManager()
