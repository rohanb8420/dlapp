"""High-volume filesystem crawler for the Content Governance audit app."""

from __future__ import annotations

import os
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable, List, Optional, Sequence, Tuple

from . import DEFAULT_DB_PATH
from . import audit_db


@dataclass
class IndexStats:
    """Lightweight progress snapshot for UI polling."""

    processed_files: int = 0
    error_count: int = 0
    current_path: Optional[str] = None


@dataclass
class IndexResult:
    """Summary returned after a crawl completes."""

    run_id: int
    total_files: int
    error_count: int
    duration_seconds: float


def _owner_for(path: Path) -> Optional[str]:
    try:
        return path.owner()
    except Exception:
        return None


def _timestamps(path: Path) -> Tuple[Optional[str], Optional[str]]:
    try:
        stat_result = path.stat()
    except Exception:
        return None, None
    created = datetime.fromtimestamp(stat_result.st_ctime).isoformat()
    modified = datetime.fromtimestamp(stat_result.st_mtime).isoformat()
    return created, modified


def _subfolder(root: Path, file_path: Path) -> str:
    """Return a normalized subfolder string relative to the root."""
    try:
        relative_parent = file_path.parent.relative_to(root)
    except ValueError:
        return str(file_path.parent).replace("\\", "/")
    if str(relative_parent) in ("", "."):
        return "/"
    return str(relative_parent).replace("\\", "/")


def index_path(
    root_path: Path,
    run_id: int,
    db_path: Optional[Path] = None,
    progress_callback: Optional[Callable[[IndexStats], None]] = None,
    batch_size: int = 500,
) -> IndexResult:
    """Crawl `root_path` recursively and persist file metadata."""
    root = Path(root_path).resolve()
    if not root.exists():
        raise FileNotFoundError(f"Root path does not exist: {root}")
    if not root.is_dir():
        raise NotADirectoryError(f"Root path is not a directory: {root}")

    database_path = audit_db.ensure_database(db_path or DEFAULT_DB_PATH)
    connection = audit_db.connect(database_path)

    total_files = 0
    error_count = 0
    start = time.time()

    try:
        audit_db.mark_run_started(run_id, connection=connection)
        batch: List[Tuple[str, str, str, str, Optional[str], Optional[str], Optional[str]]] = []

        for current_root, _, files in os.walk(root, followlinks=False):
            current_dir = Path(current_root)
            for name in files:
                file_path = current_dir / name
                created_ts, modified_ts = _timestamps(file_path)
                if created_ts is None and modified_ts is None:
                    error_count += 1
                    continue
                subfolder = _subfolder(root, file_path)
                extension = file_path.suffix.lower().lstrip(".")
                owner = _owner_for(file_path)
                batch.append(
                    (
                        str(file_path),
                        file_path.name,
                        subfolder,
                        extension,
                        created_ts,
                        modified_ts,
                        owner,
                    )
                )
                if len(batch) >= batch_size:
                    processed = len(batch)
                    _flush_batch(
                        connection,
                        run_id,
                        batch,
                        total_files,
                        error_count,
                        progress_callback,
                        current=str(file_path),
                    )
                    total_files += processed
                    batch.clear()

        if batch:
            processed = len(batch)
            _flush_batch(
                connection,
                run_id,
                batch,
                total_files,
                error_count,
                progress_callback,
                current=batch[-1][0],
            )
            total_files += processed
            batch.clear()

        duration = max(time.time() - start, 0.0)
        audit_db.finalize_run(
            run_id,
            total_files=total_files,
            total_errors=error_count,
            duration_seconds=duration,
            status="completed",
            connection=connection,
        )
        return IndexResult(
            run_id=run_id,
            total_files=total_files,
            error_count=error_count,
            duration_seconds=duration,
        )
    except Exception as exc:  # pragma: no cover - defensive
        duration = max(time.time() - start, 0.0)
        audit_db.finalize_run(
            run_id,
            total_files=total_files,
            total_errors=error_count + 1,
            duration_seconds=duration,
            status="failed",
            error_message=str(exc),
            connection=connection,
        )
        raise
    finally:
        connection.close()


def _flush_batch(
    connection: sqlite3.Connection,
    run_id: int,
    batch: Sequence[Tuple[str, str, str, str, Optional[str], Optional[str], Optional[str]]],
    processed_so_far: int,
    error_count: int,
    progress_callback: Optional[Callable[[IndexStats], None]],
    current: Optional[str],
) -> None:
    audit_db.insert_file_batch(
        run_id,
        batch,
        connection=connection,
    )
    new_total = processed_so_far + len(batch)
    audit_db.update_run_progress(
        run_id,
        total_files=new_total,
        total_errors=error_count,
        connection=connection,
    )
    if progress_callback:
        progress_callback(
            IndexStats(
                processed_files=new_total,
                error_count=error_count,
                current_path=current,
            )
        )
