"""SQLite helpers for the Content Governance audit application."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from . import DEFAULT_DB_PATH

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS scan_runs (
    run_id INTEGER PRIMARY KEY,
    root_path TEXT UNIQUE,
    queued_at TEXT,
    started_at TEXT,
    completed_at TEXT,
    status TEXT,
    total_files INTEGER DEFAULT 0,
    total_errors INTEGER DEFAULT 0,
    duration_seconds REAL,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS files (
    file_id INTEGER PRIMARY KEY,
    run_id INTEGER NOT NULL,
    path TEXT NOT NULL,
    file_name TEXT,
    subfolder TEXT,
    extension TEXT,
    created_at TEXT,
    modified_at TEXT,
    modified_by TEXT,
    FOREIGN KEY(run_id) REFERENCES scan_runs(run_id),
    UNIQUE(run_id, path)
);

CREATE INDEX IF NOT EXISTS idx_files_run_subfolder
    ON files(run_id, subfolder);
CREATE INDEX IF NOT EXISTS idx_files_run_extension
    ON files(run_id, extension);
CREATE INDEX IF NOT EXISTS idx_files_run_modified
    ON files(run_id, modified_at);
"""


@dataclass
class SortInstruction:
    column_id: str
    direction: str  # "asc" or "desc"


def _utc_now() -> str:
    return datetime.utcnow().isoformat()


def ensure_database(db_path: Optional[Path] = None) -> Path:
    """Ensure the database file exists with the expected schema."""
    database_path = Path(db_path or DEFAULT_DB_PATH)
    database_path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(database_path))
    try:
        with con:
            con.executescript(SCHEMA)
    finally:
        con.close()
    return database_path


def connect(db_path: Optional[Path] = None) -> sqlite3.Connection:
    """Return a SQLite connection configured for row access."""
    database_path = ensure_database(db_path)
    con = sqlite3.connect(str(database_path), check_same_thread=False)
    con.row_factory = sqlite3.Row
    return con


@contextmanager
def managed_connection(
    connection: Optional[sqlite3.Connection],
    db_path: Optional[Path] = None,
):
    """Yield an existing connection or create/close one transparently."""
    if connection is None:
        con = connect(db_path)
        should_close = True
    else:
        con = connection
        should_close = False
    try:
        yield con
    finally:
        if should_close:
            con.close()


def prepare_run(
    root_path: Path,
    db_path: Optional[Path] = None,
    connection: Optional[sqlite3.Connection] = None,
) -> int:
    """Create or reset a scan run row and return its run_id."""
    resolved = Path(root_path).resolve()
    now = _utc_now()
    with managed_connection(connection, db_path) as con:
        with con:
            row = con.execute(
                "SELECT run_id FROM scan_runs WHERE root_path = ?",
                (str(resolved),),
            ).fetchone()
            if row:
                run_id = int(row["run_id"])
                con.execute(
                    """
                    UPDATE scan_runs
                    SET queued_at = ?, started_at = NULL, completed_at = NULL,
                        status = 'queued', total_files = 0, total_errors = 0,
                        duration_seconds = NULL, error_message = NULL
                    WHERE run_id = ?
                    """,
                    (now, run_id),
                )
                con.execute("DELETE FROM files WHERE run_id = ?", (run_id,))
            else:
                con.execute(
                    """
                    INSERT INTO scan_runs(root_path, queued_at, status)
                    VALUES (?, ?, 'queued')
                    """,
                    (str(resolved), now),
                )
                run_id = int(
                    con.execute(
                        "SELECT run_id FROM scan_runs WHERE root_path = ?",
                        (str(resolved),),
                    ).fetchone()["run_id"]
                )
    return run_id


def mark_run_started(
    run_id: int,
    db_path: Optional[Path] = None,
    connection: Optional[sqlite3.Connection] = None,
) -> None:
    """Set run state to running."""
    now = _utc_now()
    with managed_connection(connection, db_path) as con:
        with con:
            con.execute(
                """
                UPDATE scan_runs
                SET started_at = ?, status = 'running'
                WHERE run_id = ?
                """,
                (now, run_id),
            )


def update_run_progress(
    run_id: int,
    total_files: int,
    total_errors: int,
    db_path: Optional[Path] = None,
    connection: Optional[sqlite3.Connection] = None,
) -> None:
    """Persist incremental progress metrics for a run."""
    with managed_connection(connection, db_path) as con:
        with con:
            con.execute(
                """
                UPDATE scan_runs
                SET total_files = ?, total_errors = ?
                WHERE run_id = ?
                """,
                (total_files, total_errors, run_id),
            )


def finalize_run(
    run_id: int,
    total_files: int,
    total_errors: int,
    duration_seconds: float,
    status: str = "completed",
    error_message: Optional[str] = None,
    db_path: Optional[Path] = None,
    connection: Optional[sqlite3.Connection] = None,
) -> None:
    """Mark a run as finished."""
    completed_at = _utc_now()
    with managed_connection(connection, db_path) as con:
        with con:
            con.execute(
                """
                UPDATE scan_runs
                SET completed_at = ?, status = ?, total_files = ?, total_errors = ?,
                    duration_seconds = ?, error_message = ?
                WHERE run_id = ?
                """,
                (
                    completed_at,
                    status,
                    total_files,
                    total_errors,
                    duration_seconds,
                    error_message,
                    run_id,
                ),
            )


def insert_file_batch(
    run_id: int,
    rows: Iterable[Tuple[str, str, str, str, str, Optional[str], Optional[str]]],
    db_path: Optional[Path] = None,
    connection: Optional[sqlite3.Connection] = None,
) -> None:
    """Insert or update a batch of file metadata rows."""
    with managed_connection(connection, db_path) as con:
        with con:
            con.executemany(
                """
                INSERT INTO files(
                    run_id, path, file_name, subfolder, extension,
                    created_at, modified_at, modified_by
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id, path) DO UPDATE SET
                    file_name = excluded.file_name,
                    subfolder = excluded.subfolder,
                    extension = excluded.extension,
                    created_at = COALESCE(excluded.created_at, files.created_at),
                    modified_at = COALESCE(excluded.modified_at, files.modified_at),
                    modified_by = COALESCE(excluded.modified_by, files.modified_by)
                """,
                ((run_id, *row) for row in rows),
            )


def fetch_run(
    run_id: int,
    db_path: Optional[Path] = None,
    connection: Optional[sqlite3.Connection] = None,
) -> Optional[Dict[str, object]]:
    """Return a single run summary as a dictionary."""
    with managed_connection(connection, db_path) as con:
        row = con.execute(
            "SELECT * FROM scan_runs WHERE run_id = ?",
            (run_id,),
        ).fetchone()
        return dict(row) if row else None


def fetch_run_by_path(
    root_path: Path,
    db_path: Optional[Path] = None,
    connection: Optional[sqlite3.Connection] = None,
) -> Optional[Dict[str, object]]:
    """Return the run keyed by root path."""
    resolved = str(Path(root_path).resolve())
    with managed_connection(connection, db_path) as con:
        row = con.execute(
            "SELECT * FROM scan_runs WHERE root_path = ?",
            (resolved,),
        ).fetchone()
        return dict(row) if row else None


def list_runs(
    db_path: Optional[Path] = None,
    connection: Optional[sqlite3.Connection] = None,
) -> List[Dict[str, object]]:
    """Return all scan runs ordered by most recent activity."""
    with managed_connection(connection, db_path) as con:
        rows = con.execute(
            """
            SELECT *
            FROM scan_runs
            ORDER BY
                CASE status WHEN 'running' THEN 0 WHEN 'queued' THEN 1 ELSE 2 END,
                COALESCE(completed_at, started_at, queued_at) DESC
            """
        ).fetchall()
        return [dict(row) for row in rows]


def fetch_extensions(
    run_id: int,
    db_path: Optional[Path] = None,
    connection: Optional[sqlite3.Connection] = None,
) -> List[str]:
    """Return distinct file extensions for a run."""
    with managed_connection(connection, db_path) as con:
        rows = con.execute(
            """
            SELECT DISTINCT extension
            FROM files
            WHERE run_id = ?
            ORDER BY extension
            """,
            (run_id,),
        ).fetchall()
        return [row["extension"] or "" for row in rows]


def fetch_page(
    run_id: int,
    page_number: int,
    page_size: int,
    sort_instructions: Sequence[SortInstruction],
    extensions: Optional[Sequence[str]] = None,
    subfolder_contains: Optional[str] = None,
    db_path: Optional[Path] = None,
    connection: Optional[sqlite3.Connection] = None,
) -> Tuple[List[Dict[str, object]], int]:
    """Return a page of file rows along with the total row count."""
    where_clauses = ["run_id = ?"]
    params: List[object] = [run_id]

    if extensions:
        placeholders = ",".join("?" for _ in extensions)
        where_clauses.append(f"extension IN ({placeholders})")
        params.extend(extensions)

    if subfolder_contains:
        where_clauses.append("subfolder LIKE ?")
        params.append(f"%{subfolder_contains}%")

    where_sql = " AND ".join(where_clauses)
    order_sql = _build_order_clause(sort_instructions)

    offset = max(page_number, 0) * page_size

    with managed_connection(connection, db_path) as con:
        total = con.execute(
            f"SELECT COUNT(*) AS total FROM files WHERE {where_sql}",
            tuple(params),
        ).fetchone()["total"]

        rows = con.execute(
            f"""
            SELECT file_name, subfolder, created_at, modified_at, modified_by, extension, path
            FROM files
            WHERE {where_sql}
            {order_sql}
            LIMIT ? OFFSET ?
            """,
            (*params, page_size, offset),
        ).fetchall()
        return [dict(row) for row in rows], int(total)


def _build_order_clause(sort_instructions: Sequence[SortInstruction]) -> str:
    allowed_columns = {
        "file_name",
        "subfolder",
        "created_at",
        "modified_at",
        "modified_by",
        "extension",
    }
    parts: List[str] = []
    for instruction in sort_instructions[:3]:
        if instruction.column_id not in allowed_columns:
            continue
        direction = "ASC" if instruction.direction.lower() == "asc" else "DESC"
        parts.append(f"{instruction.column_id} {direction}")
    if not parts:
        parts.append("file_name ASC")
    return "ORDER BY " + ", ".join(parts)
