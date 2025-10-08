"""SQLite helper utilities for file catalog indexing and browsing."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple

from . import DEFAULT_DB_PATH

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS files (
    file_id INTEGER PRIMARY KEY,
    root_path TEXT NOT NULL,
    path TEXT NOT NULL UNIQUE,
    file_name TEXT,
    subfolder TEXT,
    extension TEXT,
    size_bytes INTEGER,
    created_at TEXT,
    created_by TEXT,
    modified_at TEXT,
    modified_by TEXT
);

CREATE INDEX IF NOT EXISTS idx_files_root_subfolder
    ON files(root_path, subfolder);
CREATE INDEX IF NOT EXISTS idx_files_root_extension
    ON files(root_path, extension);
CREATE INDEX IF NOT EXISTS idx_files_root_modified
    ON files(root_path, modified_at);
"""


def ensure_db(db_path: Path = DEFAULT_DB_PATH) -> sqlite3.Connection:
    """Create the database file and schema if needed."""
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    _ensure_schema(con)
    return con


def _ensure_schema(con: sqlite3.Connection) -> None:
    """Ensure the files table matches the expected schema, rebuilding if needed."""
    needs_reset = False
    try:
        info_rows = con.execute("PRAGMA table_info(files)").fetchall()
    except sqlite3.DatabaseError:
        info_rows = []
    if info_rows:
        column_names = {row["name"] for row in info_rows if "name" in row.keys()}
        if "root_path" not in column_names:
            needs_reset = True
    if needs_reset:
        with con:
            con.execute("DROP TABLE IF EXISTS files")
            con.execute("DROP INDEX IF EXISTS idx_files_root_subfolder")
            con.execute("DROP INDEX IF EXISTS idx_files_root_extension")
            con.execute("DROP INDEX IF EXISTS idx_files_root_modified")
    with con:
        con.executescript(SCHEMA)


def clear_root(con: sqlite3.Connection, root_path: Path) -> None:
    """Remove previous records for the supplied root path."""
    with con:
        con.execute("DELETE FROM files WHERE root_path = ?", (str(root_path),))


def insert_files(con: sqlite3.Connection, rows: Iterable[Tuple]) -> None:
    """Insert a collection of file metadata rows."""
    with con:
        con.executemany(
            """
            INSERT OR REPLACE INTO files(
                root_path,
                path,
                file_name,
                subfolder,
                extension,
                size_bytes,
                created_at,
                created_by,
                modified_at,
                modified_by
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )


def list_roots(con: sqlite3.Connection) -> List[str]:
    """Return distinct root paths that have been indexed."""
    rows = con.execute(
        "SELECT DISTINCT root_path FROM files ORDER BY root_path"
    ).fetchall()
    return [row["root_path"] for row in rows]


def list_extensions(con: sqlite3.Connection, root_path: str) -> List[str]:
    """Return distinct extensions for a root."""
    rows = con.execute(
        """
        SELECT DISTINCT COALESCE(extension, '') AS extension
        FROM files
        WHERE root_path = ?
        ORDER BY extension
        """,
        (root_path,),
    ).fetchall()
    return [row["extension"] for row in rows]


def list_subfolders(con: sqlite3.Connection, root_path: str) -> List[str]:
    """Return distinct subfolders for a root."""
    rows = con.execute(
        """
        SELECT DISTINCT COALESCE(subfolder, '/') AS subfolder
        FROM files
        WHERE root_path = ?
        ORDER BY subfolder
        """,
        (root_path,),
    ).fetchall()
    return [row["subfolder"] for row in rows]


def count_files(
    con: sqlite3.Connection,
    root_path: str,
    extensions: Optional[Sequence[str]] = None,
    subfolder_like: Optional[str] = None,
) -> int:
    """Return the number of files for a given root with optional filters."""
    where_clauses = ["root_path = ?"]
    params: List[object] = [root_path]
    if extensions:
        placeholders = ",".join("?" for _ in extensions)
        where_clauses.append(f"COALESCE(extension, '') IN ({placeholders})")
        params.extend(extensions)
    if subfolder_like:
        where_clauses.append("COALESCE(subfolder, '/') LIKE ?")
        params.append(f"%{subfolder_like}%")

    query = f"SELECT COUNT(*) AS total FROM files WHERE {' AND '.join(where_clauses)}"
    row = con.execute(query, tuple(params)).fetchone()
    return int(row["total"] if row else 0)


def fetch_files(
    con: sqlite3.Connection,
    root_path: str,
    page: int,
    page_size: int,
    extensions: Optional[Sequence[str]] = None,
    subfolder_like: Optional[str] = None,
    sort_column: str = "subfolder",
    sort_direction: str = "ASC",
) -> List[dict]:
    """Return a page of file metadata dictionaries."""
    allowed_columns = {
        "file_name",
        "subfolder",
        "extension",
        "size_bytes",
        "created_at",
        "created_by",
        "modified_at",
        "modified_by",
    }
    if sort_column not in allowed_columns:
        sort_column = "subfolder"
    direction = "DESC" if sort_direction.upper() == "DESC" else "ASC"

    where_clauses = ["root_path = ?"]
    params: List[object] = [root_path]
    if extensions:
        placeholders = ",".join("?" for _ in extensions)
        where_clauses.append(f"COALESCE(extension, '') IN ({placeholders})")
        params.extend(extensions)
    if subfolder_like:
        where_clauses.append("COALESCE(subfolder, '/') LIKE ?")
        params.append(f"%{subfolder_like}%")
    where_sql = " AND ".join(where_clauses)

    offset = max(page, 0) * page_size
    rows = con.execute(
        f"""
        SELECT
            root_path,
            path,
            file_name,
            subfolder,
            extension,
            size_bytes,
            created_at,
            created_by,
            modified_at,
            modified_by
        FROM files
        WHERE {where_sql}
        ORDER BY {sort_column} {direction}, file_name ASC
        LIMIT ? OFFSET ?
        """,
        (*params, page_size, offset),
    ).fetchall()
    return [dict(row) for row in rows]
