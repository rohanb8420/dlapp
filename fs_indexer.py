"""Recursive filesystem indexer that writes folder and file metadata to SQLite."""

import argparse
import hashlib
import logging
import mimetypes
import os
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Tuple

LOGGER = logging.getLogger(__name__)
DEFAULT_DB = Path("artifacts") / "fs_index.db"

SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS directories (
    dir_id INTEGER PRIMARY KEY,
    path TEXT UNIQUE,
    parent_path TEXT,
    name TEXT,
    depth INTEGER,
    created_ts TEXT,
    modified_ts TEXT,
    num_files INTEGER,
    num_subdirs INTEGER,
    walk_error TEXT
);

CREATE TABLE IF NOT EXISTS files (
    file_id INTEGER PRIMARY KEY,
    dir_id INTEGER,
    path TEXT UNIQUE,
    name TEXT,
    extension TEXT,
    mime_type TEXT,
    size_bytes INTEGER,
    created_ts TEXT,
    modified_ts TEXT,
    sha1 TEXT,
    read_error TEXT,
    FOREIGN KEY(dir_id) REFERENCES directories(dir_id)
);

CREATE INDEX IF NOT EXISTS idx_directories_parent ON directories(parent_path);
CREATE INDEX IF NOT EXISTS idx_files_dir ON files(dir_id);
CREATE INDEX IF NOT EXISTS idx_files_extension ON files(extension);
"""


def ensure_db(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(db_path))
    con.execute("PRAGMA foreign_keys = ON")
    with con:
        con.executescript(SCHEMA)
    return con


def sha1_of_file(path: Path) -> Optional[str]:
    try:
        h = hashlib.sha1()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                h.update(chunk)
        return h.hexdigest()
    except Exception as exc:  # pragma: no cover
        LOGGER.debug("sha1 failed for %s: %s", path, exc)
        return None


def _timestamps(st: os.stat_result) -> Tuple[str, str]:
    created = datetime.fromtimestamp(st.st_ctime).isoformat()
    modified = datetime.fromtimestamp(st.st_mtime).isoformat()
    return created, modified


def record_directory(
    con: sqlite3.Connection,
    path: Path,
    parent_path: Optional[Path],
    depth: int,
    num_files: int,
    num_subdirs: int,
    error: Optional[str] = None,
) -> int:
    try:
        st = path.stat()
        created_ts, modified_ts = _timestamps(st)
        stat_error = None
    except Exception as exc:
        created_ts = modified_ts = None
        stat_error = f"stat_error: {exc}"
    meta: Dict[str, Optional[str]] = {
        "path": str(path),
        "parent_path": str(parent_path) if parent_path else None,
        "name": path.name or str(path),
        "depth": depth,
        "created_ts": created_ts,
        "modified_ts": modified_ts,
        "num_files": num_files,
        "num_subdirs": num_subdirs,
        "walk_error": error or stat_error,
    }
    with con:
        con.execute(
            """
            INSERT INTO directories(path, parent_path, name, depth, created_ts, modified_ts,
                                    num_files, num_subdirs, walk_error)
            VALUES (:path, :parent_path, :name, :depth, :created_ts, :modified_ts,
                    :num_files, :num_subdirs, :walk_error)
            ON CONFLICT(path) DO UPDATE SET
                parent_path=excluded.parent_path,
                name=excluded.name,
                depth=excluded.depth,
                created_ts=COALESCE(excluded.created_ts, directories.created_ts),
                modified_ts=COALESCE(excluded.modified_ts, directories.modified_ts),
                num_files=excluded.num_files,
                num_subdirs=excluded.num_subdirs,
                walk_error=excluded.walk_error
            """,
            meta,
        )
        (dir_id,) = con.execute(
            "SELECT dir_id FROM directories WHERE path = ?",
            (str(path),),
        ).fetchone()
    return int(dir_id)


def record_file(
    con: sqlite3.Connection,
    dir_id: int,
    path: Path,
    compute_hash: bool,
) -> None:
    try:
        st = path.stat()
        created_ts, modified_ts = _timestamps(st)
        size_bytes = int(st.st_size)
        read_error = None
    except Exception as exc:
        created_ts = modified_ts = None
        size_bytes = None
        read_error = f"stat_error: {exc}"
    sha1 = sha1_of_file(path) if compute_hash and read_error is None else None
    meta = {
        "dir_id": dir_id,
        "path": str(path),
        "name": path.name,
        "extension": path.suffix.lower().lstrip("."),
        "mime_type": mimetypes.guess_type(str(path))[0] or "",
        "size_bytes": size_bytes,
        "created_ts": created_ts,
        "modified_ts": modified_ts,
        "sha1": sha1,
        "read_error": read_error,
    }
    with con:
        con.execute(
            """
            INSERT INTO files(dir_id, path, name, extension, mime_type, size_bytes,
                              created_ts, modified_ts, sha1, read_error)
            VALUES (:dir_id, :path, :name, :extension, :mime_type, :size_bytes,
                    :created_ts, :modified_ts, :sha1, :read_error)
            ON CONFLICT(path) DO UPDATE SET
                dir_id=excluded.dir_id,
                name=excluded.name,
                extension=excluded.extension,
                mime_type=excluded.mime_type,
                size_bytes=excluded.size_bytes,
                created_ts=COALESCE(excluded.created_ts, files.created_ts),
                modified_ts=COALESCE(excluded.modified_ts, files.modified_ts),
                sha1=COALESCE(excluded.sha1, files.sha1),
                read_error=excluded.read_error
            """,
            meta,
        )


def crawl_filesystem(root: Path, db_path: Path, compute_hash: bool = False) -> Dict[str, int]:
    con = ensure_db(db_path)
    stats = {"directories": 0, "files": 0, "errors": 0}
    root = root.resolve()
    if not root.exists():
        raise FileNotFoundError(f"Root path does not exist: {root}")
    if not root.is_dir():
        raise NotADirectoryError(f"Root path is not a directory: {root}")
    LOGGER.info("Starting crawl at %s", root)

    for current_root, dirs, files in os.walk(root, followlinks=False):
        current_path = Path(current_root)
        relative = current_path.relative_to(root) if current_path != root else Path('.')
        depth = 0 if str(relative) in ('.', '') else len(relative.parts)
        parent = current_path.parent if current_path != root else None
        try:
            dir_id = record_directory(
                con,
                current_path,
                parent,
                depth=depth,
                num_files=len(files),
                num_subdirs=len(dirs),
            )
            stats["directories"] += 1
        except Exception:  # pragma: no cover - defensive logging
            LOGGER.exception("Failed to record directory %s", current_path)
            stats["errors"] += 1
            dir_id = None
        for name in files:
            if dir_id is None:
                continue
            file_path = current_path / name
            try:
                record_file(con, dir_id, file_path, compute_hash=compute_hash)
                stats["files"] += 1
            except Exception:  # pragma: no cover
                LOGGER.exception("Failed to record file %s", file_path)
                stats["errors"] += 1
    con.close()
    LOGGER.info(
        "Crawl complete | directories=%s files=%s errors=%s",
        stats["directories"],
        stats["files"],
        stats["errors"],
    )
    return stats


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Recursively crawl a folder and index metadata into SQLite."
    )
    parser.add_argument(
        "root",
        type=Path,
        help="Directory to crawl recursively.",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=str(DEFAULT_DB),
        help="Destination SQLite database path (default artifacts/fs_index.db).",
    )
    parser.add_argument(
        "--hash",
        action="store_true",
        help="Compute SHA1 hashes for files (slower).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
        help="Logging verbosity.",
    )
    return parser.parse_args()


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


def main() -> None:
    args = parse_args()
    configure_logging(args.log_level)
    try:
        stats = crawl_filesystem(args.root, Path(args.db), compute_hash=args.hash)
    except Exception as exc:
        LOGGER.error("Crawl failed: %s", exc)
        raise SystemExit(1) from exc
    LOGGER.info(
        "Indexed %s files across %s directories (errors=%s)",
        stats["files"],
        stats["directories"],
        stats["errors"],
    )


if __name__ == "__main__":
    main()
