"""Interactive CLI script to index filesystem metadata into SQLite."""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

from . import DEFAULT_DB_PATH
from .catalog_db import clear_root, ensure_db, insert_files


def prompt_for_path() -> Path:
    """Prompt the user for a directory path to index."""
    try:
        raw = input("Enter the folder path to index: ").strip()
    except EOFError:  # pragma: no cover - interactive use
        raise SystemExit("No input detected. Exiting.") from None
    if not raw:
        raise SystemExit("Empty input. Exiting.")
    path = Path(raw).expanduser()
    if not path.exists():
        raise SystemExit(f"Path not found: {path}")
    if not path.is_dir():
        raise SystemExit(f"Not a directory: {path}")
    return path.resolve()


def stat_metadata(path: Path) -> Tuple[Optional[str], Optional[str], Optional[int]]:
    """Return created_at, modified_at ISO strings and size in bytes."""
    try:
        stats = path.stat()
    except Exception as exc:  # pragma: no cover - defensive
        print(f"  ! stat failed: {exc}")
        return None, None, None
    created = datetime.fromtimestamp(stats.st_ctime).isoformat()
    modified = datetime.fromtimestamp(stats.st_mtime).isoformat()
    size_bytes = int(stats.st_size)
    return created, modified, size_bytes


def owner(path: Path) -> Optional[str]:
    """Return the filesystem owner if available."""
    try:
        return path.owner()
    except Exception:
        return None


def normalize_subfolder(root: Path, file_path: Path) -> str:
    """Compute relative subfolder path for display."""
    try:
        relative = file_path.parent.relative_to(root)
    except ValueError:
        return str(file_path.parent)
    if str(relative) in ("", "."):
        return "/"
    return str(relative)


def build_rows(
    root: Path,
    files: Iterable[Path],
) -> Iterable[Tuple]:
    """Yield row tuples suitable for database insertion."""
    for file_path in files:
        created_at, modified_at, size_bytes = stat_metadata(file_path)
        modified_by = owner(file_path)
        created_by = modified_by  # best available approximation
        subfolder = normalize_subfolder(root, file_path)
        extension = file_path.suffix.lower().lstrip(".")
        yield (
            str(root),
            str(file_path),
            file_path.name,
            subfolder,
            extension,
            size_bytes,
            created_at,
            created_by,
            modified_at,
            modified_by,
        )


def index_path(root: Path, db_path: Path = DEFAULT_DB_PATH, batch_size: int = 200) -> None:
    """Recursively index metadata for `root` and store it in SQLite."""
    con = ensure_db(db_path)
    clear_root(con, root)
    print(f"Indexing files under {root} into {db_path}")
    batch: List[Tuple] = []
    total = 0
    for current_root, _, filenames in os.walk(root):
        current_dir = Path(current_root)
        for name in filenames:
            file_path = current_dir / name
            print(f"[{total + 1}] {file_path}")
            created_at, modified_at, size_bytes = stat_metadata(file_path)
            modified_by = owner(file_path)
            created_by = modified_by
            subfolder = normalize_subfolder(root, file_path)
            extension = file_path.suffix.lower().lstrip(".")
            print(
                f"    subfolder={subfolder} extension={extension or '(none)'} "
                f"size={size_bytes} created_at={created_at} modified_at={modified_at} "
                f"owner={modified_by}"
            )
            batch.append(
                (
                    str(root),
                    str(file_path),
                    file_path.name,
                    subfolder,
                    extension,
                    size_bytes,
                    created_at,
                    created_by,
                    modified_at,
                    modified_by,
                )
            )
            total += 1
            if len(batch) >= batch_size:
                insert_files(con, batch)
                batch.clear()
    if batch:
        insert_files(con, batch)
        batch.clear()
    print(f"Done! Indexed {total} files.")
    con.close()


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Index filesystem metadata into SQLite.")
    parser.add_argument(
        "--root",
        type=Path,
        help="Optional root path to index (if omitted, you will be prompted).",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB_PATH,
        help=f"SQLite database destination (default: {DEFAULT_DB_PATH}).",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> None:
    args = parse_args(argv)
    if args.root:
        root = args.root.expanduser()
        if not root.exists():
            raise SystemExit(f"Path not found: {root}")
        if not root.is_dir():
            raise SystemExit(f"Not a directory: {root}")
        root = root.resolve()
    else:
        root = prompt_for_path()
    try:
        index_path(root, args.db)
    except KeyboardInterrupt:  # pragma: no cover - interactive stop
        print("\nInterrupted. Progress saved for files processed so far.")


if __name__ == "__main__":
    main(sys.argv[1:])
