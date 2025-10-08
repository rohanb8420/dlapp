"""Content Governance (cg) package providing audit indexing utilities and Dash UI."""

from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parent
DEFAULT_DB_PATH = PACKAGE_ROOT / "data" / "audit_index.db"

__all__ = ["PACKAGE_ROOT", "DEFAULT_DB_PATH"]
