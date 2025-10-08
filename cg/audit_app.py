"""Dash UI for exploring Content Governance audit scans."""

from __future__ import annotations

import argparse
import logging
import math
from pathlib import Path
import re
from typing import List, Optional

from dash import Dash, Input, Output, State, dash_table, dcc, html, no_update
from dash.exceptions import PreventUpdate

from . import DEFAULT_DB_PATH
from . import audit_db
from .audit_db import SortInstruction
from .job_manager import JobAlreadyRunningError, JobManager

PRIMARY_GREEN = "#007C41"
SECONDARY_GREEN = "#0b3d2e"
LIGHT_GREEN = "#e1f5ea"
ACCENT = "#00a86b"
TEXT_COLOR = "#042d26"


def create_app(db_path: Path = DEFAULT_DB_PATH) -> Dash:
    database_path = audit_db.ensure_database(db_path)
    job_manager = JobManager(database_path)

    app = Dash(
        __name__,
        title="Content Governance Audit",
        suppress_callback_exceptions=True,
    )
    app.layout = build_layout(database_path)
    register_callbacks(app, job_manager, database_path)
    return app


def build_layout(db_path: Path) -> html.Div:
    db_display = html.Span(str(db_path), className="db-path")
    return html.Div(
        className="app-shell",
        children=[
            dcc.Store(id="active-run-id"),
            dcc.Interval(id="status-timer", interval=1000, n_intervals=0),
            html.Div(
                className="hero",
                children=[
                    html.H1("Content Governance Audit Explorer", className="hero__title"),
                    html.P(
                        "Scan massive file shares, persist metadata to SQLite, and audit with smart filters.",
                        className="hero__subtitle",
                    ),
                    html.Div(
                        className="hero__db",
                        children=[
                            html.Span("Database:", className="hero__db-label"),
                            db_display,
                        ],
                    ),
                ],
            ),
            html.Div(
                className="control-row",
                children=[
                    html.Div(
                        className="control-card",
                        children=[
                            html.Label("Folder to scan", htmlFor="root-input"),
                            dcc.Input(
                                id="root-input",
                                type="text",
                                placeholder=r"\\\\fileserver\\records",
                                debounce=False,
                                className="input-text",
                            ),
                            html.Div(
                                className="control-actions",
                                children=[
                                    html.Button("Start Scan", id="start-button", className="btn btn--primary"),
                                ],
                            ),
                            html.Div(id="action-feedback", className="action-feedback"),
                        ],
                    ),
                    html.Div(
                        className="control-card",
                        children=[
                            html.Label("Previous scans", htmlFor="run-dropdown"),
                            dcc.Dropdown(
                                id="run-dropdown",
                                placeholder="Select a run to explore",
                                className="run-dropdown",
                                clearable=True,
                            ),
                            html.Div(
                                className="control-hint",
                                children="New scans appear automatically once indexing starts.",
                            ),
                        ],
                    ),
                ],
            ),
            html.Div(id="status-panel", className="status-panel"),
            html.Div(
                className="filter-row",
                children=[
                    html.Div(
                        className="filter-control",
                        children=[
                            html.Label("Extensions"),
                            dcc.Dropdown(
                                id="extension-filter",
                                multi=True,
                                placeholder="All extensions",
                                className="extension-dropdown",
                            ),
                        ],
                    ),
                    html.Div(
                        className="filter-control",
                        children=[
                            html.Label("Subfolder contains"),
                            dcc.Input(
                                id="subfolder-filter",
                                type="text",
                                placeholder="e.g. finance/reports",
                                debounce=True,
                                className="input-text",
                            ),
                        ],
                    ),
                ],
            ),
            html.Div(
                className="table-card",
                children=[
                    dash_table.DataTable(
                        id="file-table",
                        columns=[
                            {"name": "File Name", "id": "file_name"},
                            {"name": "Subfolder", "id": "subfolder"},
                            {"name": "Created At", "id": "created_at"},
                            {"name": "Modified At", "id": "modified_at"},
                            {"name": "Modified By", "id": "modified_by"},
                            {"name": "Extension", "id": "extension"},
                            {"name": "Path", "id": "path", "hideable": True},
                        ],
                        data=[],
                        page_action="custom",
                        page_current=0,
                        page_size=50,
                        sort_action="custom",
                        sort_mode="multi",
                        sort_by=[],
                        style_as_list_view=True,
                        style_table={
                            "border": "none",
                            "boxShadow": "0 10px 24px rgba(0, 124, 65, 0.1)",
                            "borderRadius": "14px",
                            "overflowX": "auto",
                        },
                        style_header={
                            "backgroundColor": PRIMARY_GREEN,
                            "color": "white",
                            "fontWeight": "600",
                            "border": "none",
                        },
                        style_cell={
                            "padding": "12px 14px",
                            "border": "none",
                            "whiteSpace": "normal",
                            "height": "auto",
                        },
                        style_data_conditional=[
                            {
                                "if": {"row_index": "odd"},
                                "backgroundColor": "rgba(0, 124, 65, 0.04)",
                            },
                        ],
                    ),
                    html.Div(id="table-count", className="table-count"),
                ],
            ),
        ],
    )


def register_callbacks(app: Dash, job_manager: JobManager, db_path: Path) -> None:
    @app.callback(
        Output("action-feedback", "children"),
        Output("active-run-id", "data"),
        Input("start-button", "n_clicks"),
        State("root-input", "value"),
        prevent_initial_call=True,
    )
    def handle_start(n_clicks: int, root_value: Optional[str]):
        if not root_value:
            raise PreventUpdate
        app.logger.info("Start button clicked with value=%r", root_value)
        try:
            root_path = _normalize_root_input(root_value)
        except ValueError as exc:
            app.logger.warning("Start aborted: %s", exc)
            return (html.Span(str(exc), className="feedback feedback--error"), no_update)

        if not root_path.exists():
            app.logger.warning("Start aborted: path not found %s", root_path)
            return (
                html.Span(f"Path not found: {root_path}", className="feedback feedback--error"),
                no_update,
            )
        if not root_path.is_dir():
            app.logger.warning("Start aborted: not a directory %s", root_path)
            return (
                html.Span(f"Not a directory: {root_path}", className="feedback feedback--error"),
                no_update,
            )
        try:
            job = job_manager.start(str(root_path))
        except JobAlreadyRunningError as exc:
            app.logger.info("Start rejected: %s", exc)
            return (html.Span(str(exc), className="feedback feedback--warn"), no_update)
        except Exception as exc:  # pragma: no cover - defensive
            app.logger.exception("Unable to start scan for %s", root_path)
            return (
                html.Span(f"Unable to start scan: {exc}", className="feedback feedback--error"),
                no_update,
            )
        app.logger.info("Queued run_id=%s for %s", job.run_id, job.root_path)
        return (
            html.Span(
                f"Scan queued (run {job.run_id}) for {Path(job.root_path)}",
                className="feedback feedback--ok",
            ),
            job.run_id,
        )

    @app.callback(
        Output("active-run-id", "data"),
        Input("run-dropdown", "value"),
        State("active-run-id", "data"),
        prevent_initial_call=True,
    )
    def sync_active_run(selected_run: Optional[int], current: Optional[int]) -> Optional[int]:
        if selected_run is None:
            return current
        return selected_run

    @app.callback(
        Output("run-dropdown", "value"),
        Input("active-run-id", "data"),
    )
    def update_dropdown_value(active_run_id: Optional[int]) -> Optional[int]:
        return active_run_id

    @app.callback(
        Output("status-panel", "children"),
        Output("run-dropdown", "options"),
        Input("status-timer", "n_intervals"),
        Input("start-button", "n_clicks"),
        State("active-run-id", "data"),
    )
    def refresh_status(_: int, __: Optional[int], active_run_id: Optional[int]):
        job_status = job_manager.get_status()
        runs = audit_db.list_runs(db_path)
        app.logger.debug(
            "refresh_status active_run=%s job_status=%s runs=%s",
            active_run_id,
            job_status.status if job_status else None,
            len(runs),
        )
        options = [
            {
                "label": _format_run_option(run),
                "value": run["run_id"],
            }
            for run in runs
        ]

        status_children: List[html.Div] = []
        if job_status:
            status_children.append(_build_job_status(job_status))
        if active_run_id:
            run_summary = next((run for run in runs if run["run_id"] == active_run_id), None)
            if run_summary:
                status_children.append(_build_run_summary(run_summary))

        if not status_children:
            status_children = [html.Div("No scans yet. Start by indexing a folder.", className="status-empty")]

        return status_children, options

    @app.callback(
        Output("extension-filter", "options"),
        Output("extension-filter", "value"),
        Input("active-run-id", "data"),
    )
    def refresh_extensions(active_run_id: Optional[int]):
        if not active_run_id:
            return [], []
        extensions = audit_db.fetch_extensions(active_run_id, db_path)
        options = [
            {"label": ext if ext else "(none)", "value": ext if ext else ""}
            for ext in extensions
        ]
        return options, []

    @app.callback(
        Output("file-table", "data"),
        Output("file-table", "page_count"),
        Output("table-count", "children"),
        Input("file-table", "page_current"),
        Input("file-table", "page_size"),
        Input("file-table", "sort_by"),
        Input("extension-filter", "value"),
        Input("subfolder-filter", "value"),
        Input("active-run-id", "data"),
    )
    def update_table(
        page_current: int,
        page_size: int,
        sort_by: List[dict],
        extensions: Optional[List[str]],
        subfolder_value: Optional[str],
        active_run_id: Optional[int],
    ):
        if not active_run_id:
            return [], 0, "No run selected."
        sort_instructions = [
            SortInstruction(column_id=item["column_id"], direction=item.get("direction", "asc"))
            for item in (sort_by or [])
        ]
        rows, total = audit_db.fetch_page(
            active_run_id,
            page_current,
            page_size,
            sort_instructions,
            extensions=extensions or None,
            subfolder_contains=subfolder_value or None,
            db_path=db_path,
        )
        total_display = f"{total:,} files indexed"
        page_count = max(math.ceil(total / page_size), 1) if total else 0
        return rows, page_count, total_display

    @app.callback(
        Output("start-button", "disabled"),
        Input("status-timer", "n_intervals"),
    )
    def toggle_start_disabled(_: int) -> bool:
        return job_manager.has_active_job()


def _format_run_option(run: dict) -> str:
    root = Path(run["root_path"])
    status = run["status"]
    file_count = run.get("total_files") or 0
    return f"{root.name or root} · {status} · {file_count:,} files"


def _build_job_status(job_status) -> html.Div:
    message = job_status.message
    processed = f"{job_status.processed_files:,}"
    current = job_status.current_path or ""
    status = (job_status.status or "").lower()
    status_class = "status-card"
    if status == "running":
        status_class += " status-card--running"
    elif status == "completed":
        status_class += " status-card--ok"
    elif status == "failed":
        status_class += " status-card--error"
    elif status == "queued":
        status_class += " status-card--queued"
    return html.Div(
        className=status_class,
        children=[
            html.H3("Active Scan"),
            html.P(f"Root: {job_status.root_path}", className="status-path"),
            html.P(f"Status: {job_status.status}"),
            html.P(message),
            html.P(f"Processed files: {processed}"),
            html.P(f"Errors: {job_status.error_count}"),
            html.P(current, className="status-path"),
        ],
    )


def _build_run_summary(run: dict) -> html.Div:
    totals = f"{(run.get('total_files') or 0):,}"
    errors = run.get("total_errors") or 0
    status = run.get("status") or "unknown"
    started = run.get("started_at") or run.get("queued_at") or ""
    duration = run.get("duration_seconds")
    duration_text = f"{duration:.1f}s" if duration else "—"
    return html.Div(
        className="status-card",
        children=[
            html.H3("Selected Run"),
            html.P(f"Status: {status}"),
            html.P(f"Files indexed: {totals}"),
            html.P(f"Errors: {errors}"),
            html.P(f"Started: {started}"),
            html.P(f"Duration: {duration_text}"),
        ],
    )


_DRIVE_PATTERN = re.compile(r"^[A-Za-z]:$")


def _normalize_root_input(raw_value: str) -> Path:
    cleaned = raw_value.strip().strip('"').strip()
    if not cleaned:
        raise ValueError("Folder path cannot be empty.")
    # Handle drive-letter inputs like "C:" by normalizing to "C:\"
    if _DRIVE_PATTERN.match(cleaned):
        cleaned += "\\"
    # Translate UNC paths that might use forward slashes in user input
    if cleaned.startswith("//"):
        cleaned = "\\" + cleaned.lstrip("/")
    return Path(cleaned).expanduser().resolve(strict=False)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Content Governance audit dashboard.")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH, help="SQLite database path.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8050)
    parser.add_argument("--debug", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    app = create_app(args.db)
    app.logger.setLevel(logging.INFO)
    run_callable = getattr(app, "run", None)
    if callable(run_callable):
        run_callable(host=args.host, port=args.port, debug=args.debug)
    else:  # Dash < 3.0 fallback
        app.run_server(host=args.host, port=args.port, debug=args.debug)


if __name__ == "__main__":
    main()
