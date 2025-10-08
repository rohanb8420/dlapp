"""Dash UI for browsing the indexed file catalog database."""

from __future__ import annotations

import argparse
import math
from pathlib import Path
from typing import List, Optional

from dash import Dash, Input, Output, State, dash_table, dcc, html, no_update

from . import DEFAULT_DB_PATH
from .catalog_db import (
    count_files,
    ensure_db,
    fetch_files,
    list_extensions,
    list_roots,
    list_subfolders,
)


COLUMNS = [
    {"name": "File Name", "id": "file_name"},
    {"name": "Subfolder", "id": "subfolder"},
    {"name": "Extension", "id": "extension"},
    {"name": "Size (bytes)", "id": "size_bytes"},
    {"name": "Created At", "id": "created_at"},
    {"name": "Created By", "id": "created_by"},
    {"name": "Modified At", "id": "modified_at"},
    {"name": "Modified By", "id": "modified_by"},
    {"name": "Full Path", "id": "path"},
]


def create_app(db_path: Path = DEFAULT_DB_PATH) -> Dash:
    con = ensure_db(db_path)
    con.close()

    app = Dash(__name__, title="File Catalog Browser")
    app.layout = build_layout(db_path)
    register_callbacks(app, db_path)
    return app


def build_layout(db_path: Path) -> html.Div:
    return html.Div(
        className="catalog-app",
        children=[
            dcc.Store(id="current-root"),
            dcc.Store(id="current-filters"),
            html.H1("File Catalog Browser"),
            html.P(
                f"Database: {db_path}",
                className="db-label",
            ),
            html.Div(
                className="controls",
                children=[
                    html.Div(
                        className="control",
                        children=[
                            html.Label("Indexed Roots"),
                            dcc.Dropdown(id="root-dropdown", placeholder="Select an indexed root"),
                        ],
                    ),
                    html.Div(
                        className="control",
                        children=[
                            html.Label("Extensions"),
                            dcc.Dropdown(
                                id="extension-dropdown",
                                placeholder="All extensions",
                                multi=True,
                            ),
                        ],
                    ),
                    html.Div(
                        className="control",
                        children=[
                            html.Label("Subfolder contains"),
                            dcc.Input(
                                id="subfolder-input",
                                type="text",
                                placeholder="e.g. finance/reports",
                                debounce=True,
                            ),
                        ],
                    ),
                ],
            ),
            html.Div(id="summary", className="summary"),
            dash_table.DataTable(
                id="file-table",
                columns=COLUMNS,
                data=[],
                page_action="custom",
                page_current=0,
                page_size=50,
                sort_action="custom",
                sort_mode="single",
                sort_by=[],
                filter_action="none",
                style_table={
                    "marginTop": "18px",
                    "border": "none",
                    "boxShadow": "0 10px 24px rgba(0,0,0,0.08)",
                },
                style_header={
                    "backgroundColor": "#0b3d2e",
                    "color": "white",
                    "fontWeight": "600",
                },
                style_cell={
                    "padding": "12px",
                    "border": "none",
                    "whiteSpace": "normal",
                    "height": "auto",
                },
            ),
        ],
    )


def register_callbacks(app: Dash, db_path: Path) -> None:
    @app.callback(
        Output("root-dropdown", "options"),
        Output("root-dropdown", "value"),
        Input("root-dropdown", "id"),
    )
    def init_roots(_: str):
        con = ensure_db(db_path)
        try:
            roots = list_roots(con)
        finally:
            con.close()
        if not roots:
            return [], None
        options = [{"label": root, "value": root} for root in roots]
        return options, roots[0]

    @app.callback(
        Output("extension-dropdown", "options"),
        Output("extension-dropdown", "value"),
        Output("subfolder-input", "value"),
        Input("root-dropdown", "value"),
    )
    def refresh_filters(root_value: Optional[str]):
        if not root_value:
            return [], [], ""
        con = ensure_db(db_path)
        try:
            extensions = list_extensions(con, root_value)
        finally:
            con.close()
        options = [{"label": ext or "(none)", "value": ext} for ext in extensions]
        return options, [], ""

    @app.callback(
        Output("summary", "children"),
        Output("file-table", "data"),
        Output("file-table", "page_count"),
        Input("file-table", "page_current"),
        Input("file-table", "page_size"),
        Input("file-table", "sort_by"),
        Input("extension-dropdown", "value"),
        Input("subfolder-input", "value"),
        Input("root-dropdown", "value"),
    )
    def update_table(
        page_current: int,
        page_size: int,
        sort_by: List[dict],
        extensions: Optional[List[str]],
        subfolder_value: Optional[str],
        root_value: Optional[str],
    ):
        if not root_value:
            raise no_update
        sort_column = "subfolder"
        sort_direction = "ASC"
        if sort_by:
            sort_column = sort_by[0].get("column_id", "subfolder")
            sort_direction = sort_by[0].get("direction", "asc").upper()
        con = ensure_db(db_path)
        try:
            total = count_files(
                con,
                root_value,
                extensions=extensions or None,
                subfolder_like=subfolder_value or None,
            )
            rows = fetch_files(
                con,
                root_value,
                page_current,
                page_size,
                extensions=extensions or None,
                subfolder_like=subfolder_value or None,
                sort_column=sort_column,
                sort_direction=sort_direction,
            )
        finally:
            con.close()
        page_count = max(math.ceil(total / page_size), 1) if total else 0
        summary = f"{total:,} files indexed under {root_value}"
        if extensions:
            summary += f" | extensions: {', '.join(extensions)}"
        if subfolder_value:
            summary += f" | subfolder filter: {subfolder_value}"
        return summary, rows, page_count


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Launch the file catalog Dash browser.")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH, help="SQLite database path.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8050)
    parser.add_argument("--debug", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    app = create_app(args.db)
    app.run(host=args.host, port=args.port, debug=args.debug)


if __name__ == "__main__":
    main()
