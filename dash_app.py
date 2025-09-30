"""Dash web app for browsing the document metadata SQLite database."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import List, Dict, Any, Tuple

from dash import Dash, Input, Output, State, dcc, html
from dash.dash_table import DataTable

from reader import ensure_db, fetch_file_detail, fetch_summary_rows


def build_layout(db_path: Path) -> html.Div:
    """Create the Dash layout hierarchy."""
    return html.Div(
        className="app-container",
        children=[
            html.H1("Document Metadata Dashboard"),
            html.Div(
                className="controls",
                children=[
                    html.Span("Database:", className="label"),
                    html.Code(str(db_path.resolve()), className="db-path"),
                    html.Button("Reload", id="reload-button", n_clicks=0, className="reload"),
                    dcc.Store(id="db-store", data=str(db_path.resolve())),
                ],
            ),
            html.H2("Files"),
            DataTable(
                id="file-table",
                columns=[
                    {"name": "ID", "id": "file_id"},
                    {"name": "Name", "id": "file_name"},
                    {"name": "Folder", "id": "folder"},
                    {"name": "Extension", "id": "extension"},
                    {"name": "Business Category", "id": "business_category"},
                    {"name": "Modified", "id": "modified_ts"},
                    {"name": "Size (bytes)", "id": "size_bytes"},
                ],
                data=[],
                page_size=15,
                sort_action="native",
                filter_action="native",
                row_selectable="single",
                selected_row_ids=[],
                style_table={"overflowX": "auto"},
                style_cell={"textAlign": "left", "minWidth": "120px", "whiteSpace": "normal"},
            ),
            html.Div(id="detail-section", className="detail", children=[
                html.H2("Details"),
                html.Div(id="file-detail", className="metadata"),
                html.H3("Content Preview"),
                dcc.Textarea(
                    id="content-preview",
                    value="",
                    readOnly=True,
                    style={"width": "100%", "height": "300px"},
                ),
            ]),
        ],
    )


def create_dash_app(db_path: Path) -> Dash:
    ensure_db(db_path)
    app = Dash(__name__)
    app.title = "DLM Reader Dashboard"
    app.layout = build_layout(db_path)

    @app.callback(
        Output("file-table", "data"),
        Output("file-table", "selected_row_ids"),
        Input("reload-button", "n_clicks"),
        State("db-store", "data"),
        prevent_initial_call=False,
    )
    def refresh_table(_: int, db_location: str) -> Tuple[List[Dict[str, Any]], List[int]]:
        db = Path(db_location)
        rows = fetch_summary_rows(db)
        # Ensure numeric columns serialize cleanly
        for row in rows:
            if row.get("size_bytes") is not None:
                row["size_bytes"] = int(row["size_bytes"])
        selected = [rows[0]["file_id"]] if rows else []
        return rows, selected

    @app.callback(
        Output("file-detail", "children"),
        Output("content-preview", "value"),
        Input("file-table", "selected_row_ids"),
        State("file-table", "data"),
        State("db-store", "data"),
        prevent_initial_call=False,
    )
    def populate_detail(selected_ids: List[int], table_data: List[Dict[str, Any]], db_location: str) -> Tuple[Any, str]:
        if not selected_ids:
            return html.Div("Select a file to view details."), ""
        file_id = int(selected_ids[0])
        detail = fetch_file_detail(Path(db_location), file_id)
        if not detail:
            return html.Div("No detail found for the selected file."), ""
        file_meta = detail["file"]
        labels = detail.get("labels", [])
        metadata_items = [
            html.Li(html.Span([html.Strong("Path:"), f" {file_meta['path']}"])),
            html.Li(html.Span([html.Strong("Mime Type:"), f" {file_meta.get('mime_type', '')}"])),
            html.Li(html.Span([html.Strong("Exists:"), f" {bool(file_meta.get('exists_flag'))}"])),
            html.Li(html.Span([html.Strong("SHA1:"), f" {file_meta.get('sha1') or '(none)'}"])),
            html.Li(html.Span([html.Strong("Business Categories:"), f" {', '.join(labels) if labels else '(none)'}"])),
        ]
        return html.Ul(metadata_items, className="meta-list"), detail.get("content", "")[:5000]

    return app


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Dash viewer for the document database.")
    parser.add_argument(
        "--db",
        type=Path,
        default=Path("artifacts") / "dlm_reader.db",
        help="Path to the SQLite database populated by reader.py",
    )
    parser.add_argument("--host", default="127.0.0.1", help="Hostname/interface to bind the Dash server.")
    parser.add_argument("--port", type=int, default=8050, help="Port for the Dash server.")
    parser.add_argument("--debug", action="store_true", help="Run Dash with debug=True for auto-reload.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    app = create_dash_app(args.db)
    app.run(host=args.host, port=args.port, debug=args.debug)


if __name__ == "__main__":
    main()
