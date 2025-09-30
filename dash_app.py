"""Dash web app for browsing the document metadata SQLite database."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Dict, List, Tuple

from dash import Dash, Input, Output, State, dcc, html
from dash.dash_table import DataTable

from reader import ensure_db, fetch_file_detail, fetch_summary_rows


PRIMARY_GREEN = "#007C41"  # TD green
SECONDARY_GREEN = "#0b3d2e"
LIGHT_GREEN = "#e1f5ea"
ACCENT = "#00a86b"
TEXT_COLOR = "#042d26"
CARD_BG = "#f7fffb"


def build_layout(db_path: Path) -> html.Div:
    """Create the Dash layout hierarchy."""
    return html.Div(
        style={
            "minHeight": "100vh",
            "padding": "32px",
            "background": f"linear-gradient(135deg, {LIGHT_GREEN} 0%, #ffffff 60%)",
            "fontFamily": "'Segoe UI', sans-serif",
            "color": TEXT_COLOR,
        },
        children=[
            html.Header(
                style={
                    "display": "flex",
                    "alignItems": "center",
                    "gap": "16px",
                    "marginBottom": "24px",
                },
                children=[
                    html.Div(
                        "DLM Document Viewer",
                        style={
                            "fontSize": "32px",
                            "fontWeight": "600",
                            "color": PRIMARY_GREEN,
                        },
                    ),
                    html.Div(
                        "Browse ingested files, metadata, and extracted text at a glance.",
                        style={"opacity": 0.8},
                    ),
                ],
            ),
            html.Div(
                style={
                    "display": "flex",
                    "alignItems": "center",
                    "gap": "12px",
                    "marginBottom": "24px",
                    "padding": "12px 18px",
                    "borderRadius": "10px",
                    "backgroundColor": CARD_BG,
                    "boxShadow": "0 6px 18px rgba(0, 124, 65, 0.08)",
                },
                children=[
                    html.Span("Database", style={"fontWeight": "600"}),
                    html.Code(
                        str(db_path.resolve()),
                        style={
                            "padding": "4px 8px",
                            "borderRadius": "6px",
                            "backgroundColor": "#ffffff",
                            "border": f"1px solid {LIGHT_GREEN}",
                        },
                    ),
                    html.Button(
                        "Reload",
                        id="reload-button",
                        n_clicks=0,
                        style={
                            "marginLeft": "auto",
                            "padding": "8px 16px",
                            "borderRadius": "20px",
                            "border": "none",
                            "backgroundColor": PRIMARY_GREEN,
                            "color": "white",
                            "fontWeight": "600",
                            "cursor": "pointer",
                            "boxShadow": "0 4px 12px rgba(0, 124, 65, 0.3)",
                        },
                    ),
                    dcc.Store(id="db-store", data=str(db_path.resolve())),
                ],
            ),
            html.H2(
                "Files",
                style={"color": SECONDARY_GREEN, "marginBottom": "16px", "fontWeight": "600"},
            ),
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
                row_selectable="single",
                selected_rows=[],
                page_size=15,
                sort_action="native",
                filter_action="native",
                style_as_list_view=True,
                style_table={
                    "overflowX": "auto",
                    "border": "none",
                    "boxShadow": "0 10px 24px rgba(0, 124, 65, 0.1)",
                    "borderRadius": "12px",
                },
                style_header={
                    "backgroundColor": PRIMARY_GREEN,
                    "color": "white",
                    "fontWeight": "600",
                    "border": "none",
                },
                style_data={
                    "border": "none",
                    "padding": "10px",
                    "whiteSpace": "normal",
                },
                style_data_conditional=[
                    {
                        "if": {"state": "selected"},
                        "backgroundColor": ACCENT,
                        "color": "white",
                    },
                    {
                        "if": {"row_index": "odd"},
                        "backgroundColor": "rgba(0, 124, 65, 0.03)",
                    },
                ],
            ),
            html.Div(
                id="detail-section",
                style={
                    "display": "grid",
                    "gridTemplateColumns": "repeat(auto-fit, minmax(320px, 1fr))",
                    "gap": "24px",
                    "marginTop": "32px",
                },
                children=[
                    html.Div(
                        children=[
                            html.H2(
                                "Details",
                                style={
                                    "color": SECONDARY_GREEN,
                                    "fontWeight": "600",
                                    "marginBottom": "12px",
                                },
                            ),
                            html.Div(
                                id="file-detail",
                                style={
                                    "padding": "18px",
                                    "borderRadius": "12px",
                                    "backgroundColor": CARD_BG,
                                    "boxShadow": "0 6px 20px rgba(0, 124, 65, 0.12)",
                                    "lineHeight": 1.6,
                                },
                            ),
                        ]
                    ),
                    html.Div(
                        children=[
                            html.H2(
                                "Content Preview",
                                style={
                                    "color": SECONDARY_GREEN,
                                    "fontWeight": "600",
                                    "marginBottom": "12px",
                                },
                            ),
                            dcc.Textarea(
                                id="content-preview",
                                value="",
                                readOnly=True,
                                style={
                                    "width": "100%",
                                    "height": "360px",
                                    "padding": "16px",
                                    "borderRadius": "12px",
                                    "border": f"1px solid {LIGHT_GREEN}",
                                    "backgroundColor": "#ffffff",
                                    "boxShadow": "inset 0 4px 12px rgba(0, 0, 0, 0.04)",
                                    "fontFamily": "'Consolas', 'Courier New', monospace",
                                    "fontSize": "14px",
                                },
                            ),
                        ]
                    ),
                ],
            ),
        ],
    )


def create_dash_app(db_path: Path) -> Dash:
    ensure_db(db_path)
    app = Dash(__name__)
    app.title = "DLM Reader Dashboard"
    app.layout = build_layout(db_path)

    @app.callback(
        Output("file-table", "data"),
        Output("file-table", "selected_rows"),
        Input("reload-button", "n_clicks"),
        State("db-store", "data"),
        prevent_initial_call=False,
    )
    def refresh_table(_: int, db_location: str) -> Tuple[List[Dict[str, Any]], List[int]]:
        db = Path(db_location)
        rows = fetch_summary_rows(db)
        for row in rows:
            if row.get("size_bytes") is not None:
                row["size_bytes"] = int(row["size_bytes"])
        selected = [0] if rows else []
        return rows, selected

    @app.callback(
        Output("file-detail", "children"),
        Output("content-preview", "value"),
        Input("file-table", "selected_rows"),
        State("file-table", "data"),
        State("db-store", "data"),
        prevent_initial_call=False,
    )
    def populate_detail(
        selected_rows: List[int],
        table_data: List[Dict[str, Any]],
        db_location: str,
    ) -> Tuple[Any, str]:
        if not table_data:
            return html.Div("No files available. Try reloading."), ""
        if not selected_rows:
            return html.Div("Select a file in the table to view metadata."), ""

        row_index = selected_rows[0]
        row = table_data[row_index]
        file_id = int(row["file_id"])

        detail = fetch_file_detail(Path(db_location), file_id)
        if not detail:
            return html.Div("No detail found for the selected file."), ""

        file_meta = detail["file"]
        labels = detail.get("labels") or []

        metadata_items = [
            html.Div(
                [html.Strong("Path"), html.Span(file_meta["path"], style={"marginLeft": "8px"})]
            ),
            html.Div(
                [
                    html.Strong("Mime Type"),
                    html.Span(file_meta.get("mime_type") or "–", style={"marginLeft": "8px"}),
                ]
            ),
            html.Div(
                [
                    html.Strong("Exists"),
                    html.Span(
                        "Yes" if file_meta.get("exists_flag") else "No",
                        style={"marginLeft": "8px"},
                    ),
                ]
            ),
            html.Div(
                [
                    html.Strong("SHA1"),
                    html.Span(file_meta.get("sha1") or "(not computed)", style={"marginLeft": "8px"}),
                ]
            ),
            html.Div(
                [
                    html.Strong("Business Categories"),
                    html.Span(
                        ", ".join(labels) if labels else "(none)",
                        style={"marginLeft": "8px"},
                    ),
                ]
            ),
        ]

        meta_card = html.Div(
            metadata_items,
            style={
                "display": "grid",
                "rowGap": "10px",
            },
        )

        content_preview = (detail.get("content") or "")[:5000]
        return meta_card, content_preview

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
