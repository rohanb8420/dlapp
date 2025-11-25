
"""Dash dashboard for data owners to review retention/disposition and run the model."""

from __future__ import annotations

import argparse
import hashlib
import itertools
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Tuple

from dash import Dash, Input, Output, State, callback_context, dcc, html, no_update
from dash.dash_table import DataTable


@dataclass(frozen=True)
class RetentionRule:
    code: str
    trigger: str
    period_years: int
    rationale: str


# TD-inspired palette for light/dark themes.
THEMES: Dict[str, Dict[str, str]] = {
    "light": {
        "bg": "#f4fbf7",
        "panel": "#ffffff",
        "muted": "#5c7c6b",
        "text": "#0b3d2e",
        "primary": "#007c41",
        "accent": "#00a86b",
        "warning": "#f0a202",
        "danger": "#d52b1e",
        "success": "#0b6b3a",
        "shadow": "0 14px 30px rgba(0, 124, 65, 0.14)",
        "soft-danger": "#ffebee",
        "soft-warning": "#fff7e0",
        "soft-success": "#e3f2ed",
    },
    "dark": {
        "bg": "#071c17",
        "panel": "#0f2b23",
        "muted": "#9bbcaf",
        "text": "#e7f4ee",
        "primary": "#43d487",
        "accent": "#7efcb4",
        "warning": "#f7c266",
        "danger": "#ff7b7b",
        "success": "#58f0a0",
        "shadow": "0 18px 38px rgba(0, 0, 0, 0.45)",
        "soft-danger": "#2a1217",
        "soft-warning": "#2c2110",
        "soft-success": "#10271f",
    },
}


RETENTION_RULES: Dict[str, RetentionRule] = {
    "Finance & Accounting": RetentionRule(
        code="FIN-07",
        trigger="Fiscal year close",
        period_years=7,
        rationale="Audit, tax and statutory reporting needs.",
    ),
    "Human Capital": RetentionRule(
        code="HCM-06",
        trigger="Employee termination",
        period_years=6,
        rationale="Supports employee lookbacks and compliance inquiries.",
    ),
    "Sales & Client Delivery": RetentionRule(
        code="SAL-05",
        trigger="Contract expiration",
        period_years=5,
        rationale="Preserve commercial records through agreement lifecycle.",
    ),
    "Risk & Compliance": RetentionRule(
        code="RCM-10",
        trigger="Case completion",
        period_years=10,
        rationale="Evidence for regulatory audits or investigations.",
    ),
    "Technology & Data": RetentionRule(
        code="TEC-03",
        trigger="System decommission",
        period_years=3,
        rationale="Operational copies only needed while platform is active.",
    ),
    "Operations & Shared Services": RetentionRule(
        code="OPS-04",
        trigger="Process completion",
        period_years=4,
        rationale="Default operational retention guidance.",
    ),
}
DEFAULT_RULE = RETENTION_RULES["Operations & Shared Services"]

CAPABILITY_KEYWORDS: List[Tuple[Iterable[str], str]] = [
    (("invoice", "ledger", "expense", "payable"), "Finance & Accounting"),
    (("payroll", "benefit", "performance review"), "Human Capital"),
    (("client", "prospect", "campaign", "sales"), "Sales & Client Delivery"),
    (("control", "compliance", "policy", "audit"), "Risk & Compliance"),
    (("data model", "api", "architecture", "sql"), "Technology & Data"),
    (("sop", "ops", "workflow", "procedure"), "Operations & Shared Services"),
]

EMPLOYEE_DIRECTORY: List[Tuple[str, str]] = [
    ("Alexandra Cho", "7012456"),
    ("Liam Patterson", "7159988"),
    ("Morgan Patel", "7223145"),
    ("Priya Singh", "7056712"),
    ("Daniel Romero", "7094488"),
    ("Sofia Ibrahim", "7312455"),
    ("Noah Tremblay", "7123499"),
    ("Emily Laurent", "7045981"),
    ("Brian O'Keefe", "7180031"),
    ("Chloe Martins", "7078841"),
]

STATUS_ORDER = ["Purge Recommended", "Due Next 12 Months", "Within Retention"]
MAX_MODEL_FILES = 180


def add_years(dt: datetime, years: int) -> datetime:
    """Return dt advanced by `years`, handling leap dates."""
    try:
        return dt.replace(year=dt.year + years)
    except ValueError:
        return dt.replace(month=3, day=1, year=dt.year + years)


def infer_capability(text: str) -> str:
    haystack = text.lower()
    for keywords, capability in CAPABILITY_KEYWORDS:
        if any(term in haystack for term in keywords):
            return capability
    return "Operations & Shared Services"


def choose_rule(capability: str) -> RetentionRule:
    return RETENTION_RULES.get(capability, DEFAULT_RULE)


def assign_owner(path_str: str) -> Tuple[str, str]:
    hashed = int(hashlib.sha1(path_str.encode("utf-8")).hexdigest(), 16)
    idx = hashed % len(EMPLOYEE_DIRECTORY)
    return EMPLOYEE_DIRECTORY[idx]


def categorize(purge_year: int, today: datetime | None = None) -> str:
    today = today or datetime.now()
    current_year = today.year
    if purge_year <= current_year:
        return "Purge Recommended"
    if purge_year == current_year + 1:
        return "Due Next 12 Months"
    return "Within Retention"


def build_record(
    file_name: str,
    location: str,
    created: datetime,
    capability: str | None = None,
    rule: RetentionRule | None = None,
) -> Dict[str, Any]:
    capability = capability or infer_capability(f"{location} {file_name}")
    rule = rule or choose_rule(capability)
    owner, employee_id = assign_owner(location)
    purge_year = add_years(created, rule.period_years).year
    return {
        "file_name": file_name,
        "location": location,
        "capability": capability,
        "retention_code": rule.code,
        "retention_trigger": rule.trigger,
        "retention_period": f"{rule.period_years} years",
        "rationale": rule.rationale,
        "creator": owner,
        "employee_id": employee_id,
        "created_date": created.strftime("%Y-%m-%d"),
        "recommended_purge_year": purge_year,
        "status": categorize(purge_year),
    }


def seed_records() -> List[Dict[str, Any]]:
    """Provide default rows for the overview when no scan has run yet."""
    now = datetime.now()
    examples = [
        (
            "Finance_P&L_Q3.xlsx",
            r"\\SharedDrive\\Finance\\2024\\Quarterly",
            now.replace(year=now.year - 6, month=2, day=10),
            "Finance & Accounting",
        ),
        (
            "Termination_Checklist.docx",
            r"\\SharedDrive\\HR\\Offboarding",
            now.replace(year=now.year - 5, month=11, day=2),
            "Human Capital",
        ),
        (
            "API_contract_notes.txt",
            r"\\SharedDrive\\Technology\\Platform Team",
            now.replace(year=now.year - 1, month=6, day=18),
            "Technology & Data",
        ),
        (
            "Client_Statement_2020.pdf",
            r"\\SharedDrive\\ClientDelivery\\Statements",
            now.replace(year=now.year - 8, month=9, day=5),
            "Sales & Client Delivery",
        ),
        (
            "Audit_controls_matrix.xlsx",
            r"\\SharedDrive\\Risk\\SOX",
            now.replace(year=now.year - 9, month=1, day=24),
            "Risk & Compliance",
        ),
        (
            "Ops_Playbook_v2.pptx",
            r"\\SharedDrive\\Operations\\Playbooks",
            now.replace(year=now.year - 3, month=4, day=13),
            "Operations & Shared Services",
        ),
    ]
    return [build_record(name, path, created, capability=cap) for name, path, created, cap in examples]


def build_queue_from_folder(folder: Path, limit: int = MAX_MODEL_FILES) -> List[Dict[str, Any]]:
    """Scan a folder and return records ready for model enrichment."""
    files_iter = (path for path in folder.rglob("*") if path.is_file())
    queue: List[Dict[str, Any]] = []
    for path in itertools.islice(files_iter, limit):
        try:
            stat = path.stat()
            created = datetime.fromtimestamp(stat.st_mtime)
        except OSError:
            continue
        capability = infer_capability(f"{path.parent} {path.name}")
        record = build_record(
            file_name=path.name,
            location=str(path),
            created=created,
            capability=capability,
            rule=choose_rule(capability),
        )
        queue.append(record)
    return queue

def build_layout() -> html.Div:
    seed = seed_records()
    return html.Div(
        id="theme-root",
        className="app-shell theme-light",
        children=[
            dcc.Store(id="records-store", data=seed),
            dcc.Store(id="inference-queue", data=[]),
            dcc.Store(id="inference-meta", data={"total": 0, "processed": 0, "active": False, "root": ""}),
            dcc.Store(id="recent-store", data=[]),
            dcc.Store(id="theme-store", data="light"),
            dcc.Interval(id="inference-interval", interval=900, disabled=True),
            html.Header(
                style={
                    "display": "flex",
                    "alignItems": "center",
                    "gap": "14px",
                    "marginBottom": "18px",
                },
                children=[
                    html.Div(
                        style={
                            "width": "12px",
                            "height": "48px",
                            "background": "linear-gradient(180deg, var(--primary), var(--accent))",
                            "borderRadius": "6px",
                        }
                    ),
                    html.Div(
                        children=[
                            html.Div(
                                "Data Retention & Disposition",
                                style={"fontSize": "28px", "fontWeight": 700, "lineHeight": 1.1},
                            ),
                            html.Div(
                                "See what can be purged, what's next, and keep the audit trail tight.",
                                style={"color": "var(--muted)"},
                            ),
                        ],
                    ),
                    html.Button(
                        "Switch to Dark",
                        id="theme-toggle",
                        n_clicks=0,
                        style={
                            "marginLeft": "auto",
                            "padding": "10px 16px",
                            "borderRadius": "12px",
                            "border": "none",
                            "background": "var(--panel)",
                            "color": "var(--text)",
                            "boxShadow": "var(--shadow)",
                            "cursor": "pointer",
                            "fontWeight": 700,
                        },
                    ),
                ],
            ),
            dcc.Tabs(
                id="app-tabs",
                value="overview",
                style={"backgroundColor": "transparent"},
                children=[
                    dcc.Tab(
                        label="Overview",
                        value="overview",
                        children=[build_overview_tab()],
                        style={"padding": "10px 14px", "border": "none", "background": "transparent"},
                        selected_style={
                            "padding": "10px 14px",
                            "border": "none",
                            "background": "var(--panel)",
                            "boxShadow": "var(--shadow)",
                            "borderRadius": "10px 10px 0 0",
                            "color": "var(--primary)",
                            "fontWeight": 700,
                        },
                    ),
                    dcc.Tab(
                        label="Model Runner",
                        value="runner",
                        children=[build_runner_tab()],
                        style={"padding": "10px 14px", "border": "none", "background": "transparent"},
                        selected_style={
                            "padding": "10px 14px",
                            "border": "none",
                            "background": "var(--panel)",
                            "boxShadow": "var(--shadow)",
                            "borderRadius": "10px 10px 0 0",
                            "color": "var(--primary)",
                            "fontWeight": 700,
                        },
                    ),
                ],
            ),
        ],
    )


def build_overview_tab() -> html.Div:
    return html.Div(
        className="tab-body",
        children=[
            html.Div(
                className="metric-grid",
                children=[
                    metric_card("Total files", "metric-total"),
                    metric_card("Purge recommended", "metric-purge", pill_color="var(--danger)"),
                    metric_card("Due next 12 months", "metric-due", pill_color="var(--warning)"),
                    metric_card("Within retention", "metric-within", pill_color="var(--success)"),
                ],
            ),
            html.Div(
                className="panel",
                children=[
                    html.Div(
                        className="filters",
                        children=[
                            dcc.Dropdown(
                                id="status-filter",
                                options=[{"label": s, "value": s} for s in STATUS_ORDER],
                                value=STATUS_ORDER,
                                multi=True,
                                placeholder="Filter by status",
                                style={"minWidth": "220px", "color": "var(--text)"},
                            ),
                            dcc.Dropdown(
                                id="capability-filter",
                                options=[{"label": cap, "value": cap} for cap in RETENTION_RULES.keys()],
                                value=[],
                                multi=True,
                                placeholder="Filter by capability",
                                style={"minWidth": "240px", "color": "var(--text)"},
                            ),
                            dcc.Input(
                                id="text-search",
                                type="text",
                                placeholder="Search name or location...",
                                debounce=True,
                                style={"flex": "1 1 240px"},
                            ),
                        ],
                    ),
                    html.Div(
                        style={"marginTop": "12px"},
                        children=[
                            DataTable(
                                id="files-table",
                                columns=[
                                    {"name": "Status", "id": "status"},
                                    {"name": "File Name", "id": "file_name"},
                                    {"name": "Location", "id": "location"},
                                    {"name": "Capability", "id": "capability"},
                                    {"name": "Retention Code", "id": "retention_code"},
                                    {"name": "Retention Trigger", "id": "retention_trigger"},
                                    {"name": "Retention Period", "id": "retention_period"},
                                    {"name": "Rationale", "id": "rationale"},
                                    {"name": "Creator", "id": "creator"},
                                    {"name": "Employee ID", "id": "employee_id"},
                                    {"name": "Created Date", "id": "created_date"},
                                    {"name": "Recommended Purge Year", "id": "recommended_purge_year"},
                                ],
                                data=[],
                                page_size=12,
                                sort_action="native",
                                filter_action="none",
                                style_as_list_view=True,
                                style_table={
                                    "overflowX": "auto",
                                    "border": "none",
                                },
                                style_header={
                                    "backgroundColor": "var(--panel)",
                                    "color": "var(--text)",
                                    "fontWeight": "700",
                                    "borderBottom": "1px solid rgba(0,0,0,0.06)",
                                },
                                style_cell={
                                    "padding": "12px 14px",
                                    "border": "none",
                                    "whiteSpace": "normal",
                                    "height": "auto",
                                    "backgroundColor": "var(--panel)",
                                    "color": "var(--text)",
                                },
                                style_data_conditional=[
                                    {
                                        "if": {"filter_query": "{status} = 'Purge Recommended'"},
                                        "backgroundColor": "var(--soft-danger)",
                                        "color": "var(--danger)",
                                    },
                                    {
                                        "if": {"filter_query": "{status} = 'Due Next 12 Months'"},
                                        "backgroundColor": "var(--soft-warning)",
                                        "color": "var(--warning)",
                                    },
                                    {
                                        "if": {"filter_query": "{status} = 'Within Retention'"},
                                        "backgroundColor": "var(--soft-success)",
                                        "color": "var(--success)",
                                    },
                                    {
                                        "if": {"row_index": "odd"},
                                        "filter_query": "{status} = ''",
                                        "backgroundColor": "var(--panel)",
                                    },
                                ],
                            ),
                            html.Div(
                                "Status colors highlight what can go now, what comes due next, and what's still protected.",
                                style={"marginTop": "10px", "color": "var(--muted)"},
                            ),
                        ],
                    ),
                ],
            ),
        ],
    )


def metric_card(label: str, value_id: str, pill_color: str | None = None) -> html.Div:
    return html.Div(
        className="metric-card",
        children=[
            html.Div(label, className="metric-label"),
            html.Div(id=value_id, className="metric-value"),
            html.Div(
                className="metric-pill",
                style={
                    "background": pill_color or "var(--panel)",
                    "color": "var(--bg)" if pill_color else "var(--muted)",
                    "padding": "6px 10px",
                    "border": f"1px solid {pill_color or 'rgba(0,0,0,0.05)'}",
                    "width": "fit-content",
                },
                children=" ",
            ),
        ],
    )

def build_runner_tab() -> html.Div:
    return html.Div(
        className="tab-body",
        children=[
            html.Div(
                className="panel",
                children=[
                    html.Div(
                        style={"display": "flex", "flexWrap": "wrap", "gap": "12px", "alignItems": "center"},
                        children=[
                            dcc.Input(
                                id="folder-input",
                                type="text",
                                placeholder="Enter a folder path to scan",
                                debounce=False,
                                style={"flex": "1 1 360px", "padding": "12px 14px", "borderRadius": "12px"},
                            ),
                            html.Button(
                                "Start model",
                                id="start-inference",
                                n_clicks=0,
                                style={
                                    "padding": "12px 20px",
                                    "borderRadius": "12px",
                                    "border": "none",
                                    "background": "var(--primary)",
                                    "color": "#ffffff",
                                    "fontWeight": 700,
                                    "boxShadow": "var(--shadow)",
                                    "cursor": "pointer",
                                },
                            ),
                        ],
                    ),
                    html.Div(
                        id="run-status",
                        style={
                            "marginTop": "10px",
                            "fontWeight": 600,
                            "color": "var(--muted)",
                        },
                    ),
                ],
            ),
            html.Div(
                className="panel",
                children=[
                    html.Div(
                        "Latest classified files",
                        style={"fontWeight": 700, "marginBottom": "10px", "color": "var(--text)"},
                    ),
                    DataTable(
                        id="recent-table",
                        columns=[
                            {"name": "File Name", "id": "file_name"},
                            {"name": "Location", "id": "location"},
                            {"name": "Capability", "id": "capability"},
                            {"name": "Retention Code", "id": "retention_code"},
                            {"name": "Trigger", "id": "retention_trigger"},
                            {"name": "Period", "id": "retention_period"},
                            {"name": "Purge Year", "id": "recommended_purge_year"},
                        ],
                        data=[],
                        page_size=8,
                        style_as_list_view=True,
                        style_table={"overflowX": "auto", "border": "none"},
                        style_header={
                            "backgroundColor": "var(--panel)",
                            "color": "var(--text)",
                            "fontWeight": "700",
                        },
                        style_cell={
                            "padding": "10px 12px",
                            "border": "none",
                            "whiteSpace": "normal",
                            "height": "auto",
                            "backgroundColor": "var(--panel)",
                            "color": "var(--text)",
                        },
                        style_data_conditional=[
                            {
                                "if": {"column_id": "retention_code"},
                                "color": "var(--primary)",
                                "fontWeight": "700",
                            }
                        ],
                    ),
                    html.Div(
                        "Files flow into the overview table as the model completes.",
                        style={"marginTop": "8px", "color": "var(--muted)"},
                    ),
                ],
            ),
        ],
    )

def create_dash_app() -> Dash:
    app = Dash(__name__)
    app.title = "Data Owner Retention Dashboard"
    app.layout = build_layout()

    @app.callback(
        Output("theme-store", "data"),
        Input("theme-toggle", "n_clicks"),
        State("theme-store", "data"),
        prevent_initial_call=True,
    )
    def toggle_theme(n_clicks: int, current: str | None) -> str:
        current = current or "light"
        return "dark" if current == "light" else "light"

    @app.callback(Output("theme-root", "className"), Input("theme-store", "data"))
    def update_theme_class(theme_name: str | None):
        theme = theme_name or "light"
        return f"app-shell theme-{theme}"

    @app.callback(Output("theme-toggle", "children"), Input("theme-store", "data"))
    def update_theme_label(theme_name: str | None):
        return "Switch to Light" if (theme_name or "light") == "dark" else "Switch to Dark"

    @app.callback(
        Output("files-table", "data"),
        Output("metric-total", "children"),
        Output("metric-purge", "children"),
        Output("metric-due", "children"),
        Output("metric-within", "children"),
        Input("records-store", "data"),
        Input("status-filter", "value"),
        Input("capability-filter", "value"),
        Input("text-search", "value"),
    )
    def refresh_table(
        records: List[Dict[str, Any]] | None,
        status_filter: Sequence[str] | None,
        capability_filter: Sequence[str] | None,
        search: str | None,
    ):
        records = records or []
        status_filter = list(status_filter) if status_filter else STATUS_ORDER
        capability_filter = list(capability_filter) if capability_filter else []
        search_term = (search or "").lower()

        def matches(record: Dict[str, Any]) -> bool:
            if status_filter and record.get("status") not in status_filter:
                return False
            if capability_filter and record.get("capability") not in capability_filter:
                return False
            if search_term:
                haystack = f"{record.get('file_name','')} {record.get('location','')}".lower()
                if search_term not in haystack:
                    return False
            return True

        filtered = [record for record in records if matches(record)]
        total = len(records)
        purge = sum(1 for r in records if r.get("status") == "Purge Recommended")
        due = sum(1 for r in records if r.get("status") == "Due Next 12 Months")
        within = sum(1 for r in records if r.get("status") == "Within Retention")

        return filtered, str(total), str(purge), str(due), str(within)

    @app.callback(Output("recent-table", "data"), Input("recent-store", "data"))
    def refresh_recent(recent: List[Dict[str, Any]] | None):
        return recent or []
    @app.callback(
        Output("records-store", "data"),
        Output("inference-queue", "data"),
        Output("inference-meta", "data"),
        Output("inference-interval", "disabled"),
        Output("run-status", "children"),
        Output("recent-store", "data"),
        Input("start-inference", "n_clicks"),
        Input("inference-interval", "n_intervals"),
        State("folder-input", "value"),
        State("records-store", "data"),
        State("inference-queue", "data"),
        State("inference-meta", "data"),
        State("recent-store", "data"),
        prevent_initial_call=True,
    )
    def drive_inference(
        start_clicks: int | None,
        _: int | None,
        folder_value: str | None,
        records: List[Dict[str, Any]] | None,
        queue: List[Dict[str, Any]] | None,
        meta: Dict[str, Any] | None,
        recent: List[Dict[str, Any]] | None,
    ):
        ctx = callback_context
        trigger = ctx.triggered[0]["prop_id"].split(".")[0] if ctx.triggered else None

        records = records or []
        queue = queue or []
        meta = meta or {"total": 0, "processed": 0, "active": False, "root": ""}
        recent = recent or []

        if trigger == "start-inference":
            if not folder_value or not folder_value.strip():
                return (
                    no_update,
                    no_update,
                    meta,
                    True,
                    html.Span("Enter a folder path to start the model.", style={"color": "var(--danger)"}),
                    recent,
                )

            target = Path(folder_value).expanduser()
            if target.is_file():
                target = target.parent

            if not target.exists() or not target.is_dir():
                return (
                    no_update,
                    no_update,
                    meta,
                    True,
                    html.Span(f"Could not find a folder at {target}.", style={"color": "var(--danger)"}),
                    recent,
                )

            queue = build_queue_from_folder(target, limit=MAX_MODEL_FILES)
            if not queue:
                return (
                    no_update,
                    [],
                    {"total": 0, "processed": 0, "active": False, "root": str(target)},
                    True,
                    html.Span("No files found in that folder.", style={"color": "var(--danger)"}),
                    [],
                )

            meta = {"total": len(queue), "processed": 0, "active": True, "root": str(target)}
            message = html.Span(
                f"Queued {len(queue)} files from {target}. Running the model in the background...",
                style={"color": "var(--primary)", "fontWeight": 700},
            )
            return records, queue, meta, False, message, []

        if trigger == "inference-interval":
            if not queue:
                meta = {**meta, "active": False}
                return records, queue, meta, True, "Model idle.", recent

            next_record, remaining = queue[0], queue[1:]
            records.append(next_record)
            processed = (meta.get("processed") or 0) + 1
            meta = {
                "total": meta.get("total", len(queue)),
                "processed": processed,
                "active": bool(remaining),
                "root": meta.get("root", ""),
            }
            recent = [next_record, *recent][:20]

            done = not remaining
            status_msg = (
                f"Completed {processed}/{meta['total']} files from {meta.get('root') or 'the folder'}."
                if done
                else f"Processing {processed}/{meta['total']} files..."
            )
            return records, remaining, meta, done, status_msg, recent

        return no_update, no_update, meta, True, no_update, recent

    return app


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the data owner retention dashboard.")
    parser.add_argument("--host", default="127.0.0.1", help="Host/interface to bind the Dash server.")
    parser.add_argument("--port", type=int, default=8060, help="Port for the Dash server.")
    parser.add_argument("--debug", action="store_true", help="Run Dash with debug=True.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    app = create_dash_app()
    app.run(host=args.host, port=args.port, debug=args.debug)


if __name__ == "__main__":
    main()
