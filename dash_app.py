'''Dash web app for browsing the document metadata SQLite database.'''

from __future__ import annotations

import argparse
import itertools
import random
from pathlib import Path
from typing import Any, Dict, List, Tuple

from dash import Dash, Input, Output, State, callback_context, dcc, html, no_update
from dash.dash_table import DataTable

from reader import ensure_db, fetch_file_detail, fetch_summary_rows


PRIMARY_GREEN = '#007C41'  # TD green
SECONDARY_GREEN = '#0b3d2e'
LIGHT_GREEN = '#e1f5ea'
ACCENT = '#00a86b'
TEXT_COLOR = '#042d26'
CARD_BG = '#f7fffb'

TAB_STYLE = {
    'padding': '12px 18px',
    'border': 'none',
    'background': 'rgba(255, 255, 255, 0.6)',
    'color': TEXT_COLOR,
}
TAB_SELECTED_STYLE = {
    'padding': '12px 18px',
    'border': 'none',
    'background': '#ffffff',
    'color': PRIMARY_GREEN,
    'fontWeight': '600',
    'boxShadow': '0 6px 16px rgba(0, 124, 65, 0.12)',
}

CLASSIFICATION_CAPABILITIES = [
    'Operational',
    'Transitory',
    'Reports',
    'Database',
    'Analytics',
    'Reference',
]
RETENTION_PREFIXES = ['RET', 'ARC', 'TMP', 'OPS', 'DB']
MAX_CLASSIFICATION_FILES = 120

ASSETS_DIR = Path(__file__).with_name('assets')
LIGHTHOUSE_ASSET = ASSETS_DIR / 'lighthouse.png'

GLOBAL_STYLE = '''
.app-shell {
    position: relative;
}
.intro-overlay {
    position: fixed;
    inset: 0;
    background: radial-gradient(circle at 50% 40%, rgba(255, 255, 255, 0.92) 0%, rgba(0, 124, 65, 0.2) 55%, rgba(4, 45, 38, 0.88) 100%);
    display: flex;
    align-items: center;
    justify-content: center;
    flex-direction: column;
    gap: 18px;
    z-index: 999;
    animation: lighthouseFade 3.4s ease forwards;
    pointer-events: none;
}
.intro-overlay::before {
    content: '';
    position: absolute;
    inset: 0;
    background: url(/assets/lighthouse.png) center/contain no-repeat;
    opacity: 0.88;
    filter: drop-shadow(0 32px 38px rgba(0, 0, 0, 0.35));
}
.intro-overlay__caption {
    font-size: 1.25rem;
    color: #ffffff;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    text-shadow: 0 8px 18px rgba(0, 0, 0, 0.45);
    position: relative;
}
.intro-overlay--hidden {
    display: none;
}
@keyframes lighthouseFade {
    0% { opacity: 1; transform: scale(1); }
    60% { opacity: 1; transform: scale(1.02); }
    100% { opacity: 0; transform: scale(1.05); visibility: hidden; }
}
@keyframes beaconFloat {
    from { transform: translateY(0px); }
    to { transform: translateY(-16px); }
}
.tab-body {
    margin-top: 24px;
    display: flex;
    flex-direction: column;
    gap: 24px;
}
.classification-card {
    background-color: #f7fffb;
    padding: 20px;
    border-radius: 12px;
    box-shadow: 0 6px 18px rgba(0, 124, 65, 0.08);
    display: flex;
    flex-direction: column;
    gap: 16px;
}
.classification-controls {
    display: flex;
    flex-wrap: wrap;
    gap: 12px;
    align-items: center;
}
.classification-controls input {
    flex: 1 1 320px;
    padding: 10px 14px;
    border-radius: 12px;
    border: 1px solid #c9ebdc;
    background-color: #ffffff;
    box-shadow: inset 0 2px 4px rgba(4, 45, 38, 0.05);
    font-size: 15px;
}
.classification-feedback {
    min-height: 24px;
    font-weight: 500;
}
.status {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    padding: 6px 12px;
    border-radius: 999px;
    background: rgba(0, 124, 65, 0.12);
    color: #064a34;
    font-size: 0.95rem;
}
.status--error {
    background: rgba(220, 0, 78, 0.12);
    color: #8a1035;
}
.status--success {
    background: rgba(0, 124, 65, 0.18);
    color: #053829;
}
.status--running {
    background: rgba(0, 124, 65, 0.12);
}
.dash-table-container .dash-spreadsheet-container {
    border-radius: 12px !important;
}
'''

def build_intro_overlay() -> html.Div:
    '''Return an animated overlay if lighthouse asset is available.'''
    classes = 'intro-overlay' if LIGHTHOUSE_ASSET.exists() else 'intro-overlay intro-overlay--hidden'
    return html.Div(
        id='intro-overlay',
        className=classes,
        children=[
            html.Div(
                'Guiding your retention insights',
                className='intro-overlay__caption',
            )
        ],
    )


def build_files_tab() -> html.Div:
    '''Render the original explorer view inside a tab.'''
    return html.Div(
        className='tab-body',
        children=[
            html.H2(
                'Files',
                style={'color': SECONDARY_GREEN, 'marginBottom': '16px', 'fontWeight': '600'},
            ),
            DataTable(
                id='file-table',
                columns=[
                    {'name': 'ID', 'id': 'file_id'},
                    {'name': 'Name', 'id': 'file_name'},
                    {'name': 'Folder', 'id': 'folder'},
                    {'name': 'Extension', 'id': 'extension'},
                    {'name': 'Business Category', 'id': 'business_category'},
                    {'name': 'Modified', 'id': 'modified_ts'},
                    {'name': 'Size (bytes)', 'id': 'size_bytes'},
                ],
                data=[],
                row_selectable='single',
                selected_rows=[],
                page_size=15,
                sort_action='native',
                filter_action='native',
                style_as_list_view=True,
                style_table={
                    'overflowX': 'auto',
                    'border': 'none',
                    'boxShadow': '0 10px 24px rgba(0, 124, 65, 0.1)',
                    'borderRadius': '12px',
                },
                style_header={
                    'backgroundColor': PRIMARY_GREEN,
                    'color': 'white',
                    'fontWeight': '600',
                },
                style_cell={
                    'padding': '14px 16px',
                    'border': 'none',
                    'whiteSpace': 'normal',
                    'height': 'auto',
                },
                style_data_conditional=[
                    {
                        'if': {'state': 'selected'},
                        'backgroundColor': ACCENT,
                        'color': 'white',
                    },
                    {
                        'if': {'row_index': 'odd'},
                        'backgroundColor': 'rgba(0, 124, 65, 0.03)',
                    },
                ],
            ),
            html.Div(
                id='detail-section',
                style={
                    'display': 'grid',
                    'gridTemplateColumns': 'repeat(auto-fit, minmax(320px, 1fr))',
                    'gap': '24px',
                    'marginTop': '32px',
                },
                children=[
                    html.Div(
                        children=[
                            html.H2(
                                'Details',
                                style={
                                    'color': SECONDARY_GREEN,
                                    'fontWeight': '600',
                                    'marginBottom': '12px',
                                },
                            ),
                            html.Div(
                                id='file-detail',
                                style={
                                    'padding': '18px',
                                    'borderRadius': '12px',
                                    'backgroundColor': CARD_BG,
                                    'boxShadow': '0 6px 20px rgba(0, 124, 65, 0.12)',
                                    'lineHeight': 1.6,
                                },
                            ),
                        ]
                    ),
                    html.Div(
                        children=[
                            html.H2(
                                'Content Preview',
                                style={
                                    'color': SECONDARY_GREEN,
                                    'fontWeight': '600',
                                    'marginBottom': '12px',
                                },
                            ),
                            dcc.Textarea(
                                id='content-preview',
                                value='',
                                readOnly=True,
                                style={
                                    'width': '100%',
                                    'height': '360px',
                                    'padding': '16px',
                                    'borderRadius': '12px',
                                    'border': f'1px solid {LIGHT_GREEN}',
                                    'backgroundColor': '#ffffff',
                                    'boxShadow': 'inset 0 4px 12px rgba(0, 0, 0, 0.04)',
                                    'fontFamily': 'Consolas, Courier New, monospace',
                                    'fontSize': '14px',
                                },
                            ),
                        ]
                    ),
                ],
            ),
        ],
    )


def build_classification_tab() -> html.Div:
    '''Render the prototype classification workflow.'''
    return html.Div(
        className='tab-body',
        children=[
            html.Div(
                className='classification-card',
                children=[
                    html.H2(
                        'Classification Simulator',
                        style={'color': SECONDARY_GREEN, 'fontWeight': '600'},
                    ),
                    html.P(
                        'Point the simulator at a folder to watch files receive placeholder capabilities and retention codes.',
                        style={'margin': 0, 'opacity': 0.8},
                    ),
                    html.Small(
                        'Assignments are randomized until the production classifier is ready.',
                        style={'opacity': 0.65},
                    ),
                    html.Div(
                        className='classification-controls',
                        children=[
                            dcc.Input(
                                id='location-input',
                                type='text',
                                placeholder='Enter a folder path to classify',
                                debounce=False,
                            ),
                            html.Button(
                                'Start',
                                id='start-classification',
                                n_clicks=0,
                                style={
                                    'padding': '10px 24px',
                                    'borderRadius': '20px',
                                    'border': 'none',
                                    'backgroundColor': PRIMARY_GREEN,
                                    'color': '#ffffff',
                                    'fontWeight': '600',
                                    'boxShadow': '0 6px 14px rgba(0, 124, 65, 0.26)',
                                    'cursor': 'pointer',
                                },
                            ),
                        ],
                    ),
                    html.Div(id='classification-feedback', className='classification-feedback'),
                ],
            ),
            DataTable(
                id='classification-table',
                columns=[
                    {'name': 'File Name', 'id': 'file_name'},
                    {'name': 'Capability', 'id': 'capability'},
                    {'name': 'Retention Code', 'id': 'retention_code'},
                    {'name': 'Path', 'id': 'file_path'},
                ],
                data=[],
                page_size=10,
                style_as_list_view=True,
                style_table={
                    'overflowX': 'auto',
                    'border': 'none',
                    'boxShadow': '0 10px 24px rgba(0, 124, 65, 0.1)',
                    'borderRadius': '12px',
                },
                style_header={
                    'backgroundColor': PRIMARY_GREEN,
                    'color': 'white',
                    'fontWeight': '600',
                },
                style_cell={
                    'padding': '14px 16px',
                    'border': 'none',
                    'whiteSpace': 'normal',
                    'height': 'auto',
                },
                style_data_conditional=[
                    {
                        'if': {'row_index': 'odd'},
                        'backgroundColor': 'rgba(0, 124, 65, 0.03)',
                    },
                ],
            ),
        ],
    )


def build_layout(db_path: Path) -> html.Div:
    '''Create the Dash layout hierarchy.'''
    return html.Div(
        className='app-shell',
        style={
            'minHeight': '100vh',
            'padding': '32px',
            'background': f'linear-gradient(135deg, {LIGHT_GREEN} 0%, #ffffff 60%)',
            'fontFamily': 'Segoe UI, sans-serif',
            'color': TEXT_COLOR,
            'position': 'relative',
            'overflowX': 'hidden',
        },
        children=[
            html.Style(GLOBAL_STYLE),
            build_intro_overlay(),
            html.Header(
                style={
                    'display': 'flex',
                    'alignItems': 'center',
                    'gap': '16px',
                    'marginBottom': '24px',
                },
                children=[
                    html.Div(
                        'DLM Document Viewer',
                        style={
                            'fontSize': '32px',
                            'fontWeight': '600',
                            'color': PRIMARY_GREEN,
                        },
                    ),
                    html.Div(
                        'Browse ingested files, metadata, and extracted text at a glance.',
                        style={'opacity': 0.8},
                    ),
                ],
            ),
            html.Div(
                style={
                    'display': 'flex',
                    'alignItems': 'center',
                    'gap': '12px',
                    'marginBottom': '24px',
                    'padding': '12px 18px',
                    'borderRadius': '10px',
                    'backgroundColor': CARD_BG,
                    'boxShadow': '0 6px 18px rgba(0, 124, 65, 0.08)',
                },
                children=[
                    html.Span('Database', style={'fontWeight': '600'}),
                    html.Code(
                        str(db_path.resolve()),
                        style={
                            'padding': '4px 8px',
                            'borderRadius': '6px',
                            'backgroundColor': '#ffffff',
                            'border': f'1px solid {LIGHT_GREEN}',
                        },
                    ),
                    html.Button(
                        'Reload',
                        id='reload-button',
                        n_clicks=0,
                        style={
                            'marginLeft': 'auto',
                            'padding': '8px 16px',
                            'borderRadius': '20px',
                            'border': 'none',
                            'backgroundColor': PRIMARY_GREEN,
                            'color': 'white',
                            'fontWeight': '600',
                            'cursor': 'pointer',
                            'boxShadow': '0 4px 12px rgba(0, 124, 65, 0.3)',
                        },
                    ),
                    dcc.Store(id='db-store', data=str(db_path.resolve())),
                ],
            ),
            dcc.Tabs(
                id='app-tabs',
                value='explorer',
                children=[
                    dcc.Tab(
                        label='Document Explorer',
                        value='explorer',
                        style=TAB_STYLE,
                        selected_style=TAB_SELECTED_STYLE,
                        children=[build_files_tab()],
                    ),
                    dcc.Tab(
                        label='Smart Classification',
                        value='classification',
                        style=TAB_STYLE,
                        selected_style=TAB_SELECTED_STYLE,
                        children=[build_classification_tab()],
                    ),
                ],
                style={'backgroundColor': 'transparent'},
            ),
            dcc.Store(id='classification-state', data={'queue': [], 'progress': 0}),
            dcc.Interval(id='classification-interval', interval=900, disabled=True),
        ],
    )


def create_dash_app(db_path: Path) -> Dash:
    ensure_db(db_path)
    app = Dash(__name__)
    app.title = 'DLM Reader Dashboard'
    app.layout = build_layout(db_path)

    @app.callback(
        Output('file-table', 'data'),
        Output('file-table', 'selected_rows'),
        Input('reload-button', 'n_clicks'),
        State('db-store', 'data'),
        prevent_initial_call=False,
    )
    def refresh_table(_: int, db_location: str) -> Tuple[List[Dict[str, Any]], List[int]]:
        db = Path(db_location)
        rows = fetch_summary_rows(db)
        for row in rows:
            if row.get('size_bytes') is not None:
                row['size_bytes'] = int(row['size_bytes'])
        selected = [0] if rows else []
        return rows, selected

    @app.callback(
        Output('file-detail', 'children'),
        Output('content-preview', 'value'),
        Input('file-table', 'selected_rows'),
        State('file-table', 'data'),
        State('db-store', 'data'),
        prevent_initial_call=False,
    )
    def populate_detail(
        selected_rows: List[int],
        table_data: List[Dict[str, Any]],
        db_location: str,
    ) -> Tuple[Any, str]:
        if not table_data:
            return html.Div('No files available. Try reloading.'), ''
        if not selected_rows:
            return html.Div('Select a file in the table to view metadata.'), ''

        row_index = selected_rows[0]
        row = table_data[row_index]
        file_id = int(row['file_id'])

        detail = fetch_file_detail(Path(db_location), file_id)
        if not detail:
            return html.Div('No detail found for the selected file.'), ''

        file_meta = detail['file']
        labels = detail.get('labels') or []

        metadata_items = [
            html.Div(
                [html.Strong('Path'), html.Span(file_meta['path'], style={'marginLeft': '8px'})]
            ),
            html.Div(
                [
                    html.Strong('Mime Type'),
                    html.Span(file_meta.get('mime_type') or '?', style={'marginLeft': '8px'}),
                ]
            ),
            html.Div(
                [
                    html.Strong('Exists'),
                    html.Span(
                        'Yes' if file_meta.get('exists_flag') else 'No',
                        style={'marginLeft': '8px'},
                    ),
                ]
            ),
            html.Div(
                [
                    html.Strong('SHA1'),
                    html.Span(file_meta.get('sha1') or '(not computed)', style={'marginLeft': '8px'}),
                ]
            ),
            html.Div(
                [
                    html.Strong('Business Categories'),
                    html.Span(
                        ', '.join(labels) if labels else '(none)',
                        style={'marginLeft': '8px'},
                    ),
                ]
            ),
        ]

        meta_card = html.Div(
            metadata_items,
            style={
                'display': 'grid',
                'rowGap': '10px',
            },
        )

        content_preview = (detail.get('content') or '')[:5000]
        return meta_card, content_preview

    @app.callback(
        Output('classification-table', 'data'),
        Output('classification-interval', 'disabled'),
        Output('classification-state', 'data'),
        Output('classification-feedback', 'children'),
        Input('start-classification', 'n_clicks'),
        Input('classification-interval', 'n_intervals'),
        State('location-input', 'value'),
        State('classification-state', 'data'),
        prevent_initial_call=True,
    )
    def drive_classification(
        start_clicks: int,
        n_intervals: int,
        location_value: str | None,
        state: Dict[str, Any] | None,
    ):
        ctx = callback_context
        trigger = ctx.triggered[0]['prop_id'].split('.')[0] if ctx.triggered else None

        state = state or {'queue': [], 'progress': 0}
        current_queue = state.get('queue', [])
        progress = state.get('progress', 0)

        if trigger == 'start-classification':
            if not location_value or not location_value.strip():
                message = html.Span(
                    'Enter a folder path to start the simulation.',
                    className='status status--error',
                )
                return no_update, True, state, message

            target = Path(location_value).expanduser()
            if target.is_file():
                target = target.parent

            if not target.exists() or not target.is_dir():
                message = html.Span(
                    f'Could not find a folder at {target}.',
                    className='status status--error',
                )
                return no_update, True, state, message

            try:
                files_iter = (path for path in target.rglob('*') if path.is_file())
                sampled_files = list(itertools.islice(files_iter, MAX_CLASSIFICATION_FILES))
            except Exception as exc:  # pragma: no cover
                message = html.Span(
                    f'Unable to read that folder: {exc}',
                    className='status status--error',
                )
                return no_update, True, state, message

            if not sampled_files:
                message = html.Span(
                    'No files found in that folder.',
                    className='status status--error',
                )
                return no_update, True, {'queue': [], 'progress': 0}, message

            random_queue = [
                {
                    'file_name': path.name,
                    'file_path': str(path),
                    'capability': random.choice(CLASSIFICATION_CAPABILITIES),
                    'retention_code': f'{random.choice(RETENTION_PREFIXES)}-{random.randint(100, 999)}',
                }
                for path in sampled_files
            ]

            message = html.Span(
                f'Queued {len(random_queue)} files. Assigning capabilities...',
                className='status status--running',
            )
            return [], False, {'queue': random_queue, 'progress': 0}, message

        if trigger == 'classification-interval':
            if not current_queue:
                message = html.Span(
                    'Enter a folder path to start the simulation.',
                    className='status',
                )
                return [], True, {'queue': [], 'progress': 0}, message

            if progress >= len(current_queue):
                message = html.Span(
                    f'Classification complete. Processed {len(current_queue)} files.',
                    className='status status--success',
                )
                return current_queue, True, {'queue': current_queue, 'progress': progress}, message

            progress += 1
            completed = current_queue[:progress]
            done = progress >= len(current_queue)
            message = html.Span(
                (
                    f'Classification complete. Processed {len(current_queue)} files.'
                    if done
                    else f'Assigning retention codes... {progress}/{len(current_queue)} files complete.'
                ),
                className='status status--success' if done else 'status status--running',
            )
            return completed, done, {'queue': current_queue, 'progress': progress}, message

        return no_update, True, state, no_update

    return app


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Run the Dash viewer for the document database.')
    parser.add_argument(
        '--db',
        type=Path,
        default=Path('artifacts') / 'dlm_reader.db',
        help='Path to the SQLite database populated by reader.py',
    )
    parser.add_argument('--host', default='127.0.0.1', help='Hostname/interface to bind the Dash server.')
    parser.add_argument('--port', type=int, default=8050, help='Port for the Dash server.')
    parser.add_argument('--debug', action='store_true', help='Run Dash with debug=True for auto-reload.')
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    app = create_dash_app(args.db)
    app.run(host=args.host, port=args.port, debug=args.debug)


if __name__ == '__main__':
    main()
