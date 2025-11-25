"""Streamlit dashboard for retention and disposition insights."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd
import streamlit as st

# Core TD-inspired palette. Users can toggle between light and dark variants.
THEMES: Dict[str, Dict[str, str]] = {
    "Light": {
        "bg": "#f4fbf7",
        "panel": "#ffffff",
        "text": "#0b3d2e",
        "muted": "#5c7c6b",
        "primary": "#007c41",
        "accent": "#00a870",
        "shadow": "0 14px 30px rgba(0, 124, 65, 0.14)",
    },
    "Dark": {
        "bg": "#071c17",
        "panel": "#0f2b23",
        "text": "#e7f4ee",
        "muted": "#a0c3b4",
        "primary": "#43d487",
        "accent": "#7efcb4",
        "shadow": "0 18px 38px rgba(0, 0, 0, 0.45)",
    },
}

# Status palette used for metrics and the chip column.
STATUS_STYLES: Dict[str, Dict[str, str]] = {
    "Purge Recommended": {"bg": "#ffebee", "text": "#b00020", "chip": "🔴"},
    "Due Next 12 Months": {"bg": "#fff4e5", "text": "#bb4a00", "chip": "🟠"},
    "Within Retention": {"bg": "#e3f2ed", "text": "#0b6b3a", "chip": "🟢"},
    "Metadata Missing": {"bg": "#eceff1", "text": "#37474f", "chip": "⚪️"},
}
STATUS_ORDER = list(STATUS_STYLES.keys())

TRAINING_DATA_PATH = Path("assets") / "training_data.xlsx"
DEFAULT_SCAN_ROOT = Path.cwd()
MAX_SCAN_LIMIT = 1000


@dataclass(frozen=True)
class RetentionRule:
    code: str
    trigger: str
    period_years: int
    rationale: str


RETENTION_RULES: Dict[str, RetentionRule] = {
    "Finance & Accounting": RetentionRule(
        code="FIN-07",
        trigger="Fiscal year close",
        period_years=7,
        rationale="Audit and statutory reporting obligations.",
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
        rationale="Process reference required during lookback windows.",
    ),
}
DEFAULT_RULE = RetentionRule(
    code="OPS-04",
    trigger="Process completion",
    period_years=4,
    rationale="Default operational retention guidance.",
)

# Keywords -> capability signals. Multiple keywords per tuple.
CAPABILITY_KEYWORDS: List[Tuple[Iterable[str], str]] = [
    (("payroll", "benefit", "performance review"), "Human Capital"),
    (("invoice", "ledger", "expense"), "Finance & Accounting"),
    (("client", "prospect", "campaign"), "Sales & Client Delivery"),
    (("control", "compliance", "policy"), "Risk & Compliance"),
    (("data model", "api", "architecture"), "Technology & Data"),
    (("sop", "ops", "workflow"), "Operations & Shared Services"),
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

DEFAULT_SCAN_ROOT = Path.cwd()
MAX_SCAN_LIMIT = 1000


def inject_theme(theme: Dict[str, str]) -> None:
    """Inject CSS variables so Streamlit widgets inherit TD look."""
    css = f"""
    <style>
    body, .stApp {{
        background-color: {theme['bg']};
        color: {theme['text']};
    }}
    .ret-card {{
        background: {theme['panel']};
        border-radius: 16px;
        padding: 18px 22px;
        box-shadow: {theme['shadow']};
    }}
    .ret-card h3 {{
        margin: 0;
        font-size: 0.95rem;
        color: {theme['muted']};
        font-weight: 500;
    }}
    .ret-card p {{
        margin: 6px 0 0;
        font-size: 1.8rem;
        color: {theme['text']};
        font-weight: 600;
    }}
    .status-chip {{
        display: inline-flex;
        align-items: center;
        gap: 6px;
        padding: 6px 12px;
        border-radius: 999px;
        font-weight: 600;
    }}
    </style>
    """
    st.markdown(css, unsafe_allow_html=True)


def add_years(dt: datetime, years: int) -> datetime:
    """Return dt advanced by `years`, handling leap dates."""
    try:
        return dt.replace(year=dt.year + years)
    except ValueError:
        return dt.replace(month=3, day=1, year=dt.year + years)


def infer_capability(record: Dict[str, str]) -> str:
    haystack = f"{record['folder']} {record['file_name']}".lower()
    for keywords, capability in CAPABILITY_KEYWORDS:
        if any(term in haystack for term in keywords):
            return capability
    ext = record.get("extension", "").lower()
    if ext in {"py", "sql", "json", "yaml"}:
        return "Technology & Data"
    if ext in {"ppt", "pptx"}:
        return "Sales & Client Delivery"
    if ext in {"xls", "xlsx"}:
        return "Finance & Accounting"
    return "Operations & Shared Services"


def lookup_rule(capability: str) -> RetentionRule:
    return RETENTION_RULES.get(capability, DEFAULT_RULE)


def assign_owner(path_str: str) -> Tuple[str, str]:
    hashed = int(hashlib.sha1(path_str.encode("utf-8")).hexdigest(), 16)
    idx = hashed % len(EMPLOYEE_DIRECTORY)
    return EMPLOYEE_DIRECTORY[idx]


def determine_status(created_dt: Optional[datetime], rule: RetentionRule) -> Tuple[str, Optional[datetime]]:
    if not created_dt:
        return "Metadata Missing", None
    purge_dt = add_years(created_dt, rule.period_years)
    today = datetime.now()
    soon = today + timedelta(days=365)
    if purge_dt <= today:
        return "Purge Recommended", purge_dt
    if purge_dt <= soon:
        return "Due Next 12 Months", purge_dt
    return "Within Retention", purge_dt


def collect_file_metadata(root: Path, limit: int, include_hidden: bool = False) -> List[Dict[str, Optional[str]]]:
    records: List[Dict[str, Optional[str]]] = []
    seen = 0
    for path in root.rglob("*"):
        if path.is_dir():
            continue
        if not include_hidden and path.name.startswith("."):
            continue
        try:
            stat = path.stat()
        except OSError:
            continue
        created = datetime.fromtimestamp(stat.st_ctime) if stat.st_ctime else None
        record = {
            "file_name": path.name,
            "location": str(path),
            "folder": str(path.parent),
            "extension": path.suffix.lstrip(".").lower(),
            "created_ts": created,
            "size_mb": round(stat.st_size / (1024 * 1024), 2),
        }
        records.append(record)
        seen += 1
        if seen >= limit:
            break
    return records


def enrich_retention_rows(records: Iterable[Dict[str, Optional[str]]]) -> List[Dict[str, Optional[str]]]:
    enriched: List[Dict[str, Optional[str]]] = []
    for record in records:
        capability = infer_capability(record)
        rule = lookup_rule(capability)
        status, purge_dt = determine_status(record.get("created_ts"), rule)
        owner_name, owner_id = assign_owner(record["location"])
        enriched.append(
            {
                "File Name": record["file_name"],
                "Location": record["location"],
                "Capability": capability,
                "Retention Code": rule.code,
                "Retention Trigger": rule.trigger,
                "Retention Period": f"{rule.period_years} years",
                "Rationale": rule.rationale,
                "Creator": owner_name,
                "Workday ID": owner_id,
                "File Created": record["created_ts"].date().isoformat() if record["created_ts"] else "Unknown",
                "Recommended Year of Purge": purge_dt.year if purge_dt else "Unknown",
                "Status": status,
                "Status Chip": f"{STATUS_STYLES[status]['chip']} {status}",
            }
        )
    return enriched


@st.cache_data(show_spinner=False)
def build_inventory_dataframe(path_str: str, limit: int, include_hidden: bool) -> pd.DataFrame:
    root = Path(path_str).expanduser()
    if not root.exists():
        raise FileNotFoundError(f"No folder found at {root}")
    if not root.is_dir():
        raise NotADirectoryError(f"{root} is not a directory")
    records = collect_file_metadata(root, limit=limit, include_hidden=include_hidden)
    data = enrich_retention_rows(records)
    return pd.DataFrame(data)


@st.cache_data(show_spinner=False)
def load_training_samples(path_str: str = str(TRAINING_DATA_PATH)) -> pd.DataFrame:
    dataset_path = Path(path_str)
    if not dataset_path.exists():
        raise FileNotFoundError(f"Training data not found at {dataset_path}")
    df = pd.read_excel(dataset_path)
    rename_map = {
        "Business Capability": "capability",
        "Retention Code": "retention_code",
    }
    df = df.rename(columns=rename_map)
    if "capability" not in df.columns:
        raise ValueError("Training sheet missing 'Business Capability' column.")
    df["capability"] = df["capability"].astype(str).str.strip()
    if "retention_code" not in df.columns:
        df["retention_code"] = ""
    else:
        df["retention_code"] = df["retention_code"].fillna("").astype(str)
    df = df[df["capability"].str.len() > 0]
    return df[["capability", "retention_code"]]


def sample_training_assignments(count: int) -> List[Dict[str, str]]:
    dataset = load_training_samples()
    if dataset.empty:
        raise ValueError("Training dataset is empty.")
    replace = len(dataset) < count
    sampled = dataset.sample(n=count, replace=replace)
    return sampled.to_dict(orient="records")


def render_summary_cards(df: pd.DataFrame, theme: Dict[str, str]) -> None:
    cols = st.columns(3)
    tabulated = df["Status"].value_counts()
    for idx, key in enumerate(STATUS_ORDER[:3]):
        with cols[idx]:
            count = int(tabulated.get(key, 0))
            styles = STATUS_STYLES[key]
            card = f"""
            <div class="ret-card" style="border-top: 4px solid {styles['text']};">
                <h3>{key}</h3>
                <p>{count}</p>
            </div>
            """
            st.markdown(card, unsafe_allow_html=True)


def filter_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    available_status = [s for s in STATUS_ORDER if s in df["Status"].unique()]
    status_filter = st.multiselect(
        "Status filter",
        options=available_status,
        default=available_status,
    )
    capabilities = sorted(df["Capability"].unique())
    cap_filter = st.multiselect(
        "Capability filter",
        options=capabilities,
        default=capabilities,
    )
    filtered = df[df["Status"].isin(status_filter) & df["Capability"].isin(cap_filter)]
    return filtered


def render_inventory_tab(theme: Dict[str, str]) -> None:
    st.subheader("Shared Drive Inventory")
    if "inventory_path" not in st.session_state:
        st.session_state["inventory_path"] = str(DEFAULT_SCAN_ROOT)
    if "inventory_df" not in st.session_state:
        st.session_state["inventory_df"] = None

    path_value = st.text_input("Shared drive path", value=st.session_state["inventory_path"])
    include_hidden = st.checkbox("Include hidden files", value=False)
    limit_value = st.slider("Scan limit", min_value=50, max_value=MAX_SCAN_LIMIT, value=400, step=50)
    scan_clicked = st.button("Scan shared drive", use_container_width=True)

    if scan_clicked:
        try:
            with st.spinner("Indexing folders..."):
                df = build_inventory_dataframe(path_value, limit_value, include_hidden)
            st.session_state["inventory_df"] = df
            st.session_state["inventory_path"] = path_value
            st.success(f"Loaded {len(df)} files from {path_value}")
        except (FileNotFoundError, NotADirectoryError) as exc:
            st.error(str(exc))
        except Exception as exc:  # pragma: no cover
            st.error(f"Unable to scan that folder: {exc}")

    df = st.session_state["inventory_df"]
    if df is None or df.empty:
        st.info("Scan a shared drive to populate the dashboard.")
        return

    render_summary_cards(df, theme)
    st.markdown("---")

    filtered = filter_dataframe(df)
    st.caption(f"{len(filtered)} of {len(df)} files match the active filters.")

    st.dataframe(
        filtered[
            [
                "Status Chip",
                "File Name",
                "Location",
                "Capability",
                "Retention Code",
                "Retention Trigger",
                "Retention Period",
                "Rationale",
                "Creator",
                "Workday ID",
                "File Created",
                "Recommended Year of Purge",
            ]
        ],
        use_container_width=True,
        hide_index=True,
    )

    csv = filtered.to_csv(index=False).encode("utf-8")
    st.download_button("Download filtered table", data=csv, file_name="retention_inventory.csv", mime="text/csv")


def simulate_model_run(folder: Path, limit: int, include_hidden: bool = False) -> pd.DataFrame:
    records = collect_file_metadata(folder, limit=limit, include_hidden=include_hidden)
    if not records:
        return pd.DataFrame(
            columns=[
                "File Name",
                "Location",
                "Business Capability",
                "Retention Code",
                "Retention Trigger",
                "Retention Period",
                "Rationale",
            ]
        )

    try:
        assignments = sample_training_assignments(len(records))
    except Exception:
        assignments = None

    rows: List[Dict[str, str]] = []
    for idx, record in enumerate(records):
        assigned_capability: Optional[str] = None
        assigned_code: Optional[str] = None
        if assignments:
            sample = assignments[idx]
            assigned_capability = sample.get("capability")
            assigned_code = sample.get("retention_code")

        capability = assigned_capability or infer_capability(record)
        rule = lookup_rule(capability)
        retention_code = assigned_code or rule.code

        rows.append(
            {
                "File Name": record["file_name"],
                "Location": record["location"],
                "Business Capability": capability,
                "Retention Code": retention_code,
                "Retention Trigger": rule.trigger,
                "Retention Period": f"{rule.period_years} years",
                "Rationale": rule.rationale,
            }
        )

    return pd.DataFrame(rows)


def render_model_tab(theme: Dict[str, str]) -> None:
    st.subheader("Run Retention Model")
    if "model_results" not in st.session_state:
        st.session_state["model_results"] = None

    folder = st.text_input(
        "Folder to score",
        value=st.session_state.get("inventory_path", str(DEFAULT_SCAN_ROOT)),
    )
    limit = st.slider("Max files for model run", min_value=20, max_value=MAX_SCAN_LIMIT, value=200, step=10, key="model_limit")
    include_hidden = st.checkbox("Include hidden files for model run", value=False, key="model_hidden")
    start = st.button("Start model", use_container_width=True, key="start_model_btn")

    if start:
        target = Path(folder).expanduser()
        if not target.exists():
            st.error("That folder does not exist.")
        elif not target.is_dir():
            st.error("Provide a folder path, not a file.")
        else:
            try:
                with st.spinner("Dispatching model..."):
                    df = simulate_model_run(target, limit=limit, include_hidden=include_hidden)
                st.session_state["model_results"] = df
                st.success(f"Model evaluated {len(df)} files.")
            except Exception as exc:  # pragma: no cover
                st.error(f"Model failed: {exc}")

    df = st.session_state["model_results"]
    if df is None or df.empty:
        st.info("Kick off a model run to populate this table.")
        return

    st.dataframe(df, use_container_width=True, hide_index=True)
    csv = df.to_csv(index=False).encode("utf-8")
    st.download_button("Download model output", data=csv, file_name="retention_model_output.csv", mime="text/csv")


def main() -> None:
    st.set_page_config(
        page_title="Retention & Disposition Dashboard",
        layout="wide",
        page_icon="📁",
    )
    if "theme_choice" not in st.session_state:
        st.session_state["theme_choice"] = "Light"

    with st.sidebar:
        st.header("Display")
        theme_options = list(THEMES.keys())
        default_idx = theme_options.index(st.session_state["theme_choice"])
        theme_choice = st.radio("Theme", options=theme_options, index=default_idx)
        st.session_state["theme_choice"] = theme_choice
        st.markdown("Designed in TD colors with light and dark modes.")

    theme = THEMES[st.session_state["theme_choice"]]
    inject_theme(theme)

    st.title("Data Retention & Disposition Dashboard")
    st.caption("Inventory, monitor, and act on retention obligations in one TD-styled workspace.")

    tab1, tab2 = st.tabs(["Inventory Overview", "Run Model"])
    with tab1:
        render_inventory_tab(theme)
    with tab2:
        render_model_tab(theme)


if __name__ == "__main__":
    main()
