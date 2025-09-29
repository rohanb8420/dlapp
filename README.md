
# DLM Reader & Training Demo

**Focus:** Reader module → SQLite dataset → Baseline training (no rules, no inference, no logs yet).

## What it does
- Ingests an Excel with columns: `file_location`, `business_category`
- For each path, extracts **metadata** and **raw text** (pdf/docx/pptx/xlsx/csv/twb/twbx/others via Tika)
- Stores into **SQLite** (`files`, `labels`, `content`)
- Builds a dataset and trains a baseline classifier (path tokens, extension, optional content TF‑IDF)

## Run
```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
streamlit run app.py
```

> Tika is optional but expands format coverage (e.g., OneNote). It requires Java and will download a JAR on first use.

