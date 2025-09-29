
import os
import io
import re
import csv
import zipfile
import hashlib
import sqlite3
import mimetypes
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, Tuple

import pandas as pd

# Optional imports (soft dependencies)
try:
    from docx import Document as DocxDocument
except Exception:
    DocxDocument = None

try:
    from pptx import Presentation as PptxPresentation
except Exception:
    PptxPresentation = None

try:
    import openpyxl
except Exception:
    openpyxl = None

try:
    from PyPDF2 import PdfReader
except Exception:
    PdfReader = None

# Tika is optional; requires Java and first-run download
try:
    from tika import parser as tika_parser
except Exception:
    tika_parser = None

DB_PATH_DEFAULT = os.path.join("artifacts", "dlm_reader.db")
MAX_TEXT = 100_000  # limit stored text per file for demo

SCHEMA = """
CREATE TABLE IF NOT EXISTS files(
  file_id INTEGER PRIMARY KEY,
  path TEXT UNIQUE,
  folder TEXT,
  file_name TEXT,
  extension TEXT,
  mime_type TEXT,
  size_bytes INTEGER,
  created_ts TEXT,
  modified_ts TEXT,
  sha1 TEXT,
  exists_flag INTEGER,
  read_error TEXT
);
CREATE TABLE IF NOT EXISTS labels(
  file_id INTEGER,
  business_category TEXT,
  FOREIGN KEY(file_id) REFERENCES files(file_id)
);
CREATE TABLE IF NOT EXISTS content(
  file_id INTEGER,
  content_text TEXT,
  FOREIGN KEY(file_id) REFERENCES files(file_id)
);
CREATE INDEX IF NOT EXISTS idx_files_path ON files(path);
CREATE INDEX IF NOT EXISTS idx_labels_file ON labels(file_id);
"""

def ensure_db(db_path: str):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    con = sqlite3.connect(db_path)
    with con:
        con.executescript(SCHEMA)
    con.close()

def sha1_of_file(path: str) -> Optional[str]:
    try:
        h = hashlib.sha1()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return None

def stat_path(path: str) -> Dict[str, Any]:
    p = Path(path)
    info = {
        "path": str(p),
        "folder": str(p.parent),
        "file_name": p.name,
        "extension": p.suffix.lower().lstrip("."),
        "mime_type": mimetypes.guess_type(str(p))[0] or "",
        "size_bytes": None,
        "created_ts": None,
        "modified_ts": None,
        "sha1": None,
        "exists_flag": 0,
        "read_error": None,
    }
    try:
        st = p.stat()
        info.update({
            "size_bytes": int(st.st_size),
            "created_ts": datetime.fromtimestamp(st.st_ctime).isoformat(),
            "modified_ts": datetime.fromtimestamp(st.st_mtime).isoformat(),
            "sha1": sha1_of_file(str(p)),
            "exists_flag": 1
        })
    except Exception as e:
        info["read_error"] = f"stat_error: {e}"
    return info

def _read_txt(path: str) -> str:
    try:
        with open(path, "r", errors="ignore") as f:
            return f.read()
    except Exception:
        return ""

def _read_csv(path: str) -> str:
    try:
        out = []
        with open(path, newline="", errors="ignore") as f:
            for i, row in enumerate(csv.reader(f)):
                out.append(",".join(row))
                if len("".join(out)) > MAX_TEXT:
                    break
        return "\n".join(out)
    except Exception:
        return ""

def _read_pdf(path: str) -> str:
    if PdfReader is None:
        return ""
    try:
        reader = PdfReader(path)
        texts = []
        for i, page in enumerate(reader.pages):
            if len("".join(texts)) > MAX_TEXT:
                break
            txt = page.extract_text() or ""
            texts.append(txt)
        return "\n".join(texts)
    except Exception:
        return ""

def _read_docx(path: str) -> str:
    if DocxDocument is None:
        return ""
    try:
        doc = DocxDocument(path)
        paras = [p.text for p in doc.paragraphs if p.text]
        return "\n".join(paras)[:MAX_TEXT]
    except Exception:
        return ""

def _read_pptx(path: str) -> str:
    if PptxPresentation is None:
        return ""
    try:
        prs = PptxPresentation(path)
        texts = []
        for slide in prs.slides:
            for shape in slide.shapes:
                if hasattr(shape, "text"):
                    texts.append(shape.text)
            if len("".join(texts)) > MAX_TEXT:
                break
        return "\n".join(texts)[:MAX_TEXT]
    except Exception:
        return ""

def _read_xlsx(path: str) -> str:
    if openpyxl is None:
        return ""
    try:
        wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
        texts = []
        for ws in wb.worksheets[:3]:
            for row in ws.iter_rows(min_row=1, max_row=200, values_only=True):
                vals = [str(v) for v in row if v is not None]
                if vals:
                    texts.append(" ".join(vals))
                if len("".join(texts)) > MAX_TEXT:
                    break
        return "\n".join(texts)[:MAX_TEXT]
    except Exception:
        return ""

def _read_twb(path: str) -> str:
    # Tableau .twb is XML; extract titles, captions, calc names
    try:
        tree = ET.parse(path)
        root = tree.getroot()
        texts = []
        for elem in root.iter():
            for attr in ["name", "caption", "label", "value"]:
                v = elem.attrib.get(attr)
                if v:
                    texts.append(v)
            if elem.text and elem.text.strip():
                texts.append(elem.text.strip())
            if len("".join(texts)) > MAX_TEXT:
                break
        return "\n".join(texts)[:MAX_TEXT]
    except Exception:
        return ""

def _read_twbx(path: str) -> str:
    # Zip containing a .twb and possibly data extracts; read the .twb
    try:
        with zipfile.ZipFile(path, "r") as z:
            twb_members = [m for m in z.namelist() if m.lower().endswith(".twb")]
            texts = []
            for m in twb_members:
                with z.open(m) as f:
                    try:
                        xml = f.read()
                        root = ET.fromstring(xml)
                        for elem in root.iter():
                            for attr in ["name", "caption", "label", "value"]:
                                v = elem.attrib.get(attr)
                                if v:
                                    texts.append(v)
                            if elem.text and elem.text.strip():
                                texts.append(elem.text.strip())
                            if len("".join(texts)) > MAX_TEXT:
                                break
                    except Exception:
                        continue
            return "\n".join(texts)[:MAX_TEXT]
    except Exception:
        return ""

def _read_with_tika(path: str) -> str:
    if tika_parser is None:
        return ""
    try:
        parsed = tika_parser.from_file(path)
        content = parsed.get("content") or ""
        return content[:MAX_TEXT]
    except Exception:
        return ""

def extract_text(path: str, extension: str) -> str:
    ext = extension.lower()
    if ext in ("txt", "log", "md"):
        return _read_txt(path)
    if ext in ("csv",):
        return _read_csv(path)
    if ext in ("pdf",):
        t = _read_pdf(path)
        return t if t else _read_with_tika(path)
    if ext in ("docx",):
        t = _read_docx(path)
        return t if t else _read_with_tika(path)
    if ext in ("pptx",):
        t = _read_pptx(path)
        return t if t else _read_with_tika(path)
    if ext in ("xlsx", "xlsm"):
        t = _read_xlsx(path)
        return t if t else _read_with_tika(path)
    if ext in ("twb",):
        return _read_twb(path)
    if ext in ("twbx",):
        return _read_twbx(path)
    if ext in ("one", "ppt", "doc", "xls", "msg"):
        # legacy / complex -> try Tika
        return _read_with_tika(path)
    # default: try Tika; else empty
    return _read_with_tika(path)

def upsert_file(con, meta: dict) -> int:
    cur = con.cursor()
    cur.execute("""
        INSERT INTO files(path, folder, file_name, extension, mime_type, size_bytes, created_ts, modified_ts, sha1, exists_flag, read_error)
        VALUES (:path, :folder, :file_name, :extension, :mime_type, :size_bytes, :created_ts, :modified_ts, :sha1, :exists_flag, :read_error)
        ON CONFLICT(path) DO UPDATE SET
           folder=excluded.folder,
           file_name=excluded.file_name,
           extension=excluded.extension,
           mime_type=excluded.mime_type,
           size_bytes=excluded.size_bytes,
           created_ts=excluded.created_ts,
           modified_ts=excluded.modified_ts,
           sha1=excluded.sha1,
           exists_flag=excluded.exists_flag,
           read_error=excluded.read_error
    """, meta)
    con.commit()
    cur.execute("SELECT file_id FROM files WHERE path=?", (meta["path"],))
    return int(cur.fetchone()[0])

def upsert_label(con, file_id: int, business_category: str):
    con.execute("""
        INSERT INTO labels(file_id, business_category) VALUES (?,?)
    """, (file_id, business_category))
    con.commit()

def upsert_content(con, file_id: int, text: str):
    con.execute("""
        INSERT INTO content(file_id, content_text) VALUES (?,?)
    """, (file_id, text))
    con.commit()

def ingest_from_excel(excel_path: str, db_path: str, limit: int = None):
    ensure_db(db_path)
    df = pd.read_excel(excel_path)
    assert {"file_location", "business_category"}.issubset(df.columns), "Excel must have file_location, business_category"
    if limit:
        df = df.head(limit)
    con = sqlite3.connect(db_path)
    inserted = 0
    errors = 0
    empty_text = 0
    for row in df.itertuples():
        path = str(row.file_location)
        meta = stat_path(path)
        try:
            file_id = upsert_file(con, meta)
            upsert_label(con, file_id, str(row.business_category))
            txt = ""
            if meta["exists_flag"]:
                txt = extract_text(path, meta["extension"] or "")
            if not txt:
                empty_text += 1
            upsert_content(con, file_id, txt)
            inserted += 1
        except Exception as e:
            errors += 1
    con.close()
    return {"inserted": inserted, "errors": errors, "empty_text": empty_text}

def peek_db(db_path: str, head: int = 50):
    con = sqlite3.connect(db_path)
    df_files = pd.read_sql_query("SELECT * FROM files LIMIT ?", con, params=(head,))
    df_labels = pd.read_sql_query("SELECT * FROM labels LIMIT ?", con, params=(head,))
    df_content = pd.read_sql_query("SELECT file_id, substr(content_text,1,300) AS content_text FROM content LIMIT 5", con)
    con.close()
    return df_files, df_labels, df_content
