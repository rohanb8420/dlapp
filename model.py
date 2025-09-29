
import os
import re
import sqlite3
import joblib
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.svm import LinearSVC
from sklearn.metrics import classification_report, confusion_matrix

MODEL_PATH = os.path.join("artifacts", "reader_baseline_model.joblib")

TOKEN_PATTERN = r"[A-Za-z0-9]+"

def tokenize_path(path: str) -> str:
    path = path.replace("\\\\", "/").lower()
    parts = re.findall(TOKEN_PATTERN, path)
    ext = path.split(".")[-1] if "." in path else ""
    if ext:
        parts.append(f"ext_{ext}")
    return " ".join(parts)

def build_dataset_from_db(db_path: str, use_content=True, use_path_tokens=True, use_extension=True) -> pd.DataFrame:
    con = sqlite3.connect(db_path)
    q = """
        SELECT f.path, f.extension, l.business_category, c.content_text
        FROM files f
        JOIN labels l ON l.file_id = f.file_id
        LEFT JOIN content c ON c.file_id = f.file_id
    """
    df = pd.read_sql_query(q, con)
    con.close()
    # Feature columns
    cols = {}
    if use_path_tokens:
        cols["path_tokens"] = df["path"].map(tokenize_path)
    if use_content:
        cols["content_text"] = (df["content_text"] or "").fillna("")
    if use_extension:
        cols["extension"] = df["extension"].fillna("")
    X = pd.DataFrame(cols)
    y = df["business_category"].astype(str)
    X["business_category"] = y
    return X

def train_pipeline(df: pd.DataFrame, algo="logreg", test_size=0.2, random_state=42):
    y = df["business_category"]
    X = df.drop(columns=["business_category"])

    transformers = []
    if "path_tokens" in X.columns:
        transformers.append(("path", TfidfVectorizer(ngram_range=(1,2), max_features=80000), "path_tokens"))
    if "content_text" in X.columns:
        transformers.append(("content", TfidfVectorizer(ngram_range=(1,2), max_features=120000, min_df=2), "content_text"))
    if "extension" in X.columns:
        transformers.append(("ext", OneHotEncoder(handle_unknown="ignore"), ["extension"]))

    pre = ColumnTransformer(transformers)
    if algo == "linearsvm":
        clf = LinearSVC()
    else:
        clf = LogisticRegression(max_iter=3000, class_weight="balanced")

    pipe = Pipeline([("pre", pre), ("clf", clf)])

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size, random_state=random_state, stratify=y)
    pipe.fit(X_train, y_train)
    y_pred = pipe.predict(X_test)
    report = classification_report(y_test, y_pred, zero_division=0)
    labels = sorted(y.unique())
    cm = pd.DataFrame(confusion_matrix(y_test, y_pred, labels=labels), index=labels, columns=labels)
    return pipe, report, cm

def save_model(pipe):
    os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
    joblib.dump(pipe, MODEL_PATH)
