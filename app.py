
import os
import sqlite3
import pandas as pd
import streamlit as st
from datetime import datetime
from reader import ingest_from_excel, peek_db, DB_PATH_DEFAULT
from model import build_dataset_from_db, train_pipeline, save_model, MODEL_PATH

st.set_page_config(page_title="DLM Reader & Training Demo", layout="wide")
st.sidebar.title("DLM Demo")
page = st.sidebar.radio("Navigate", ["Reader / Dataset", "Train"])

# ---------------- Reader / Dataset ----------------
if page == "Reader / Dataset":
    st.header("Reader & Dataset Builder")
    st.write("Upload your **training Excel** (columns: `file_location`, `business_category`). "
             "The reader will extract metadata + raw text for each file and store it in **SQLite**.")

    uploaded = st.file_uploader("Upload training Excel (.xlsx)", type=["xlsx"])
    use_sample = st.checkbox("Use bundled sample instead", value=not uploaded)

    db_path = st.text_input("SQLite DB path", value=DB_PATH_DEFAULT)
    limit = st.number_input("Ingest limit (0 = all rows)", min_value=0, value=0, step=100)

    if st.button("Ingest now"):
        if use_sample:
            excel_path = os.path.join("data", "sample_training.xlsx")
        elif uploaded:
            # save temp
            tmp = os.path.join("artifacts", "uploaded_training.xlsx")
            with open(tmp, "wb") as f:
                f.write(uploaded.getbuffer())
            excel_path = tmp
        else:
            st.warning("Please upload an Excel or use the sample.")
            st.stop()

        with st.spinner("Reading files and populating SQLite..."):
            stats = ingest_from_excel(excel_path, db_path, limit=limit or None)
        st.success("Ingestion complete.")
        st.json(stats)

    st.subheader("Peek into the DB")
    if os.path.exists(db_path):
        df_files, df_labels, df_content = peek_db(db_path, head=50)
        st.write("Files (first 50)")
        st.dataframe(df_files, use_container_width=True)
        st.write("Labels (first 50)")
        st.dataframe(df_labels, use_container_width=True)
        st.write("Content (first 5, truncated)")
        st.dataframe(df_content, use_container_width=True)
    else:
        st.info("No DB found yet. Ingest to create one.")

# ---------------- Train ----------------
else:
    st.header("Train Baseline Model")
    db_path = st.text_input("SQLite DB path", value=DB_PATH_DEFAULT)
    use_content = st.checkbox("Use raw content text", value=True)
    use_path_tokens = st.checkbox("Use path/folder tokens", value=True)
    use_extension = st.checkbox("Use file extension", value=True)

    test_size = st.slider("Holdout size", 0.1, 0.4, 0.2, 0.05)
    algo = st.selectbox("Classifier", ["logreg", "linearsvm"])

    if st.button("Build dataset & train"):
        if not os.path.exists(db_path):
            st.error("DB not found. Please ingest first.")
            st.stop()
        with st.spinner("Building dataset..."):
            df = build_dataset_from_db(db_path, use_content=use_content, use_path_tokens=use_path_tokens, use_extension=use_extension)
        st.write(f"Dataset rows: {len(df)}, classes: {df['business_category'].nunique()}")
        st.dataframe(df.head(20), use_container_width=True)

        with st.spinner("Training..."):
            pipe, report, cm = train_pipeline(df, algo=algo, test_size=test_size)
            save_model(pipe)
        st.success(f"Model trained and saved to {MODEL_PATH}")
        st.subheader("Classification report")
        st.text(report)
        st.subheader("Confusion matrix")
        st.dataframe(cm, use_container_width=True)
