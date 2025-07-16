# absa_quad_extractor/data_loader.py
import pandas as pd
import json
import os
import sqlite3

class DataLoader:
    @staticmethod
    def load_data(file_path):
        ext = os.path.splitext(file_path)[-1].lower()

        if ext == ".csv":
            df = pd.read_csv(file_path, index_col=False)
        elif ext == ".json":
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            df = pd.DataFrame(data)
        elif ext in [".txt", ".log"]:
            with open(file_path, "r", encoding="utf-8") as f:
                lines = f.readlines()
            df = pd.DataFrame({"review": [line.strip() for line in lines if line.strip()]})
        else:
            raise ValueError(f"Unsupported file extension: {ext}")

        if "content" not in df.columns:
            raise ValueError("Input file must contain a 'content' column")

        return df, df["content"].astype(str).tolist()

    @staticmethod
    def load_data_from_db(db_path, table_name, review_column="content"):
        if not os.path.exists(db_path):
            raise FileNotFoundError(f"Database not found at {db_path}")

        conn = sqlite3.connect(db_path)
        query = f"SELECT {review_column} FROM {table_name}"
        df = pd.read_sql_query(query, conn)
        conn.close()

        if review_column not in df.columns:
            raise ValueError(f"Table '{table_name}' must contain a '{review_column}' column")

        return df, df[review_column].astype(str).tolist()
