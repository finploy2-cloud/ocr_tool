# db/crud.py
from sqlalchemy import text
import pandas as pd
from utils.db_utils import engine   # ensure you expose engine in db_utils

def upsert_candidate_details(details: dict, table_name: str = "candidate_details", unique_key: str = "mobile_number"):
    # Read table columns (one-time or cache this)
    with engine.begin() as conn:
        cols = pd.read_sql(f"SELECT * FROM {table_name} LIMIT 1", conn).columns.tolist()
        # Filter incoming details to only DB columns
        payload = {k: v for k, v in details.items() if k in cols}
        if not payload:
            raise ValueError("No matching DB columns in payload.")
        insert_cols = ", ".join(payload.keys())
        insert_vals = ", ".join([f":{c}" for c in payload.keys()])
        update_clause = ", ".join([f"{c}=VALUES({c})" for c in payload.keys()])
        sql = f"""
            INSERT INTO {table_name} ({insert_cols})
            VALUES ({insert_vals})
            ON DUPLICATE KEY UPDATE {update_clause}
        """
        conn.execute(text(sql), payload)
