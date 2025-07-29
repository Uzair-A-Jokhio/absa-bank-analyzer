from .. import config
from supabase import create_client, Client
import os
from dotenv import load_dotenv
from datetime import datetime
import logging 
import numpy as np

def save_to_csv(df, output_path):
    df.to_csv(output_path, index=False)
    print(f"✅ Saved with separated columns at: {output_path}")

# Load environment variables
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def upload_to_supabase(df, table_name: str):
    """
    Uploads the DataFrame to a Supabase table.

    Args:
        df (pd.DataFrame): DataFrame to upload
        table_name (str): Supabase table name
    """
    records = df.to_dict(orient='records')
    for record in records:
        try:
            supabase.table(table_name).insert(record).execute()
        except Exception as e:
            print(f"❌ Failed to insert record: {record}")
            print(e)

def get_existing_reviews(table_name="reviews"):
    try:
        logging.info("📥 Fetching existing reviews from Supabase...")
        response = supabase.table("reviews").select("content,app").execute()
        existing = response.data if response.data else []
        logging.info(f"📊 Fetched {len(existing)} existing reviews from Supabase.")
        return existing
    except Exception as e:
        print("❌ Failed to fetch existing reviews and apps:", e)
        return set()
    

def save_summary_to_supabase(summary_df):
    if summary_df.empty:
        print("⚠️ No summary data to save.")
        return


    required_fields = ["app", "category", "sentiment", "summary", "aspects", "opinions"]
    missing = [col for col in required_fields if col not in summary_df.columns]
    if missing:
        print(f"❌ Missing required columns in summary_df: {missing}")
        return

    # Add updated_at
    summary_df["updated_at"] = datetime.utcnow().isoformat()

    # Clean invalid values
    summary_df.replace([np.inf, -np.inf], np.nan, inplace=True)
    summary_df.dropna(subset=required_fields, inplace=True)

    # Select only required fields
    summary_df = summary_df[required_fields + ["updated_at"]]

    # Convert to list of dicts
    data = summary_df.to_dict(orient="records")

    try:
        supabase.table("summaries").upsert(
            data,
            on_conflict="app,category,sentiment"
        ).execute()
        print(f"✅ Upserted {len(data)} summary records to Supabase.")
    except Exception as e:
        print(f"❌ Failed to upsert summary data:\n{e}")


