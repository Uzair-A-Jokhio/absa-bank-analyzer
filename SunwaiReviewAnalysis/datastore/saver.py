from .. import config
from supabase import create_client, Client
import os
from dotenv import load_dotenv
import logging 

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