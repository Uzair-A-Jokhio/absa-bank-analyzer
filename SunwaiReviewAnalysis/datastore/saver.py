from .. import config
from supabase import create_client, Client
import os
from dotenv import load_dotenv

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
