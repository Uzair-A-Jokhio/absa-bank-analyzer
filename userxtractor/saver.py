# absa_quad_extractor/saver.py
import pandas as pd

def save_to_csv(df, output_path):
    df.to_csv(output_path, index=False)
    print(f"✅ Saved with separated columns at: {output_path}")
