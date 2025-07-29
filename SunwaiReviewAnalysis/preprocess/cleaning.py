import logging 

def clean_reviews_for_model(df):
    logging.info("🧹 Cleaning review data for prediction...")
    df.dropna(subset=["content"], inplace=True)
    df["content"] = df["content"].astype(str).str.strip()
    df = df[df["content"].str.split().str.len() > 2]  # Remove very short reviews
    df.reset_index(drop=True, inplace=True)
    return df[["content", "app"]]
