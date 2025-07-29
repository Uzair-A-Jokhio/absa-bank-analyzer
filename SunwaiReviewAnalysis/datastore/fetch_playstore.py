import os
import time
import random
import pandas as pd
from google_play_scraper import Sort, reviews
from .saver import get_existing_reviews
import logging

def fetch_new_reviews(app_package_names, reviews_per_app=100, output_dir='reviews_output'):
    os.makedirs(output_dir, exist_ok=True)

    logging.info("📥 Fetching new reviews from Play Store...")
    existing = get_existing_reviews()
    existing_set = set(
        (r["content"].strip().lower(), r["app"].strip().lower())
        for r in existing if r.get("content") and r.get("app")
    )

    combined_df = pd.DataFrame()
    grand_total_skipped = 0

    for app_package in app_package_names:
        logging.info(f"\n📱 Fetching reviews for: {app_package}")
        unique_reviews = []
        seen_this_batch = set()
        next_token = None
        skipped_this_app = 0

        while len(unique_reviews) < reviews_per_app:
            batch, next_token = reviews(
                app_package,
                lang='en',
                country='us',
                sort=Sort.NEWEST,
                count=100,
                continuation_token=next_token
            )

            if not batch:
                logging.warning("⚠️ No more reviews available from Play Store.")
                break

            for review in batch:
                content = review.get("content", "").strip().lower()
                app = app_package.strip().lower()

                if not content:
                    continue

                key = (content, app)
                if key not in existing_set and key not in seen_this_batch:
                    review["app"] = app_package
                    unique_reviews.append(review)
                    seen_this_batch.add(key)
                else:
                    skipped_this_app += 1
                    grand_total_skipped += 1

                if len(unique_reviews) >= reviews_per_app:
                    break

            if not next_token:
                logging.warning("📭 Reached end of available reviews.")
                break

            time.sleep(random.uniform(1.5, 3.0))

        if not unique_reviews:
            logging.info(f"❌ No new unique reviews found for {app_package}")
            continue

        df = pd.DataFrame(unique_reviews[:reviews_per_app])  # Truncate if more
        df["content"] = df["content"].astype(str).str.strip()
        df = df[["content", "score", "app"]].copy()

        logging.info(f"✅ {len(df)} new reviews fetched for {app_package}")
        logging.info(f"🗑️ {skipped_this_app} duplicates skipped for {app_package}")

        combined_df = pd.concat([combined_df, df], ignore_index=True)

    logging.info(f"\n🎯 Total duplicate reviews skipped across all apps: {grand_total_skipped}")
    return combined_df
