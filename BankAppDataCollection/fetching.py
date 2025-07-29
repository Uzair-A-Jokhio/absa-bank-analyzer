from google_play_scraper import Sort, reviews
import pandas as pd
import time
import random
import os

# List of app package names
app_package_names = [
        
    "com.ofss.digx.mobile.obdx.bahl"  
]

# Directory to save results
os.makedirs('reviews_output', exist_ok=True)

# Max number of reviews to fetch per app
MAX_REVIEWS = 400

for app_package_name in app_package_names:
    print(f"\n📱 Fetching reviews for: {app_package_name}")
    all_reviews = []
    next_token = None
    total_fetched = 0

    while True:
        review_batch, next_token = reviews(
            app_package_name,
            lang='en',
            country='us',
            sort=Sort.NEWEST,
            count=100,
            continuation_token=next_token
        )

        if not review_batch:
            print("No more new reviews. Stopping.")
            break

        all_reviews.extend(review_batch)

        # Trim if over limit
        if len(all_reviews) >= MAX_REVIEWS:
            all_reviews = all_reviews[:MAX_REVIEWS]
            print(f"✅ Reached limit of {MAX_REVIEWS} reviews.")
            break

        new_total = len(all_reviews)
        print(f"Fetched {new_total} reviews so far...")

        if new_total == total_fetched:
            print("No new reviews added. Likely end of dataset.")
            break

        total_fetched = new_total

        if not next_token:
            print("Reached the end. No more continuation token.")
            break

        time.sleep(random.uniform(1.5, 3.0))  # avoid being blocked

    # Save to CSV
    app_name_safe = app_package_name.replace('.', '_')
    df = pd.DataFrame(all_reviews)
    file_path = f"reviews_output/{app_name_safe}_reviews.csv"
    df.to_csv(file_path, index=False)
    print(f"✅ Saved {len(df)} reviews to {file_path}")

print("\n🎉 All apps processed!")
