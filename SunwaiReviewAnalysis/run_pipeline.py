from . import config
from .datastore.loader import DataLoader
from .models.ABSA import QuadrupleExtractor
from .models.extractor import extract_quadruples
from .datastore.saver import save_to_csv, upload_to_supabase, get_existing_reviews, save_summary_to_supabase
from .preprocess.mapping import parse_and_map, group_reviews_by_app_category_sentiment
from .models.summary_model import generate_summaries
import logging
from .datastore.fetch_playstore import fetch_new_reviews
from .preprocess.cleaning import clean_reviews_for_model
logging.basicConfig(
    level=logging.INFO,  # can use DEBUG, WARNING, ERROR
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class PipelineRunner:
    def __init__(self, input_path=config.INPUT_CSV, output_path=config.OUTPUT_CSV, model_name=config.MODEL_NAME):
        self.input_path = input_path
        self.output_path = output_path
        self.model_name = model_name
        self.df = None
        self.content = None
        self.model = None

    def fetch_and_prepare_reviews(self):
        logging.info("🌐 Fetching reviews from Play Store...")
        app_list = ["app.com.brd","com.ofss.digx.mobile.obdx.bahl",
                    "com.ofss.tx.meezan",
                    "com.sbp.sbp_complaints_management",
                    "com.hbl.android.hblmobilebanking"]
        rename_map = {
            "com.ofss.digx.mobile.obdx.bahl": "Al Habib",
            "com.ofss.tx.meezan":"Meezan Bank",
            "app.com.brd":"UBL Digital",
            "com.sbp.sbp_complaints_management":"Sunwai",
            "com.hbl.android.hblmobilebanking":"HBL",
        }

        df = fetch_new_reviews(app_list, reviews_per_app=2000)

        if df.empty:
            logging.info("📭 No new reviews fetched from Play Store.")
            return False

        df = clean_reviews_for_model(df)

        df["app"] = df["app"].map(rename_map).fillna(df["app"]) 

        df.to_csv(self.input_path, index=False)
        logging.info(f"✅ Cleaned data saved to {self.input_path}")
        return True
    
    def load_data(self): 
        logging.info("Loading Data...")
        self.df, self.content = DataLoader.load_data(self.input_path)
        self._filter_duplicates()  # 🆕 Add duplicate filtering

    def _filter_duplicates(self):
        logging.info("🔎 Checking for duplicate reviews in Supabase...")

        existing_reviews = get_existing_reviews()

        if not existing_reviews:
            logging.info("ℹ️ No existing reviews found in Supabase.")
            return

        existing_set = set()
        skipped = 0
        for r in existing_reviews:
            content = r.get("content")
            app = r.get("app")
            if content and app:
                existing_set.add((content.strip().lower(), app.strip().lower()))
            else:
                skipped += 1

        logging.info(f"⚠️ Skipped {skipped} invalid existing reviews (missing content or app)")

        original_count = len(self.df)

        self.df["content_lower"] = self.df["content"].str.strip().str.lower()
        self.df["app_lower"] = self.df["app"].str.strip().str.lower()

        self.df = self.df[
            ~self.df.apply(
                lambda row: (row["content_lower"], row["app_lower"]) in existing_set,
                axis=1,
            )
        ]
        filtered_count = original_count - len(self.df)
        logging.info(f"✅ Filtered {filtered_count} duplicate reviews.")

        self.df.drop(columns=["content_lower", "app_lower"], inplace=True)
        self.content = self.df["content"].tolist()

        
    def load_model(self):
        logging.info("loading Model...")
        self.model = QuadrupleExtractor(model_name=self.model_name)

    def extract(self):
        logging.info("🔍 Extracting quadruples...")
        aspects, opinions, sentiments, categories = extract_quadruples(self.content, self.model)
        logging.info("🧩 Adding columns...")
        self.df["aspects"] = aspects
        self.df["opinions"] = opinions
        self.df["sentiments"] = sentiments
        self.df["categories"] = categories
        logging.info("🧩 Mapping categories...")
        self.df = parse_and_map(self.df)
        if self.df.columns[0].lower() in ["unnamed: 0", "index"]:
             self.df = self.df.drop(self.df.columns[0], axis=1)

    
    def summarize(self):
        logging.info("📝 Grouping according to categories...")
        group_df = group_reviews_by_app_category_sentiment(self.df)
        logging.info("📝 Generating summaries...")
        summary_df = generate_summaries(group_df)
        if summary_df.columns[0].lower() in ["unnamed: 0", "index", ""]:
             summary_df = summary_df.drop(summary_df.columns[0], axis=1)
        summary_df.to_csv("summary_all.csv", index=False)
        logging.info("🚀 Uploading summaries to Supabase...")
        save_summary_to_supabase(summary_df)
        
        
    def save_reviews(self):
        logging.info("🚀 Uploading to Supabase: reviews table...")
        upload_to_supabase(self.df, "reviews") 
        save_to_csv(self.df , "all_predictied.csv")

    def run(self):
        logging.info("🚀 Starting the pipeline...")

        # 1. Fetch reviews from Play Store
        if not self.fetch_and_prepare_reviews():
            logging.info("🛑 No new reviews fetched. Exiting pipeline.")
            return

        # 2. Load data (now it loads freshly fetched CSV)
        self.load_data()

        # 3. Check if anything left after filtering duplicates
        if not self.content:
            logging.info("🛑 All reviews are already predicted. No new reviews to process.")
            return

        # 4. Run model + save + summarize
        self.load_model()
        self.extract()
        self.save_reviews()
        self.summarize()
