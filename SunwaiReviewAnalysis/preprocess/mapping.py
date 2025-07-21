import ast
import logging 
from collections import defaultdict
import pandas as pd
# Fix parsing of lists in 'aspects' and 'opinions' columns
def safe_parse_list(x):
    if isinstance(x, str):
        try:
            return ast.literal_eval(x)
        except Exception:
            return []
    elif isinstance(x, list):
        return x
    else:
        return []

# Define bank-app-specific category mapping (replacing generic or unrelated categories)
bank_category_map = {
    "LAPTOP#GENERAL": "APP#GENERAL_EXPERIENCE",
    "SUPPORT#GENERAL": "CUSTOMER_SUPPORT",
    "SOFTWARE#GENERAL": "APP#FEATURES",
    "SERVICE#GENERAL": "BRANCH_SERVICE",
    "SOFTWARE#OPERATION_PERFORMANCE": "APP#PERFORMANCE",
    "LAPTOP#OPERATION_PERFORMANCE": "DEVICE#PERFORMANCE",
    "LAPTOP#USABILITY": "APP#USABILITY",
    "OS#OPERATION_PERFORMANCE": "SYSTEM#PERFORMANCE",
    "HARDWARE#OPERATION_PERFORMANCE": "BIOMETRIC#PERFORMANCE",
    "HARDWARE#GENERAL": "BIOMETRIC#GENERAL",
    "POWER_SUPPLY#GENERAL": "UTILITIES#POWER_ISSUE",
    "RESTAURANT#GENERAL": "LOCATION_FEEDBACK_MISC",
    "KEYBOARD#OPERATION_PERFORMANCE": "BIOMETRIC#LOGIN_ISSUES",
    "LAPTOP#DESIGN_FEATURES": "UI_UX#DESIGN",
    "SINCE#GENERAL": "ATM_SERVICE",
    "SOFTWARE#USABILITY": "APP#USABILITY_FEATURES",
    "AMBIENCE#GENERAL": "ENVIRONMENT_FEEDBACK",
    "COSTS#GENERAL": "FEES_CHARGES",
    "COMPANY#GENERAL": "BANK_REPUTATION",
    "OS#GENERAL": "SYSTEM_UI",
    "SUPPORT#USABILITY": "CARD_SETTINGS_SUPPORT",
    "SERIES#GENERAL": "SERIES_FEEDBACK_MISC",
    "POWER_SUPPLY#OPERATION_PERFORMANCE": "UTILITIES#PERFORMANCE",
    "SHIPPING#GENERAL": "DELIVERY_RELATED_FEEDBACK",
    "DISPLAY#GENERAL": "SCREEN#GENERAL_FEEDBACK",
    "FOOD#QUALITY": "MISC#APP_FEEDBACK",
    "SINCE#OPERATION_PERFORMANCE": "TRANSACTION_SPEED",
    "SMS#OPERATION_PERFORMANCE": "EMAIL_SMS_SERVICE",
    "LAPTOP#PRICE": "APP#COST_FEEDBACK",
    "CURRENCY#OPERATION_PERFORMANCE": "BALANCE#FUNCTIONALITY",
    "SUPPORT#OPERATION_PERFORMANCE": "SUPPORT#PERFORMANCE",
    "Font#OPERATION_PERFORMANCE": "UI_FONT#ISSUE",
    "OS#DESIGN_FEATURES": "SYSTEM#DESIGN_FEATURES",
    "LOCATION#OPERATION_PERFORMANCE": "LOCATION_FEATURES",
    "KEYBOARD#GENERAL": "LOGIN_SECURITY_ISSUES",
    "LOCATION#GENERAL": "LIGHTING_OR_LOCATION_ISSUE",
    "DISPLAY#USABILITY": "APP#TABLET_VIEW_ISSUE",
    "SHIPPING#OPERATION_PERFORMANCE": "PAYMENT#STUCK",
    "SOFTWARE#DESIGN_FEATURES": "APP#DESIGN_FEEDBACK",
    "PAYMENT#GENERAL": "TAX_CHARGES_FEEDBACK",
    "LAPTOP#QUALITY": "APP#QUALITY_FEEDBACK",
    "KEYBOARD#DESIGN_FEATURES": "PASSWORD#FORMAT_ISSUES",
    "SUPPORT#QUALITY": "SUPPORT#RESPONSE",
    "COMplaint#OPERATION_PERFORMANCE": "COMPLAINT_PORTAL#FUNCTIONALITY",
    "MULTIMEDIA_DEVICES#OPERATION_PERFORMANCE": "CAMERA#PERFORMANCE",
    "CNIC#GENERAL": "CNIC#RELATED_FEEDBACK",
    "DRINKS#OPERATION_PERFORMANCE": "ACCOUNT_RECOVERY#ISSUES",
    "OS#USABILITY": "UI_COLOR#USABILITY",
    "DISPLAY#OPERATION_PERFORMANCE": "SCREEN#ISSUES",
    "SMS#GENERAL": "SMS#OTP_ISSUE",
    "CURRENCY#GENERAL": "DIGITAL_CURRENCY_FEEDBACK",
    "": "UNCLASSIFIED_ISSUES",
    "SOFTWARE#PRICE": "APP#PRICING_FEEDBACK",
    "CHARTER#GENERAL": "FEES_POLICY_COMPLAINTS",
}

def map_categories(raw_category_list):
    return [bank_category_map.get(cat.strip(), "Miscellaneous") for cat in raw_category_list]

def parse_and_map(df):
    df["categories"] = df["categories"].apply(safe_parse_list)
    df["mapped_categories"] = df["categories"].apply(map_categories)
    return df

def group_reviews_by_app_category_sentiment(df):
    logging.info("📊 Grouping reviews by app, category, and sentiment...")
    grouped_data = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: {
        "reviews": [],
        "aspects": [],
        "opinions": []
    })))

    for _, row in df.iterrows():
        app = str(row.get("app", "")).strip()
        content = str(row.get("content", "")).strip()
        if len(content.split()) <= 1:
            continue

        try:
            categories = ast.literal_eval(str(row.get("mapped_categories", [])))
            sentiments = ast.literal_eval(str(row.get("sentiments", [])))
            aspects = ast.literal_eval(str(row.get("aspects", [])))
            opinions = ast.literal_eval(str(row.get("opinions", [])))
        except Exception as e:
            logging.warning(f"Skipping row due to eval error: {e}")
            continue

        for i in range(min(len(categories), len(sentiments))):
            cat = categories[i]
            sent = sentiments[i]
            asp = aspects[i] if i < len(aspects) else "NULL"
            opn = opinions[i] if i < len(opinions) else "NULL"

            grouped_entry = grouped_data[app][cat][sent]
            grouped_entry["reviews"].append(content)
            grouped_entry["aspects"].append(asp)
            grouped_entry["opinions"].append(opn)

    records = []
    for app, cat_dict in grouped_data.items():
        for cat, sent_dict in cat_dict.items():
            for sentiment, data in sent_dict.items():
                records.append({
                    "app": app,
                    "category": cat,
                    "sentiment": sentiment,
                    "reviews": data["reviews"],
                    "aspects": data["aspects"],
                    "opinions": data["opinions"]
                })

    grouped_df = pd.DataFrame(records)
    logging.info("✅ Grouped DataFrame created.")
    return grouped_df


# def group_reviews_by_app_category_sentiment(df):
#     logging.info("📊 Grouping reviews by app, category, and sentiment...")
#     grouped_reviews = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

#     for _, row in df.iterrows():
#         app = str(row.get("app", "")).strip()
#         content = str(row.get("content", "")).strip()
#         if len(content.split()) <= 1:
#             continue

#         try:
#             categories = ast.literal_eval(str(row.get("mapped_categories", [])))
#             sentiments = ast.literal_eval(str(row.get("sentiments", [])))
#         except Exception as e:
#             logging.warning(f"Skipping row due to eval error: {e}")
#             continue

#         for cat, sent in zip(categories, sentiments):
#             grouped_reviews[app][cat][sent].append(content)

#     records = []
#     for app, cat_dict in grouped_reviews.items():
#         for cat, sent_dict in cat_dict.items():
#             for sentiment, reviews in sent_dict.items():
#                 records.append({
#                     "app": app,
#                     "category": cat,
#                     "sentiment": sentiment,
#                     "reviews": reviews
#                 })

#     grouped_df = pd.DataFrame(records)
#     logging.info("✅ Grouped DataFrame created.")
#     return grouped_df
