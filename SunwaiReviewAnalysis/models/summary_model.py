from transformers import pipeline
import pandas as pd
import ast

summarizer = pipeline("summarization", model="facebook/bart-large-cnn", device=0)

def generate_summaries(df, num_sentences=5):
    summaries = []
    grouped = df.groupby(["app", "category", "sentiment"])

    for (app, category, sentiment), group in grouped:
        try:
            reviews = group["reviews"].tolist()
            flat_reviews = []

            for r in reviews:
                if isinstance(r, str):
                    try:
                        parsed = ast.literal_eval(r) if r.startswith("[") else [r]
                        flat_reviews.extend([str(s).strip() for s in parsed if str(s).strip()])
                    except Exception as e:
                        print(f"⚠️ Skipping malformed review: {r} — {e}")
                elif isinstance(r, list):
                    flat_reviews.extend([str(s).strip() for s in r if str(s).strip()])

            combined_text = " ".join(flat_reviews)
            input_word_count = len(combined_text.split())
            print(f"[Summary] ({app}, {category}, {sentiment}) → {input_word_count} words")

            if input_word_count < 5:
                summary = combined_text
            else:
                max_len = min(130, int(input_word_count * 1.3))
                min_len = min(30, max(10, int(max_len * 0.5)))
                result = summarizer(combined_text, max_length=max_len, min_length=min_len, do_sample=False)
                summary = result[0]["summary_text"]

            summaries.append({
                "app": app,
                "category": category,
                "sentiment": sentiment,
                "summary": summary
            })

        except Exception as e:
            print(f"⚠️ Error summarizing ({app}, {category}, {sentiment}): {e}")

    return pd.DataFrame(summaries)
