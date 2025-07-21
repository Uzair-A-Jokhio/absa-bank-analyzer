# from transformers import pipeline
# import pandas as pd
# import ast

# summarizer = pipeline("summarization", model="facebook/bart-large-cnn", device=0)

# def generate_summaries(df, num_sentences=5):
#     summaries = []
#     grouped = df.groupby(["app", "category", "sentiment"])

#     for (app, category, sentiment), group in grouped:
#         try:
#             reviews = group["reviews"].tolist()
#             flat_reviews = []

#             for r in reviews:
#                 if isinstance(r, str):
#                     try:
#                         parsed = ast.literal_eval(r) if r.startswith("[") else [r]
#                         flat_reviews.extend([str(s).strip() for s in parsed if str(s).strip()])
#                     except Exception as e:
#                         print(f"⚠️ Skipping malformed review: {r} — {e}")
#                 elif isinstance(r, list):
#                     flat_reviews.extend([str(s).strip() for s in r if str(s).strip()])

#             combined_text = " ".join(flat_reviews)
#             input_word_count = len(combined_text.split())
#             print(f"[Summary] ({app}, {category}, {sentiment}) → {input_word_count} words")

#             if input_word_count < 5:
#                 summary = combined_text
#             else:
#                 max_len = min(130, int(input_word_count * 1.3))
#                 min_len = min(30, max(10, int(max_len * 0.5)))
#                 result = summarizer(combined_text, max_length=max_len, min_length=min_len, do_sample=False)
#                 summary = result[0]["summary_text"]

#             summaries.append({
#                 "app": app,
#                 "category": category,
#                 "sentiment": sentiment,
#                 "summary": summary
#             })

#         except Exception as e:
#             print(f"⚠️ Error summarizing ({app}, {category}, {sentiment}): {e}")

#     return pd.DataFrame(summaries)
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
            aspects = group["aspects"].tolist()
            opinions = group["opinions"].tolist()

            flat_reviews, flat_aspects, flat_opinions = [], [], []

            for r in reviews:
                if isinstance(r, str):
                    try:
                        parsed = ast.literal_eval(r) if r.startswith("[") else [r]
                        flat_reviews.extend([str(s).strip() for s in parsed if str(s).strip()])
                    except Exception as e:
                        print(f"⚠️ Skipping malformed review: {r} — {e}")
                elif isinstance(r, list):
                    flat_reviews.extend([str(s).strip() for s in r if str(s).strip()])

            for a_list in aspects:
                try:
                    parsed = ast.literal_eval(a_list) if isinstance(a_list, str) else a_list
                    flat_aspects.extend([a for a in parsed if a != "NULL"])
                except:
                    pass

            for o_list in opinions:
                try:
                    parsed = ast.literal_eval(o_list) if isinstance(o_list, str) else o_list
                    flat_opinions.extend([o for o in parsed if o != "NULL"])
                except:
                    pass

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
                "summary": summary,
                "aspects": ", ".join(set(flat_aspects)),
                "opinions": ", ".join(set(flat_opinions))
            })

        except Exception as e:
            print(f"⚠️ Error summarizing ({app}, {category}, {sentiment}): {e}")

    return pd.DataFrame(summaries)
