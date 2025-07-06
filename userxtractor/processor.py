# absa_quad_extractor/processor.py

def extract_quadruples(reviews, extractor):
    all_aspects, all_opinions, all_sentiments, all_categories = [], [], [], []

    for review in reviews:
        try:
            result = extractor.predict(review)
            quads = result.get("Quadruples", [])
            aspects = [q["aspect"] for q in quads]
            opinions = [q["opinion"] for q in quads]
            sentiments = [q["polarity"] for q in quads]
            categories = [q["category"] for q in quads]
        except Exception as e:
            print(f"Error: {e} for review: {review[:50]}...")
            aspects, opinions, sentiments, categories = [], [], [], []

        all_aspects.append(aspects)
        all_opinions.append(opinions)
        all_sentiments.append(sentiments)
        all_categories.append(categories)

    return all_aspects, all_opinions, all_sentiments, all_categories
