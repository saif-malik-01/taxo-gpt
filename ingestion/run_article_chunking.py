import json
from chunk_articles import chunk_article

with open("data/processed/articles.json", "r", encoding="utf-8") as f:
    articles = json.load(f)

all_chunks = []

for article in articles:
    all_chunks.extend(chunk_article(article))

with open("data/processed/article_chunks.json", "w", encoding="utf-8") as f:
    json.dump(all_chunks, f, ensure_ascii=False, indent=2)

print(f"Article Chunks Created: {len(all_chunks)}")
