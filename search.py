#
# Dominik Mifkovič 2025
#
import json
import math
import unicodedata
import re

INDEX_FILE = "index.jsonl"
DOC_STATS_FILE = "docs_meta.json"
DOC_SOURCE_FILE = "pages.jsonl"

index = {}
n_docs = 0
docs = {}
ready = False

#regex pre tokenizaciu, berie aj cisla
token_pattern = re.compile(r"[^\W_]+", re.UNICODE)

def normalize_text(text):
    text = unicodedata.normalize("NFKD", text)
    text = "".join([c for c in text if not unicodedata.combining(c)])
    return text.lower()

def tokenize(text):
    return token_pattern.findall(normalize_text(text))

#klasicke IDF
def idf_classic(df, N):
    return math.log((N + 1) / (df + 1))

#pravdepodobnostne IDF
def idf_probabilistic(df, N):
    if df == 0 or df == N:
        return 0.0
    return max(0.0, math.log((N - df) / df))

#prednahratie indexu do pamate
def preload():
    global index, n_docs, docs, ready

    with open(INDEX_FILE, "r", encoding="utf-8") as f:
        for line in f:
            item = json.loads(line)
            index[item["token"]] = item["postings"]

    with open(DOC_STATS_FILE, "r", encoding="utf-8") as f:
        meta = json.load(f)
    n_docs = int(meta["total_docs"])

    with open(DOC_SOURCE_FILE, "r", encoding="utf-8") as f:
        for i, line in enumerate(f, start=1):
            d = json.loads(line)
            docs[i] = {"url": d.get("url", ""), "title": d.get("title", "")}

    ready = True

#hladanie v indexe
def search(query, idf_mode="classic", top_k=10):
    if not ready:
        preload()

    tokens = tokenize(query)
    if not tokens:
        return []

    if idf_mode == "classic":
        idf_func = idf_classic
    elif idf_mode == "prob":
        idf_func = idf_probabilistic
    else:
        raise ValueError("Unknown idf_mode")

    #nacitame postings pre kazdy token
    token_postings = []
    for token in tokens:
        postings = index.get(token)
        if not postings:
            return []  #ak jeden token nema ziadne dokumenty, vratime prazdny vysledok
        token_postings.append(dict(postings))  #dict kvoli rychlemu lookupu

    #prienik dokumentov obsahujucich vsetky tokeny
    common_docs = set(token_postings[0].keys())
    for p in token_postings[1:]:
        common_docs &= set(p.keys())

    if not common_docs:
        return []

    scores = {}
    for doc_id in common_docs:
        score = 0.0
        for p in token_postings:
            tf = p[doc_id]
            df = len(p)
            idf = idf_func(df, n_docs)
            score += (1 + math.log(tf)) * idf
        scores[doc_id] = score

    ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    results = []
    for doc_id, score in ranked[:top_k]:
        doc = docs.get(doc_id, {})
        results.append({
            "title": doc.get("title", ""),
            "url": doc.get("url", ""),
            "score": round(score, 4)
        })
    return results

if __name__ == "__main__":
    preload()
    while True:
        query = input("Query: ").strip()
        if not query:
            break
        for mode in ["classic", "prob"]:
            print(f"\nResults ({mode})")
            results = search(query, idf_mode=mode)
            if not results:
                print("No results.")
            for r in results:
                print(f"[{r['score']:.4f}] {r['url']} — {r['title']}")
