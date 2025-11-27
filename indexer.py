#
# Dominik Mifkovič 2025
#
import json
import re
import unicodedata
from collections import defaultdict, Counter

INPUT_FILE = "pages.jsonl"
INDEX_FILE = "index.jsonl"
DOC_STATS_FILE = "docs_meta.json"

#regex pre tokenizaciu, berie aj cisla
token_pattern = re.compile(r"[^\W_]+", re.UNICODE)

def normalize_text(text: str) -> str:
    if text is None:
        return ""
    text = unicodedata.normalize("NFKD", str(text))
    text = "".join(c for c in text if not unicodedata.combining(c))
    return text.lower().strip()

def tokenize(text: str):
    text = normalize_text(text)
    return token_pattern.findall(text)

#rekurzivne zozbiera vsetky hodnoty
def gather_values(x):
    if x is None:
        return []
    if isinstance(x, (str, int, float)):
        return [str(x)]
    if isinstance(x, dict):
        out = []
        for v in x.values():
            out.extend(gather_values(v))
        return out
    if isinstance(x, (list, tuple, set)):
        out = []
        for v in x:
            out.extend(gather_values(v))
        return out
    return []

#vytvori invertovany index
def build_index():
    index = defaultdict(list)   #token -> list[(doc_id, tf)]
    global_tf = Counter()       #token -> total tf
    doc_meta = {}               #doc_id -> {url, title, type}
    doc_lengths = {}            #doc_id -> token count
    total_docs = 0

    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            doc = json.loads(line)
            total_docs += 1
            doc_id = total_docs

            pieces = gather_values(doc)
            combined = " ".join(pieces)
            tokens = tokenize(combined)
            if not tokens:
                continue

            counts = Counter(tokens)
            for token, tf in counts.items():
                index[token].append((doc_id, tf))
                global_tf[token] += tf

            doc_meta[doc_id] = {
                "url": doc.get("url", ""),
                "title": doc.get("title", ""),
                "type": doc.get("type", "")
            }
            doc_lengths[doc_id] = sum(counts.values())

            if total_docs % 200 == 0:
                print(f"\rIndexed {total_docs} docs...", end="", flush=True)

    print(f"\nTotal indexed: {total_docs}")
    print(f"Unique tokens: {len(index)}")

    with open(INDEX_FILE, "w", encoding="utf-8") as out:
        for token, postings in index.items():
            out.write(json.dumps({
                "token": token,
                "postings": postings,  #[(doc_id, tf)]
                "global_tf": global_tf[token]
            }, ensure_ascii=False) + "\n")

    with open(DOC_STATS_FILE, "w", encoding="utf-8") as meta_out:
        json.dump({
            "total_docs": total_docs,
            "doc_lengths": doc_lengths,
            "docs": doc_meta
        }, meta_out, ensure_ascii=False, indent=2)

    print("Done.")

if __name__ == "__main__":
    build_index()
