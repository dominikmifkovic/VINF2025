import json
import os
import lucene
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.store import MMapDirectory,  FSDirectory
from org.apache.lucene.index import (
    IndexWriter,
    IndexWriterConfig,
    DirectoryReader
)
from org.apache.lucene.search import IndexSearcher
from org.apache.lucene.document import (
    Document,
    StringField,
    TextField,
    Field
)
from java.nio.file import Paths



INPUT_DIR = "./merge_out"
INDEX_DIR = "./lucene_index"


def safe_add(doc, name, value, fieldtype):
    if value is None:
        return
    if isinstance(value, (list, tuple)):
        for v in value:
            if v:
                doc.add(fieldtype(name, str(v), Field.Store.YES))
    else:
        if value:
            doc.add(fieldtype(name, str(value), Field.Store.YES))


def build_big_text(rec):
    blocks = []

    #base description
    safe = rec.get("text")
    if safe:
        blocks.append(safe)

    #wiki blocks
    for f in ["wiki_lead", "wiki_history", "wiki_geography"]:
        if rec.get(f):
            blocks.append(rec[f])

    #decisions texts
    for dec in rec.get("decisions", []):
        if dec.get("text"):
            blocks.append(dec["text"])

    #SOC texts
    for soc in rec.get("state_of_conservation", []):
        if soc.get("text"):
            blocks.append(soc["text"])

    return "\n".join(blocks)


def create_document(rec):
    doc = Document()

    #ID fields
    safe_add(doc, "whs_id", rec.get("whs_id"), StringField)
    safe_add(doc, "property_id", rec.get("property_id"), StringField)
    safe_add(doc, "url", rec.get("url"), StringField)
    safe_add(doc, "wiki_link", rec.get("wiki_link"), StringField)

    #title variations
    safe_add(doc, "title", rec.get("title"), TextField)
    safe_add(doc, "wiki_title", rec.get("wiki_title"), TextField)
    safe_add(doc, "wiki_title_norm", rec.get("wiki_title_norm"), TextField)
    safe_add(doc, "norm_title", rec.get("norm_title"), TextField)

    #UNESCO info
    safe_add(doc, "criteria", rec.get("criteria"), StringField)
    safe_add(doc, "state_parties", rec.get("state_parties"), StringField)

    #coordinates
    safe_add(doc, "lat", rec.get("wiki_lat"), StringField)
    safe_add(doc, "lon", rec.get("wiki_lon"), StringField)

    #wiki extracted fields
    safe_add(doc, "wiki_lead", rec.get("wiki_lead"), TextField)
    safe_add(doc, "wiki_history", rec.get("wiki_history"), TextField)
    safe_add(doc, "wiki_geography", rec.get("wiki_geography"), TextField)
    safe_add(doc, "wiki_txt_aliases", rec.get("wiki_txt_aliases"), TextField)
    safe_add(doc, "wiki_txt_related_whs_titles", rec.get("wiki_txt_related_whs_titles"), TextField)

    #first year from wiki
    safe_add(doc, "wiki_txt_first_year", rec.get("wiki_txt_first_year"), StringField)

    #flags
    safe_add(doc, "wiki_txt_mentions_endangered", rec.get("wiki_txt_mentions_endangered"), StringField)

    #decisions
    for dec in rec.get("decisions", []):
        safe_add(doc, "decision_text", dec.get("text"), TextField)
        safe_add(doc, "decision_code", dec.get("decision_code"), StringField)
        safe_add(doc, "decision_themes", dec.get("themes"), TextField)

    #SOC
    for soc in rec.get("state_of_conservation", []):
        safe_add(doc, "soc_text", soc.get("text"), TextField)
        safe_add(doc, "soc_year", soc.get("year"), StringField)
        safe_add(doc, "soc_summary", soc.get("summary"), TextField)

    #searchable combined text field
    bigtext = build_big_text(rec)
    safe_add(doc, "fulltext", bigtext, TextField)

    return doc


def main():
    lucene.initVM()


    if not os.path.exists(INDEX_DIR):
        os.makedirs(INDEX_DIR)

    directory = MMapDirectory(Paths.get(INDEX_DIR))
    analyzer = StandardAnalyzer()
    config = IndexWriterConfig(analyzer)
    writer = IndexWriter(directory, config)

    #iterate over ALL files in merge_out
    for filename in os.listdir(INPUT_DIR):
        if not filename.endswith(".json"):
            continue

        path = os.path.join(INPUT_DIR, filename)
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                rec = json.loads(line)
                doc = create_document(rec)
                writer.addDocument(doc)

    writer.commit()
    writer.close()
    print("Indexing complete.")


if __name__ == "__main__":
    main()
