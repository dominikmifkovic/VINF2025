import re
import lucene
from org.apache.lucene.store import MMapDirectory
from java.lang import String
from java.nio.file import Paths
from org.apache.lucene.search import (
    IndexSearcher,
    BooleanQuery, BooleanClause,
    TermQuery, PrefixQuery,
    WildcardQuery, PhraseQuery,
    FuzzyQuery, TermRangeQuery, BooleanClause
)
from org.apache.lucene.search.highlight import (
    Highlighter,
    QueryScorer,
    SimpleHTMLFormatter,
    TokenSources
)
from org.apache.lucene.index import DirectoryReader, Term
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.queryparser.classic import MultiFieldQueryParser
from org.apache.lucene.util import BytesRef
from org.apache.lucene.queryparser.classic import QueryParser, QueryParserBase
from rich.console import Console
from rich.panel import Panel
from rich.text import Text

console = Console()

INDEX_DIR = "lucene_index"

FIELDS = [
    "title", "site_name", "wiki_title",
    "wiki_lead", "wiki_history", "wiki_geography",
    "decision_text", "soc_text", "soc_summary",
    "fulltext"
]

SEARCH_FIELDS = (
    ["title"] * 5 +
    ["site_name"] * 5 +
    ["wiki_title"] * 3 +
    ["wiki_lead"] * 2 +
    ["fulltext"] * 1
)

FIELD_MAP = {
    "lead": "wiki_lead",
    "wiki_lead": "wiki_lead",
    "history": "wiki_history",
    "hist": "wiki_history",
    "wiki_history": "wiki_history",
    "geography": "wiki_geography",
    "geo": "wiki_geography",
    "wiki_geography": "wiki_geography",

    "aliases": "wiki_txt_aliases",
    "related": "wiki_txt_related_whs_titles",

    "decision": "decision_text",
    "decisions": "decision_text",
    "themes": "decision_themes",
    "code": "decision_code",

    "soc": "soc_text",
    "summary": "soc_summary",
    "year": "soc_year",

    "criteria": "criteria",
    "state": "state_parties",
    "states": "state_parties",

    "lat": "lat",
    "lon": "lon",
    "endangered": "wiki_txt_mentions_endangered",

    "text": "fulltext",
    "fulltext": "fulltext",
    "all": "fulltext",
}


def open_searcher():
    directory = MMapDirectory(Paths.get(INDEX_DIR))
    reader = DirectoryReader.open(directory)
    searcher = IndexSearcher(reader)
    return searcher, reader


def parse_user_query(q, parser):
    q = q.strip()
    m = re.match(r"(\w+):(\d+)-(\d+)$", q)
    if m:
        field_raw, start, end = m.groups()
        key = field_raw.lower()
        field = FIELD_MAP.get(key, field_raw)
        return TermRangeQuery.newStringRange(
            field,
            BytesRef(start),
            BytesRef(end),
            True,
            True
        )
    return parser.parse(q)


def run_query(qobj):
    searcher, reader = open_searcher()
    top = searcher.search(qobj, 20)

    print(f"\nFound {top.totalHits.value} results:\n")
    for hit in top.scoreDocs:
        doc = searcher.storedFields().document(hit.doc)
        print(f"[{hit.score:.3f}] {doc.get('title')}")
    reader.close()


GREEN = "\033[92m"
RESET = "\033[0m"
BOLD = "\033[1m"


def main():
    lucene.initVM()

    directory = MMapDirectory(Paths.get(INDEX_DIR))
    reader = DirectoryReader.open(directory)
    searcher = IndexSearcher(reader)
    analyzer = StandardAnalyzer()

    while True:
        try:
            q_raw = input("\nquery> ").strip()
        except EOFError:
            break

        if not q_raw:
            continue

        words = q_raw.split()
        words_l = [w.lower() for w in words]
        modes = []
        for w in words_l:
            if w in FIELD_MAP:
                modes.append(FIELD_MAP[w])

        main_tokens = [w for w in words if w.lower() not in FIELD_MAP]
        main_query_text = " ".join(main_tokens).strip()
        if not main_query_text:
            main_query_text = q_raw

        try:
            flags = [BooleanClause.Occur.SHOULD] * len(SEARCH_FIELDS)
            query = MultiFieldQueryParser.parse(
                [main_query_text] * len(SEARCH_FIELDS),
                SEARCH_FIELDS,
                flags,
                analyzer
            )

            hits = searcher.search(query, 10)
            total = hits.totalHits.value()

            print(f"\nFound {total} results:\n")

            #deduplication
            modes_clean = []
            seen = set()
            for m in modes:
                if m not in seen:
                    modes_clean.append(m)
                    seen.add(m)

            stored_fields = searcher.storedFields()

            for sd in hits.scoreDocs:
                doc_id = sd.doc
                doc = stored_fields.document(doc_id)

                title = (
                    doc.get("site_name") or
                    doc.get("title") or
                    doc.get("wiki_title") or
                    "<no title>"
                )

                wiki_link = doc.get("wiki_link") or ""
                unesco_link = doc.get("url") or ""

                links_header = ""
                if wiki_link:
                    links_header += f"[bold blue]WIKI[/bold blue]: {wiki_link}\n"
                if unesco_link:
                    links_header += f"[bold green]UNESCO[/bold green]: {unesco_link}\n"

                header = Text()
                header.append(title, style="bold red")
                header.append(f"  (score={sd.score:.2f})", style="bold yellow")

                body = ""

                if links_header:
                    body += links_header + "\n"

                if modes_clean:
                    for field in modes_clean:
                        val = doc.get(field)
                        if not val:
                            continue
                        out = val[:300] + ("..." if len(val) > 300 else "")
                        body += f"\n[bold]{field.upper()}[/bold]:\n{out}\n"

                console.print(Panel(body.rstrip(), title=header, expand=True))

        except Exception as e:
            print("ERROR:", e)

    reader.close()


if __name__ == "__main__":
    main()
