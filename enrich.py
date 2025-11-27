import re
import urllib.parse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType, LongType,
    IntegerType, ArrayType, MapType
)
from pyspark.sql.window import Window

WIKI_XML   = "enwiki-latest-pages-articles.xml"
PAGES_JSON = "pages.jsonl"
OUT_DIR    = "./join_out"

RE_TITLE = re.compile(r"<title>(.*?)</title>", re.DOTALL)
RE_TEXT = re.compile(r"<text[^>]*>([\s\S]*?)</text>", re.DOTALL)
RE_REDIRECT = re.compile(r"<redirect\b", re.IGNORECASE)

RE_INFOBOX_ANY = re.compile(
    r"\{\{\s*Infobox\s+(?:UNESCO\s+)?World\s+Heritage\s+Site"
    r"|\{\{\s*Infobox\s*WHS"
    r"|\{\{\s*Infobox\s*protected\s*area",
    re.IGNORECASE
)

RE_WHS_ID_PARAM = re.compile(r"\|\s*(?:id|reference(?:_number|_no|\s*no)?)\s*=\s*([0-9]{1,7})\b", re.IGNORECASE)
RE_UNESCO_LINK = re.compile(r"(?:https?:\/\/)?whc\.unesco\.org\/en\/list\/(\d{1,7})", re.IGNORECASE)

RE_CRITERIA_PARSE = re.compile(r"\b(i|ii|iii|iv|v|vi|vii|viii|ix|x)\b", re.IGNORECASE)

RE_COORD = re.compile(r"\{\{\s*coord\|([^}]+)\}\}", re.IGNORECASE)

RE_SECTION = re.compile(r"^==\s*(.*?)\s*==\s*$", re.MULTILINE | re.IGNORECASE)

RE_TXT_FIRST_YEAR = re.compile(r"\b(1[0-9]{3}|20[0-9]{2})\b")
RE_TXT_ENDANGERED = re.compile(r"\bendangered\b", re.IGNORECASE)

RE_WIKI_LINK = re.compile(r"\[\[([^\]|#]+)(?:\|([^\]]+))?\]\]")

RE_WORLD_HERITAGE = re.compile(r"world\s+heritage", re.IGNORECASE)

RE_ALIAS_PHRASE   = re.compile(
    r"\b(?:also known as|formerly|sometimes called|locally known as)\s+([^.;\n]+)",
    re.IGNORECASE
)

INCLUDE_TITLE_PAT = re.compile(
    r"(World\s+Heritage|National\s+Park|Historic\s+(?:Centre|Center|Monuments?)|"
    r"Cultural\s+Landscape|Cathedral|Basilica|Abbey|Monastery|Mosque|Temple|Church|"
    r"Palace|Fort|Castle|Citadel|Archaeological|Old\s+Town|Sanctuary|Shrine|"
    r"Pyramids?|Valley|Terraces|Grottoes|Pagoda|Tombs?|Necropolis|Aqueduct|"
    r"City\s+Walls?|Historic\s+Quarter|Historic\s+City)",
    re.IGNORECASE
)

EXCLUDE_TITLE_PAT = re.compile(
    r"^(?:(?:UNESCO\s+)?World\s+Heritage(?:\s+Site|(?:\s+Committee)?|(?:\s+List)?)|"
    r"Lists?\s+of\s+.*|Tentative\s+List|Category:.*|Template:.*|Help:.*|Portal:.*|"
    r"File:.*|Image:.*)$",
    re.IGNORECASE
)

EXCLUDE_KEYWORDS = re.compile(
    r"(species|forest|rainforest|river|lake|continent|sea|gulf|bay|peninsula|"
    r"tropical|temperate|boreal|glacial|max(?:imum)?|storm|biology|conservation|"
    r"logging|habitat|ecosystem|tiger|leopard|bear|wolf|deer|boar|owl|salmon|"
    r"oak|pine|spruce|fir|birch|province|krai|oblast|federation|republic|state|"
    r"north|south|east|west|central|region)",
    re.IGNORECASE
)

def norm_title(t):
    return t.strip().replace(" ", "_") if t else None

def normalize_for_join(t):
    if not t:
        return None
    return re.sub(r"[\s_]+", "_", t.strip().lower())

def _normalize_title(s):
    return s.strip().replace("_", " ").split("#", 1)[0].strip()

def _to_int(x):
    try:
        return int(x) if x else None
    except:
        return None

def clean_markup(text):
    if not text:
        return ""
    t = text
    t = re.sub(r"\{\{[^\}]*\}\}", "", t)
    t = re.sub(r"\[\[[^\]\|]*\|([^\]]+)\]\]", r"\1", t)
    t = re.sub(r"\[\[|\]\]", "", t)
    t = re.sub(r"<ref[^>]*>.*?</ref>", "", t, flags=re.DOTALL)
    t = re.sub(r"<ref[^/>]*/>", "", t)
    t = re.sub(r"'{2,}", "", t)
    return t.strip()

def clean_infobox_value(v):
    if not v:
        return ""

    t = v

    #remove references <ref>...</ref> / <ref />
    t = re.sub(r"<ref[^>]*>.*?</ref>", "", t, flags=re.DOTALL)
    t = re.sub(r"<ref[^/>]*/>", "", t)

    #resolve URL templates {{URL|http://...}}
    t = re.sub(r"\{\{\s*URL\|([^}]+)\}\}", r"\1", t, flags=re.IGNORECASE)

    #resolve convert templates: {{convert|3.01|ha|acre|abbr=on}}
    def _convert_sub(m):
        parts = [x.strip() for x in m.group(1).split("|")]
        if len(parts) >= 2:
            return f"{parts[0]} {parts[1]}"
        return parts[0]

    t = re.sub(r"\{\{\s*convert\|([^}]+)\}\}", _convert_sub, t, flags=re.IGNORECASE)

    #remove remaining templates {{...}}
    t = re.sub(r"\{\{[^}]+\}\}", "", t)

    #wikilinks: [[A|B]] → B, [[A]] → A
    t = re.sub(r"\[\[([^|\]]*\|)?([^\]]+)\]\]", r"\2", t)

    #remove leftover pipes
    t = t.replace("|", " ")

    #collapse spaces
    t = re.sub(r"\s+", " ", t)

    return t.strip()

def extract_infobox_block(wikitext):
    m = RE_INFOBOX_ANY.search(wikitext)
    if not m:
        return None
    start = m.start()
    depth = 0
    i = start
    while i < len(wikitext):
        if wikitext.startswith("{{", i):
            depth += 1
            i += 2
            continue
        if wikitext.startswith("}}", i):
            depth -= 1
            i += 2
            if depth == 0:
                return wikitext[start:i]
            continue
        i += 1
    return None

def parse_infobox_fields(ibox):
    if not ibox:
        return {}

    s = ibox.strip()
    if s.startswith("{{"):
        s = s[2:]
    if s.endswith("}}"):
        s = s[:-2]

    if "|" in s:
        s = s.split("|", 1)[1]

    fields = {}
    i = 0
    length = len(s)

    depth_tpl = 0
    depth_link = 0
    key = None
    val = []

    def store():
        if key is None:
            return
        raw = "".join(val).strip()
        fields[key] = clean_infobox_value(raw)

    while i < length:
        if s.startswith("{{", i):
            depth_tpl += 1
            val.append("{{")
            i += 2
            continue
        if s.startswith("}}", i):
            if depth_tpl > 0:
                depth_tpl -= 1
            val.append("}}")
            i += 2
            continue

        if s.startswith("[[", i):
            depth_link += 1
            val.append("[[")
            i += 2
            continue
        if s.startswith("]]", i):
            if depth_link > 0:
                depth_link -= 1
            val.append("]]")
            i += 2
            continue

        if depth_tpl == 0 and depth_link == 0 and s[i] == "|":
            store()
            key = None
            val = []
            i += 1
            start = i
            while i < length and s[i] not in "=\n|":
                i += 1
            key_raw = s[start:i].strip().lower().replace(" ", "_")
            if i < length and s[i] == "=":
                i += 1
            key = key_raw
            continue

        val.append(s[i])
        i += 1

    store()
    return fields


#lead, history, geography
def clean_section_text(text):
    if not text:
        return None
    t = text
    t = re.sub(r"<ref[^>]*>.*?</ref>", "", t, flags=re.DOTALL)
    t = re.sub(r"<ref[^/>]*/>", "", t)
    t = re.sub(r"^\s*\{\{[^\}]+\}\}\s*$", "", t)
    t = t.replace("{{", "").replace("}}", "")
    t = re.sub(r"\|\s*[A-Za-z0-9_ ]+\s*=\s*", "", t)
    t = re.sub(r"\[\[([^\]|]+\|)?([^\]]+)\]\]", r"\2", t)
    t = re.sub(r"\n{2,}", "\n\n", t)
    t = t.strip()
    return t if t else None

def extract_wiki_sections(wikitext):
    if not wikitext:
        return None, None, None

    wikitext = wikitext.replace("\r\n", "\n")

    #remove HTML comments
    wikitext = re.sub(r"<!--.*?-->", "", wikitext, flags=re.DOTALL)

    #remove <ref> blocks
    wikitext = re.sub(r"<ref[^>]*>.*?</ref>", "", wikitext, flags=re.DOTALL)
    wikitext = re.sub(r"<ref[^/>]*/>", "", wikitext)

    #remove cite templates
    wikitext = re.sub(r"\{\{cite[^}]+\}\}", "", wikitext, flags=re.IGNORECASE)

    #remove infobox
    ibox = extract_infobox_block(wikitext)
    if ibox:
        wikitext = wikitext.replace(ibox, "")

    #fixes inline: "== Geography ==Text"
    wikitext = re.sub(
        r"\s*(==+[^=]+?==+)\s*",
        r"\n\1\n",
        wikitext
    )

    section_re = re.compile(r"^==+\s*(.*?)\s*==+\s*$", re.MULTILINE)
    parts = section_re.split(wikitext)
    lead_raw = parts[0].strip()

    #keep first paragraph
    if "\n\n" in lead_raw:
        lead_raw = lead_raw.split("\n\n", 1)[0]

    lead = clean_markup_paragraphs(lead_raw)

    history = None
    geography = None

    #iterate sections
    for i in range(1, len(parts), 2):
        name = parts[i].strip().lower()
        body_raw = parts[i+1]
        body = clean_markup_paragraphs(body_raw)

        if not body:
            continue

        if name in ("history", "historical background", "background", "early history"):
            history = body

        elif name == "geography":
            geography = body

    return lead, history, geography


def clean_markup_paragraphs(text):
    if not text:
        return None

    t = text

    #remove <ref> blocks (should already be gone, but just in case)
    t = re.sub(r"<ref[^>]*>.*?</ref>", "", t, flags=re.DOTALL)
    t = re.sub(r"<ref[^/>]*/>", "", t)

    #remove nested templates {{…}}
    t = re.sub(r"\{\{[^}]+\}\}", "", t)

    #convert wikilinks
    t = re.sub(r"\[\[([^|\]]*\|)?([^\]]+)\]\]", r"\2", t)

    #remove bold/italic quotes
    t = re.sub(r"'{2,}", "", t)

    #remove leftover |
    t = re.sub(r"\|\s*[A-Za-z0-9_]*\s*=", "", t)
    t = re.sub(r"\s+", " ", t)
    t = t.strip()

    return t if t else None

def parse_coord(payload):
    parts = [p.strip() for p in payload.split("|")]
    nums = []
    for p in parts:
        try:
            nums.append(float(re.sub(r"[NnSsEeWw]$", "", p)))
        except:
            pass
        if len(nums) >= 2:
            break
    return (nums[0], nums[1]) if len(nums) >= 2 else (None, None)

def parse_criteria(raw):
    if not raw:
        return []
    return [v.lower() for v in RE_CRITERIA_PARSE.findall(raw)]

def related_titles(text, own_title):
    out, seen = [], set()
    for m in RE_WIKI_LINK.finditer(text):
        target, disp = m.group(1), m.group(2)
        t = _normalize_title(target)
        if not t:
            continue

        shown = _normalize_title(disp or target)
        if not shown:
            continue

        if _normalize_title(own_title) == t:
            continue
        if EXCLUDE_TITLE_PAT.match(t) or EXCLUDE_KEYWORDS.search(t):
            continue
        if EXCLUDE_TITLE_PAT.match(shown) or EXCLUDE_KEYWORDS.search(shown):
            continue
        if (" " not in t) and not INCLUDE_TITLE_PAT.search(t):
            continue

        s, e = m.span()
        ctx = text[max(0, s-120):min(len(text), e+120)]
        looks_heritage = (
            RE_WORLD_HERITAGE.search(shown) or
            RE_WORLD_HERITAGE.search(t) or
            RE_WORLD_HERITAGE.search(ctx) or
            INCLUDE_TITLE_PAT.search(t) or
            INCLUDE_TITLE_PAT.search(shown)
        )
        if not looks_heritage:
            continue

        if t not in seen:
            seen.add(t)
            out.append(t)

    return out


def alias_titles(text, title):
    lead = text[:1500]
    cand = []
    for m in RE_ALIAS_PHRASE.finditer(lead):
        chunk = clean_markup(m.group(1))
        parts = re.split(r"\s*(?:/|,|;|\(|\)|\bor\b|\balso\b|\band\b)\s*", chunk)
        for part in parts:
            ali = _normalize_title(part)
            if not ali or ali.lower() == _normalize_title(title).lower():
                continue
            if re.search(r"\d", ali):
                continue
            if EXCLUDE_TITLE_PAT.match(ali) or EXCLUDE_KEYWORDS.search(ali):
                continue
            if len(ali) < 3:
                continue
            cand.append(ali)

    seen, out = set(), []
    for a in cand:
        if a not in seen:
            seen.add(a)
            out.append(a)
    return out

def extract_fields(title, wikitext):
    ibox = extract_infobox_block(wikitext)
    ibox_fields = parse_infobox_fields(ibox)

    #UNESCO ID
    whs_id = None
    if ibox:
        m = RE_WHS_ID_PARAM.search(ibox)
        if m:
            whs_id = _to_int(m.group(1))
    if whs_id is None:
        m = RE_UNESCO_LINK.search(wikitext)
        if m:
            whs_id = _to_int(m.group(1))

    if not whs_id:
        return None

    #criteria
    crit_raw = ibox_fields.get("criteria")
    criteria_list = parse_criteria(crit_raw)

    #coords
    lat = lon = None
    m = RE_COORD.search(wikitext)
    if m:
        lat, lon = parse_coord(m.group(1))

    #lead + history + geography
    lead_txt, history_txt, geography_txt = extract_wiki_sections(wikitext)

    txt_first_year = _to_int(RE_TXT_FIRST_YEAR.search(wikitext).group(1)) \
                     if RE_TXT_FIRST_YEAR.search(wikitext) else None
    txt_mentions_end = bool(RE_TXT_ENDANGERED.search(wikitext))

    rel_titles = related_titles(wikitext, title)
    aliases    = alias_titles(wikitext, title)

    wiki_title = norm_title(title)
    wiki_link  = f"https://en.wikipedia.org/wiki/{urllib.parse.quote(wiki_title)}" if wiki_title else None
    wiki_title_norm = normalize_for_join(title)

    return {
        "wiki_title": wiki_title,
        "wiki_title_norm": wiki_title_norm,
        "whs_id": whs_id,
        "wiki_criteria": criteria_list,
        "wiki_lat": lat,
        "wiki_lon": lon,
        "wiki_lead": lead_txt,
        "wiki_history": history_txt,
        "wiki_geography": geography_txt,
        "wiki_txt_first_year": txt_first_year,
        "wiki_txt_mentions_endangered": txt_mentions_end,
        "wiki_txt_related_whs_titles": rel_titles,
        "wiki_txt_aliases": aliases,
        "wiki_link": wiki_link,
        "wiki_infobox_fields": ibox_fields,
    }

def parse_page_chunk(xml_chunk):
    if RE_REDIRECT.search(xml_chunk):
        return None
    mt = RE_TITLE.search(xml_chunk)
    mx = RE_TEXT.search(xml_chunk)
    if not mt or not mx:
        return None
    title = mt.group(1).strip()
    text  = mx.group(1)
    return extract_fields(title, text)


def split_pages(iterator):
    buf = []
    inside = False
    for line in iterator:
        if "<page>" in line:
            inside = True
            buf = []
        if inside:
            buf.append(line)
        if "</page>" in line and inside:
            yield "".join(buf)
            inside = False

SCHEMA = StructType([
    StructField("wiki_title", StringType(), True),
    StructField("wiki_title_norm", StringType(), True),
    StructField("whs_id", LongType(), True),
    StructField("wiki_criteria", ArrayType(StringType()), True),
    StructField("wiki_lat", DoubleType(), True),
    StructField("wiki_lon", DoubleType(), True),
    StructField("wiki_lead", StringType(), True),
    StructField("wiki_history", StringType(), True),
    StructField("wiki_geography", StringType(), True),
    StructField("wiki_txt_first_year", IntegerType(), True),
    StructField("wiki_txt_mentions_endangered", BooleanType(), True),
    StructField("wiki_txt_related_whs_titles", ArrayType(StringType()), True),
    StructField("wiki_txt_aliases", ArrayType(StringType()), True),
    StructField("wiki_link", StringType(), True),
    StructField("wiki_infobox_fields", MapType(StringType(), StringType()), True),
])

if __name__ == "__main__":
    spark = SparkSession.builder.appName("wikijoin").getOrCreate()
    sc = spark.sparkContext

    wiki_rdd = sc.textFile(WIKI_XML).mapPartitions(split_pages)
    feats_rdd = wiki_rdd.map(parse_page_chunk).filter(lambda x: x is not None)

    feats_df = spark.createDataFrame(feats_rdd, schema=SCHEMA)

    score = (
        F.when(F.size("wiki_criteria") > 0, 5).otherwise(0)
        + F.when(F.col("wiki_lat").isNotNull(), 3).otherwise(0)
        + F.when(F.col("wiki_lead").isNotNull(), 4).otherwise(0)
        + F.when(F.col("wiki_title").startswith("Draft:"), -100).otherwise(0)
        + F.when(F.col("wiki_title").rlike("(?i)province|district|prefecture|National_Park"), -30).otherwise(0)
    )

    scored = feats_df.withColumn("rank_score", score)
    scored = scored.filter(F.col("rank_score") >= 0)

    w = Window.partitionBy("whs_id").orderBy(F.desc("rank_score"), F.asc("wiki_title"))
    canonical_feats = (
        scored.withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn", "rank_score")
    )

    pages_df = spark.read.json(PAGES_JSON)

    list_df = (
        pages_df
        .filter((F.col("type") == "list_property") & (F.col("url").rlike(r"/list/\d+")))
        .withColumn(
            "whs_id",
            F.coalesce(
                F.col("property_id").cast("long"),
                F.regexp_extract(F.col("url"), r"/list/(\d+)", 1).cast("long"),
            )
        )
        .withColumn(
            "norm_title",
            F.lower(
                F.regexp_replace(F.col("title"), r"[\s_]+", "_")
            )
        )
    )

    by_id = list_df.alias("p").join(
        canonical_feats.alias("w"),
        "whs_id",
        "left"
    )

    by_title = list_df.alias("p").join(
        canonical_feats.alias("w"),
        (F.col("p.norm_title") == F.col("w.wiki_title_norm")),
        "left"
    )

    combined = (
        by_id
        .join(
            by_title.select(
                "p.norm_title",
                *[F.col(f"w.{c}") for c in canonical_feats.columns if c != "whs_id"]
            ).alias("t"),
            "norm_title",
            "left"
        )
    )

    select_cols = []
    for c in canonical_feats.columns:
        if c == "whs_id":
            continue
        select_cols.append(
            F.coalesce(F.col(f"w.{c}"), F.col(f"t.{c}")).alias(c)
        )

    joined = combined.select("p.*", *select_cols)

    others_df = pages_df.filter(~((F.col("type") == "list_property") & (F.col("url").rlike(r"/list/\d+"))))

    out_df = joined.unionByName(others_df, allowMissingColumns=True)
    out_df.write.mode("overwrite").json(OUT_DIR)

    spark.stop()
