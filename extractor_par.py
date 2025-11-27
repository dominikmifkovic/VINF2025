#
# Dominik Mifkovič 2025
#
import os
import re
import json
import html
import multiprocessing
from multiprocessing import Pool, cpu_count

SRC_DIR = "pages_filtered"
OUT_FILE = "pages.jsonl"
TIMEOUT_SEC = 10

MAIN_LIST_URL_RE = re.compile(r"^https://whc\.unesco\.org/en/list/(\d+)/?$", re.I)
DECISION_URL_RE = re.compile(r"^https://whc\.unesco\.org/en/decisions/(\d+)/?$", re.I)
SOC_URL_RE = re.compile(r"^https://whc\.unesco\.org/en/soc/(\d+)/?$", re.I)


def filename_to_url(filename):
    name = os.path.splitext(filename)[0]
    return "https://" + name.replace("_", "/")

def is_not_found(html_src):
    return bool(re.search(r"(?is)<h1[^>]*>\s*404\s*</h1>", html_src))

def clean_text(s):
    s = re.sub(r"(?s)<[^>]+>", " ", s)
    return " ".join(s.split()).strip()

#nechcene labely
SECTION_LABELS = {
    "documents", "gallery", "maps", "indicators", "video", "assistance",
    "see also", "related links", "external links",
    "decision", "decisions", "state of conservation"
}

STATE_JUNK = {
    "the state parties", "state parties", "list of state parties",
    "states parties", "the states parties"
}

ROMAN_GROUP = r"(?:i|ii|iii|iv|v|vi|vii|viii|ix|x)"

#list
def extract_list_page(html_content, url):
    html_min = re.sub(r"(?is)<(script|style|header|footer|nav)[^>]*>.*?</\1>", " ", html_content)
    html_min = re.sub(r"(?is)<!--.*?-->", " ", html_min)

    m = MAIN_LIST_URL_RE.match(url)
    property_id = int(m.group(1)) if m else None

    t = re.search(r"(?is)<title[^>]*>(.*?)</title>", html_min)
    title = html.unescape(t.group(1).strip()) if t else ""

    desc = re.search(r'(?is)<div[^>]*class="[^"]*\bdescription\b[^"]*"[^>]*>(.*?)</div>', html_min)
    description = clean_text(desc.group(1)) if desc else ""

    #state parties
    state_raw = re.findall(r'<a[^>]+href="[^"]*/statesparties/[^"]*"[^>]*>(.*?)</a>', html_min, re.I)
    states = []
    seen_states = set()
    for s in state_raw:
        txt = clean_text(s)
        low = txt.lower()
        if not txt or low in STATE_JUNK or low.endswith(":"):
            continue
        if txt not in seen_states:
            seen_states.add(txt)
            states.append(txt)
    if states:
        states.sort()

    #region
    region = ""
    m = re.search(r"Region:</b>\s*([^<]+)", html_min, re.I)
    if m:
        region = clean_text(m.group(1))

    #category
    category = ""
    m = re.search(r"(?:Property type|Category):</b>\s*([^<]+)", html_min, re.I)
    if m:
        category = clean_text(m.group(1))

    #criteria
    crits = sorted({c.lower() for c in re.findall(rf"\(({ROMAN_GROUP})\)", html_min, re.I)})

    #year
    inscription_year = None
    m = re.search(r"Inscribed\s+in\s+(19|20)\d{2}", html_min, re.I)
    if m:
        inscription_year = int(m.group(0).split()[-1])

    #area
    area_hectares = None
    m = re.search(r"\b([\d.,]+)\s*ha\b", html_min, re.I)
    if m:
        val = m.group(1).replace(" ", "").replace(",", ".")
        try:
            area_hectares = float(val)
        except:
            pass

    #coords
    coordinates = None
    m = re.search(r"([-+]?\d{1,2}\.\d+)\s*[,;]\s*([-+]?\d{1,3}\.\d+)", html_min)
    if m:
        try:
            coordinates = {"lat": float(m.group(1)), "lon": float(m.group(2))}
        except:
            pass

    #text
    parts = re.findall(r"(?is)<(h[1-5]|p|li)[^>]*>(.*?)</\1>", html_min)
    seen = set()
    text_list = []
    for tag, inner in parts:
        s = clean_text(inner)
        if not s:
            continue
        low = s.lower().strip(" :·")
        if low in SECTION_LABELS or low in STATE_JUNK or low.endswith(":"):
            continue
        if tag.startswith("h") and (low in SECTION_LABELS or len(s.split()) <= 2):
            continue
        if s not in seen:
            seen.add(s)
            text_list.append(s)

    text = html.unescape(" ".join(text_list))

    doc = {
        "url": url,
        "type": "list_property",
        "property_id": property_id,
        "title": title,
        "description": description or None,
        "state_parties": states or None,
        "region": region or None,
        "category": category or None,
        "criteria": crits or None,
        "inscription_year": inscription_year,
        "area_hectares": area_hectares,
        "coordinates": coordinates,
        "text": text
    }
    return {k: v for k, v in doc.items() if v is not None}

#decisions
def extract_decision_page(html_content, url):
    html_min = re.sub(r"(?is)<(script|style|header|footer|nav)[^>]*>.*?</\1>", " ", html_content)
    html_min = re.sub(r"(?is)<!--.*?-->", " ", html_min)

    m = DECISION_URL_RE.match(url)
    decision_id = int(m.group(1)) if m else None

    #title
    t = re.search(r"(?is)<title[^>]*>(.*?)</title>", html_min)
    title = html.unescape(t.group(1).strip()) if t else ""

    #decision code
    code = None
    m = re.search(r"\b\d+\s*(?:COM|EXTCOM|BUR)\s*[0-9A-Za-z.\-]+", title, re.I)
    if not m:
        m = re.search(r"Decision\s+(\d+\s*(?:COM|EXTCOM|BUR)\s*[0-9A-Za-z.\-]+)", html_min, re.I)
        code = clean_text(m.group(1)) if m else None
    else:
        code = clean_text(m.group(0))

    #session code
    session_code = None
    if code:
        ms = re.search(r"\b(\d+)\s*(COM|EXTCOM|BUR)\b", code, re.I)
        if ms:
            session_code = f"{ms.group(1)}{ms.group(2).upper()}"

    #related property
    prop_id = None
    mp = re.search(r"/en/list/(\d+)/?", html_min)
    if mp:
        prop_id = int(mp.group(1))

    #year
    year = None
    candidates = []
    for y in re.findall(r"\b(19\d{2}|20\d{2})\b", title):
        candidates.append(int(y))
    intro_ps = re.findall(r"(?is)<p[^>]*>(.*?)</p>", html_min)[:3]
    for p in intro_ps:
        for y in re.findall(r"\b(19\d{2}|20\d{2})\b", p):
            candidates.append(int(y))
    m_year = re.search(r'(?is)<div[^>]*>\s*Year\s*</div>.*?<a[^>]*>(19\d{2}|20\d{2})</a>', html_min)
    if m_year:
        candidates.append(int(m_year.group(1)))
    candidates = [y for y in candidates if 1973 <= y <= 2100]
    if candidates:
        year = max(candidates)

    meta = {}

    #themes
    m = re.search(r'(?is)<div[^>]*>\s*Themes\s*</div>\s*<[^>]*class="[^"]*tag[^"]*"[^>]*>(.*?)</', html_min)
    if m:
        meta["themes"] = [clean_text(x) for x in re.split(r"[,;]\s*", m.group(1)) if x.strip()]

    #focal point
    m = re.search(r'(?is)<div[^>]*>\s*Focal Point\s*</div>\s*<[^>]*class="[^"]*tag[^"]*"[^>]*>(.*?)</', html_min)
    if m:
        meta["focal_point"] = clean_text(m.group(1))

    #states parties
    states = re.findall(r'<a[^>]+href="[^"]*/statesparties/[^"]*"[^>]*>(.*?)</a>', html_min, re.I)
    if states:
        meta["states_parties"] = sorted({clean_text(s) for s in states if s.strip()})

    #properties
    props = re.findall(r'<a[^>]+href="[^"]*/list/[^"]*"[^>]*>(.*?)</a>', html_min, re.I)
    if props:
        meta["properties"] = sorted({clean_text(p) for p in props if p.strip()})

    #session
    m = re.search(r'(?is)<div[^>]*>\s*Session\s*</div>\s*<a[^>]*>(.*?)</a>', html_min)
    if m:
        meta["session_name"] = clean_text(m.group(1))

    #soc reports
    socs = re.findall(r'<a[^>]+href="[^"]*/soc/(\d+)/"[^>]*>(.*?)</a>', html_min, re.I)
    if socs:
        meta["soc_reports"] = [
            {"soc_id": int(i), "title": clean_text(t)} for i, t in socs
        ]

    #text
    parts = re.findall(r"(?is)<(h[1-5]|p|li)[^>]*>(.*?)</\1>", html_min)
    seen = set()
    text_list = []
    for tag, inner in parts:
        s = clean_text(inner)
        if not s:
            continue
        low = s.lower().strip(" :·")
        if low in SECTION_LABELS or low.endswith(":"):
            continue
        if tag.startswith("h") and len(s.split()) <= 2:
            continue
        if s not in seen:
            seen.add(s)
            text_list.append(s)
    text = html.unescape(" ".join(text_list))

    doc = {
        "url": url,
        "type": "decision",
        "decision_id": decision_id,
        "title": title,
        "decision_code": code or None,
        "session_code": session_code or None,
        "year": year,
        "related_property_id": prop_id,
        "text": text,
        **meta
    }
    return {k: v for k, v in doc.items() if v is not None}


def extract_soc_page(html_content, url):
    html_min = re.sub(r"(?is)<(script|style|header|footer|nav)[^>]*>.*?</\1>", " ", html_content)
    html_min = re.sub(r"(?is)<!--.*?-->", " ", html_min)

    #odstrani sa vsetko pod nadpisom "Decisions adopted by the Committee" lebo obsahuje irelevantne info o inych pamiatkach
    #inak by to davalo irelevantne vysledky pri vyhladavani
    #napriklad ked som hladal bardejov tak to naslo boliviu lebo sa tam na spodku spominal :DDD
    decisions_hdr_re = re.compile(
        r'(?is)<h[1-6][^>]*>\s*Decisions\s+adopted\s+by\s+the\s+Committee(?:\s+in\s+\d{4})?\s*</h[1-6]>'
    )
    m_dec = decisions_hdr_re.search(html_min)
    if m_dec:
        html_min = html_min[:m_dec.start()]

    m = SOC_URL_RE.match(url)
    soc_id = int(m.group(1)) if m else None

    t = re.search(r"(?is)<title[^>]*>(.*?)</title>", html_min)
    title = html.unescape(t.group(1).strip()) if t else ""

    #year
    year = None
    my = re.search(r"\bSOC\s*(19|20)\d{2}\b", title, re.I)
    if my:
        year = int(re.search(r"(19|20)\d{2}", my.group(0)).group(0))
    else:
        my2 = re.search(r"\b(19|20)\d{2}\b", title) or re.search(r"\b(19|20)\d{2}\b", html_min)
        if my2:
            year = int(re.search(r"(19|20)\d{2}", my2.group(0)).group(0))

    #related property id
    prop_id = None
    mp = re.search(r"/en/list/(\d+)/?", html_min)
    if mp:
        prop_id = int(mp.group(1))

    #site name
    h1 = re.search(r"(?is)<h1[^>]*>(.*?)</h1>", html_min)
    site_name = clean_text(h1.group(1)) if h1 else None

    #summary
    ps = re.findall(r"(?is)<p[^>]*>(.*?)</p>", html_min)
    summary = None
    for p in ps:
        s = clean_text(p)
        if s and len(s.split()) > 8:
            summary = s
            break

    #text
    parts = re.findall(r"(?is)<(h[1-5]|p|li)[^>]*>(.*?)</\1>", html_min)
    seen = set()
    text_list = []
    for tag, inner in parts:
        s = clean_text(inner)
        if not s:
            continue
        low = s.lower().strip(" :·")
        if low in SECTION_LABELS or low.endswith(":"):
            continue
        if tag.startswith("h") and (len(s.split()) <= 2):
            continue
        if s not in seen:
            seen.add(s)
            text_list.append(s)
    text = html.unescape(" ".join(text_list))

    doc = {
        "url": url,
        "type": "soc",
        "soc_id": soc_id,
        "title": title,
        "year": year,
        "related_property_id": prop_id,
        "site_name": site_name or None,
        "summary": summary or None,
        "text": text
    }
    return {k: v for k, v in doc.items() if v is not None}


def process_file(args):
    src_path, fname = args
    try:
        with open(src_path, "r", encoding="utf-8", errors="ignore") as f:
            html_src = f.read()

        if is_not_found(html_src):
            return None, "404"

        url = filename_to_url(fname)

        if MAIN_LIST_URL_RE.match(url):
            doc = extract_list_page(html_src, url)
        elif DECISION_URL_RE.match(url):
            doc = extract_decision_page(html_src, url)
        elif SOC_URL_RE.match(url):
            doc = extract_soc_page(html_src, url)
        else:
            return None, None

        if not doc.get("text"):
            return None, None

        return doc, None

    except Exception as e:
        return None, f"Error: {src_path}: {e}"


def main():
    files = []
    for root, _, names in os.walk(SRC_DIR):
        for fname in names:
            files.append((os.path.join(root, fname), fname))

    total = written = skipped = errs = 0
    procs = min(4, cpu_count())

    with open(OUT_FILE, "w", encoding="utf-8") as out, Pool(processes=procs, maxtasksperchild=200) as pool:
        jobs = [pool.apply_async(process_file, args=(f,)) for f in files]

        for job in jobs:
            try:
                doc, msg = job.get(timeout=TIMEOUT_SEC)
            except Exception as e:
                doc, msg = None, f"Timeout/Error: {e!r}"

            total += 1
            if msg == "404":
                skipped += 1
            elif msg and not doc:
                errs += 1
                print(f"\n[WARN] {msg}")
            elif doc:
                out.write(json.dumps(doc, ensure_ascii=False) + "\n")
                out.flush()
                written += 1

            print(f"\rProcessed {total}/{len(files)} | written: {written} | 404: {skipped} | errors: {errs}", end="", flush=True)

    print("\nDone.")

if __name__ == "__main__":
    multiprocessing.set_start_method("spawn")
    main()
