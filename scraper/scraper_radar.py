import os, requests, re, datetime as dt
import xml.etree.ElementTree as ET

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SERVICE_ROLE = os.environ["SUPABASE_SERVICE_ROLE"]
DEV_MODE = os.environ.get("DEV_MODE", "0") == "1"  # si 1, usa fuente de prueba y menos filtros

RPC_UPSERT_TENDER = f"{SUPABASE_URL}/rest/v1/rpc/upsert_tender"
RPC_ASSIGN_CATS   = f"{SUPABASE_URL}/rest/v1/rpc/assign_categories_from_keywords"

HTTP_HEADERS = {
    "User-Agent": "Radar-Teknovashop/1.0 (+https://teknovashop.com)",
    "Accept": "application/xml,text/xml,application/atom+xml,*/*",
}

SB_HEADERS = {
    "apikey": SERVICE_ROLE,
    "Authorization": f"Bearer {SERVICE_ROLE}",
    "Content-Type": "application/json",
}

# --- Fuentes ---
SOURCES = [
    ("DEV", "https://hnrss.org/frontpage") if DEV_MODE else None,
    # Ejemplo de feed TED (Atom genérico). Luego afinamos con consultas:
    ("TED", "https://ted.europa.eu/udl?uri=TED:FEED:ES:ATOM"),
    # BOE RSS puede dar 404/403 desde runners; lo activaremos con scraping HTML:
    # ("BOE", "https://www.boe.es/diario_boe/rss.php"),
]
SOURCES = [s for s in SOURCES if s]

KEYWORDS = re.compile(
    r"(inteligencia artificial|machine learning|ux|diseño|realidad (virtual|aumentada)|kubernetes|aws|azure|datos|bi)\b",
    re.I,
)

def fetch(url: str) -> bytes:
    r = requests.get(url, headers=HTTP_HEADERS, timeout=45, allow_redirects=True)
    r.raise_for_status()
    return r.content

def parse_rss_or_atom(xml_bytes: bytes):
    root = ET.fromstring(xml_bytes)
    ns = {"atom": "http://www.w3.org/2005/Atom"}
    items = []
    for it in root.findall(".//item"):
        title = (it.findtext("title") or "").strip()
        link  = (it.findtext("link") or "").strip()
        desc  = (it.findtext("description") or "").strip()
        pub   = (it.findtext("pubDate") or "")
        items.append((title, link, desc, pub))
    if items:
        return items
    for e in root.findall(".//atom:entry", ns):
        title = (e.findtext("atom:title", default="", namespaces=ns) or "").strip()
        link_el = e.find("atom:link", ns)
        link = link_el.get("href") if link_el is not None else ""
        desc = (e.findtext("atom:summary", default="", namespaces=ns) or
                e.findtext("atom:content", default="", namespaces=ns) or "").strip()
        pub  = (e.findtext("atom:updated", default="", namespaces=ns) or
                e.findtext("atom:published", default="", namespaces=ns) or "")
        items.append((title, link, desc, pub))
    return items

def parse_date(s: str):
    for fmt in ("%a, %d %b %Y %H:%M:%S %Z", "%a, %d %b %Y %H:%M:%S %z"):
        try:
            return dt.datetime.strptime(s, fmt)
        except Exception:
            pass
    try:
        return dt.datetime.fromisoformat(s.replace("Z",""))
    except Exception:
        return dt.datetime.utcnow()

def upsert_tender(source_code, external_id, title, summary, body, url, published_at):
    payload = {
        "p_source_code": source_code,
        "p_external_id": external_id,
        "p_title": title[:8000],
        "p_summary": summary[:8000] if summary else None,
        "p_body": body[:200000] if body else None,
        "p_url": url or "https://example.com",
        "p_status": "open",
        "p_budget": None,
        "p_currency": "EUR",
        "p_entity": None,
        "p_cpv": None,
        "p_country": "ES",
        "p_region": None,
        "p_published": (published_at or dt.datetime.utcnow()).isoformat(),
        "p_deadline": None
    }
    r = requests.post(RPC_UPSERT_TENDER, headers=SB_HEADERS, json=payload, timeout=45)
    r.raise_for_status()
    data = r.json()
    return data[0] if isinstance(data, list) else data

def assign_categories(tender_id):
    r = requests.post(RPC_ASSIGN_CATS, headers=SB_HEADERS, json={"p_tender_id": tender_id}, timeout=30)
    r.raise_for_status()

def main():
    total = 0
    for source_code, url in SOURCES:
        try:
            xml = fetch(url)
        except Exception as e:
            print(f"[WARN] No se pudo obtener {source_code}: {e}")
            continue
        try:
            items = parse_rss_or_atom(xml)
        except Exception as e:
            print(f"[WARN] No se pudo parsear {source_code}: {e}")
            continue

        for title, link, desc, pub in items[:100]:
            text = f"{title}\n{desc}"
            if not DEV_MODE and not KEYWORDS.search(text):
                continue
            published = parse_date(pub)
            external_id = link or (title[:32] + str(abs(hash(text)) % 10**8))
            try:
                tid = upsert_tender(source_code, external_id, title, desc, None, link, published)
                assign_categories(tid)
                total += 1
                print("Upserted:", source_code, tid, title[:80])
            except Exception as e:
                print(f"[WARN] Falló upsert para {source_code}: {e}")
    print(f"TOTAL INSERTADOS: {total}")

if __name__ == "__main__":
    main()
