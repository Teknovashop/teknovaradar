import os, requests, re, datetime as dt
import xml.etree.ElementTree as ET

# ====== Config ======
SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SERVICE_ROLE = os.environ["SUPABASE_SERVICE_ROLE"]
PLACSP_FEEDS = os.environ.get("PLACSP_FEEDS", "").strip()
STRICT_FILTER = os.environ.get("STRICT_FILTER", "true").lower() == "true"
MAX_ITEMS = int(os.environ.get("MAX_ITEMS", "200"))

if not PLACSP_FEEDS:
    raise SystemExit("Falta PLACSP_FEEDS (lista separada por comas con URLs de feeds Atom/RSS de la PLACSP)")

RPC_UPSERT_TENDER = f"{SUPABASE_URL}/rest/v1/rpc/upsert_tender"
RPC_ASSIGN_CATS   = f"{SUPABASE_URL}/rest/v1/rpc/assign_categories_from_keywords"

HTTP_HEADERS = {
    "User-Agent": "Radar-Teknovashop/1.1 (+https://teknovashop.com)",
    "Accept": "application/atom+xml, application/rss+xml, application/xml, text/xml, */*",
}

SB_HEADERS = {
    "apikey": SERVICE_ROLE,
    "Authorization": f"Bearer {SERVICE_ROLE}",
    "Content-Type": "application/json",
}

# Palabras clave tecnológicas (ajústalas a gusto)
KEYWORDS = re.compile(
    r"(inteligencia artificial|ai\b|machine learning|deep learning|datos|data\b|big data|anal[ií]tica|"
    r"visualizaci[oó]n|cloud|nube|aws|azure|gcp|kubernetes|devops|software|desarrollo|"
    r"ux|ui|diseño|ciberseguridad|seguridad inform[aá]tica|iot|realidad (virtual|aumentada)|"
    r"blockchain|gemelos digitales|5g|hpc|supercomputaci[oó]n|plataforma digital|data lake|data mesh)"
    r"\b", re.I
)

# ====== Utils ======
def fetch(url: str) -> bytes:
    r = requests.get(url, headers=HTTP_HEADERS, timeout=60, allow_redirects=True)
    r.raise_for_status()
    return r.content

def parse_rss_or_atom(xml_bytes: bytes):
    """Devuelve lista de (title, link, summary, published_raw)."""
    root = ET.fromstring(xml_bytes)

    items = []

    # RSS 2.0
    for it in root.findall(".//item"):
        title = (it.findtext("title") or "").strip()
        link  = (it.findtext("link") or "").strip()
        desc  = (it.findtext("description") or "").strip()
        pub   = (it.findtext("pubDate") or "").strip()
        items.append((title, link, desc, pub))
    if items:
        return items

    # Atom con namespace clásico
    ns = {"atom": "http://www.w3.org/2005/Atom"}
    for e in root.findall(".//atom:entry", ns):
        title = (e.findtext("atom:title", default="", namespaces=ns) or "").strip()
        link_el = e.find("atom:link", ns)
        link = link_el.get("href") if link_el is not None else ""
        desc = (e.findtext("atom:summary", default="", namespaces=ns) or
                e.findtext("atom:content", default="", namespaces=ns) or "").strip()
        pub  = (e.findtext("atom:updated", default="", namespaces=ns) or
                e.findtext("atom:published", default="", namespaces=ns) or "").strip()
        items.append((title, link, desc, pub))
    if items:
        return items

    # Atom sin declarar namespace (poco habitual, pero por si acaso)
    for e in root.findall(".//entry"):
        title = (e.findtext("title") or "").strip()
        link_el = e.find("link")
        link = link_el.get("href") if link_el is not None else ""
        desc = (e.findtext("summary") or e.findtext("content") or "").strip()
        pub  = (e.findtext("updated") or e.findtext("published") or "").strip()
        items.append((title, link, desc, pub))
    return items

def parse_date(s: str):
    s = (s or "").strip().replace("Z","+0000")
    fmts = [
        "%a, %d %b %Y %H:%M:%S %z",  # RSS en inglés
        "%d/%m/%Y %H:%M",            # algunos feeds locales
        "%Y-%m-%dT%H:%M:%S%z",       # ISO con tz
        "%Y-%m-%dT%H:%M:%S",         # ISO sin tz
        "%Y-%m-%d",
    ]
    for fmt in fmts:
        try:
            return dt.datetime.strptime(s, fmt)
        except Exception:
            pass
    try:
        return dt.datetime.fromisoformat(s.replace("Z","+00:00"))
    except Exception:
        return dt.datetime.utcnow()

def upsert_tender(source_code, external_id, title, summary, url, published_at):
    payload = {
        "p_source_code": source_code,   # 'ES-PLACSP'
        "p_external_id": external_id,   # usamos link como id externo
        "p_title": title[:8000],
        "p_summary": (summary or "")[:8000] or None,
        "p_body": None,
        "p_url": url or external_id or "https://example.com",
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
    r = requests.post(RPC_UPSERT_TENDER, headers=SB_HEADERS, json=payload, timeout=60)
    r.raise_for_status()
    data = r.json()
    return data[0] if isinstance(data, list) else data

def assign_categories(tender_id):
    r = requests.post(RPC_ASSIGN_CATS, headers=SB_HEADERS, json={"p_tender_id": tender_id}, timeout=30)
    r.raise_for_status()

# ====== Main ======
def main():
    total = 0
    feeds = [u.strip() for u in PLACSP_FEEDS.split(",") if u.strip()]
    print(f"[INFO] Feeds PLACSP recibidos: {len(feeds)}")

    for feed_url in feeds:
        print(f"[INFO] Leyendo: {feed_url}")
        try:
            xml = fetch(feed_url)
            items = parse_rss_or_atom(xml)
        except Exception as e:
            print(f"[WARN] No se pudo procesar feed {feed_url}: {e}")
            continue

        print(f"[INFO] Items en feed: {len(items)}")
        for t, l, _, _ in items[:3]:
            print(f"       - {t[:80]} -> {l}")

        # Filtro por keywords (opcional)
        filtered = []
        for title, link, summary, pub in items:
            if not STRICT_FILTER:
                filtered.append((title, link, summary, pub))
                continue
            text = f"{title}\n{summary}"
            if KEYWORDS.search(text):
                filtered.append((title, link, summary, pub))

        print(f"[INFO] Items tras filtro (STRICT_FILTER={STRICT_FILTER}): {len(filtered)}")

        for title, link, summary, pub in filtered[:MAX_ITEMS]:
            published = parse_date(pub)
            external_id = link or (title[:32] + str(abs(hash(title+summary)) % 10**8))
            try:
                tid = upsert_tender("ES-PLACSP", external_id, title, summary, link, published)
                assign_categories(tid)
                total += 1
                print("  [+] Upsert ES-PLACSP:", tid, "->", title[:100])
            except Exception as e:
                print(f"[WARN] Falló upsert ES-PLACSP: {e}")

    print(f"[DONE] TOTAL INSERTADOS (ES-PLACSP): {total}")

if __name__ == "__main__":
    main()
