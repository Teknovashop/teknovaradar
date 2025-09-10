import os, requests, re, datetime as dt
import xml.etree.ElementTree as ET

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SERVICE_ROLE = os.environ["SUPABASE_SERVICE_ROLE"]
DEV_MODE = os.environ.get("DEV_MODE", "0") == "1"  # si 1, usa feed de prueba y menos filtros

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
# BOE puede fallar con 404/403 desde runners, así que no bloqueamos si falla.
SOURCES = [
    # ("BOE", "https://www.boe.es/diario_boe/rss.php"),  # (puede devolver 404/403)
    # Feed de prueba (para validar pipeline). Cámbialo por fuentes reales cuando quieras:
    ("DEV", "https://hnrss.org/frontpage"),  # SOLO DEV: genera ítems
    # EU TED (ejemplo atom genérico). Ajustaremos consultas específicas después:
    ("TED", "https://ted.europa.eu/udl?uri=TED:FEED:ES:ATOM"),
]

KEYWORDS = re.compile(
    r"(inteligencia artificial|machine learning|ux|diseño|realidad (virtual|aumentada)|kubernetes|aws|azure|datos|bi)\b",
    re.I
)

def fetch(url: str):
    r = requests.get(url, headers=HTTP_HEADERS, timeout=45, allow_redirects=True)
    if not r.ok:
        raise requests.HTTPError(f"{r.status_code} for {url}")
    return r.content

def parse_rss_or_atom(xml_bytes: bytes):
    root = ET.fromstring(xml_bytes)
    ns = {"atom": "http://www.w3.org/2005/Atom"}
    # RSS items
    items = []
    for it in root.findall(".//item"):
        title = (it.findtext("title") or "").strip()
        link  = (it.findtext("link") or "").strip()
        desc  = (it.findtext("description") or "").strip()
        pub   = (it.findtext("pubDate") or "")
        items.append((title, link, desc, pub))
    if items:
        return items
    # Atom entries
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
    import json
    payload = {
        "p_source_code": source_code,
        "p_external_id": external_id,
        "p_title": title[:8000],
        "p_summary": summary[:8000] if summary else None,
        "p_body": body[:200000] if body else None,
        "p_url": url or "https://example.com",
        "p_statu_
