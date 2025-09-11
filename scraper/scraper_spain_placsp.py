import os, requests, re, datetime as dt, urllib.parse as up
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

# Cabeceras "de navegador" para evitar portales intermedios
HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Radar-Scraper/1.2",
    "Accept": "application/atom+xml, application/rss+xml, application/xml, text/xml, */*",
    "Accept-Language": "es-ES,es;q=0.9",
    "Referer": "https://www.contrataciondelestado.es/",
    "Connection": "keep-alive",
}

SB_HEADERS = {
    "apikey": SERVICE_ROLE,
    "Authorization": f"Bearer {SERVICE_ROLE}",
    "Content-Type": "application/json",
}

# Palabras clave tecnológicas (ajústalas)
KEYWORDS = re.compile(
    r"(inteligencia artificial|ai\b|machine learning|deep learning|datos|data\b|big data|anal[ií]tica|"
    r"visualizaci[oó]n|cloud|nube|aws|azure|gcp|kubernetes|devops|software|desarrollo|"
    r"ux|ui|diseño|ciberseguridad|seguridad inform[aá]tica|iot|realidad (virtual|aumentada)|"
    r"blockchain|gemelos digitales|5g|hpc|supercomputaci[oó]n|plataforma digital|data lake|data mesh)"
    r"\b", re.I
)

# ====== Utils ======
def is_probably_html(b: bytes) -> bool:
    sample = (b or b"")[:512].lower()
    return sample.startswith(b"<!doctype html") or b"<html" in sample or b"<meta" in sample

def variants_for(url: str):
    """Genera variantes (www/http) y fallback proxy r.jina.ai."""
    urls = [url]

    try:
        p = up.urlparse(url)
        host = p.netloc
        if host and not host.startswith("www."):
            with_www = up.urlunparse(p._replace(netloc="www."+host))
            urls.append(with_www)
        if p.scheme == "https":
            urls.append(up.urlunparse(p._replace(scheme="http")))
    except Exception:
        pass

    # Fallback proxy (sólo lectura). Sirve el contenido plano sin cookies.
    # Nota: mantiene el esquema original al proxificar.
    def to_proxy(u: str):
        p = up.urlparse(u)
        prox = f"https://r.jina.ai/{p.scheme}://{p.netloc}{p.path}"
        if p.query:
            prox += f"?{p.query}"
        return prox

    urls.append(to_proxy(url))
    return list(dict.fromkeys(urls))  # quitar duplicados preservando orden

def fetch_xml_with_fallback(url: str) -> bytes | None:
    """Intenta descargar XML. Si recibe HTML/portal, prueba variantes y proxy."""
    for candidate in variants_for(url):
        try:
            r = requests.get(candidate, headers=HTTP_HEADERS, timeout=60, allow_redirects=True)
            print(f"[FETCH] {candidate} -> {r.status_code} bytes:{len(r.content)}")
            r.raise_for_status()
            if is_probably_html(r.content):
                print("[FETCH] Parece HTML/portal, probando siguiente variante...")
                continue
            # Validar que parsea como XML
            ET.fromstring(r.content)
            print("[FETCH] XML válido ✔")
            return r.content
        except Exception as e:
            print(f"[FETCH] Fallo con {candidate}: {e}")
            continue
    print("[FETCH] No se pudo obtener XML válido para:", url)
    return None

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

    # Atom sin namespace (fallback)
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
        "%a, %d %b %Y %H:%M:%S %z",  # RSS
        "%d/%m/%Y %H:%M",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S",
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
        "p_source_code": source_code,
        "p_external_id": external_id,
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

        xml = fetch_xml_with_fallback(feed_url)
        if not xml:
            continue

        try:
            items = parse_rss_or_atom(xml)
        except Exception as e:
            print(f"[WARN] No se pudo parsear XML final: {e}")
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
