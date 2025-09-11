import os, requests, re, datetime as dt, urllib.parse as up, time, random
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

# Cabeceras tipo navegador
HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Radar-Scraper/1.3",
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

KEYWORDS = re.compile(
    r"(inteligencia artificial|ai\b|machine learning|deep learning|datos|data\b|big data|anal[ií]tica|"
    r"visualizaci[oó]n|cloud|nube|aws|azure|gcp|kubernetes|devops|software|desarrollo|"
    r"ux|ui|diseño|ciberseguridad|seguridad inform[aá]tica|iot|realidad (virtual|aumentada)|"
    r"blockchain|gemelos digitales|5g|hpc|supercomputaci[oó]n|plataforma digital|data lake|data mesh)"
    r"\b", re.I
)

# ====== Utils ======
def is_probably_html_or_json(b: bytes) -> bool:
    s = (b or b"")[:512].strip().lower()
    return (
        s.startswith(b"<!doctype html")
        or s.startswith(b"<html")
        or b"<html" in s
        or s.startswith(b"{")   # JSON u otra respuesta api/gateway
        or s.startswith(b"<!doctype")
    )

def safe_head(b: bytes, n=200) -> str:
    try:
        return (b or b"")[:n].decode("utf-8", "replace")
    except Exception:
        return str((b or b"")[:n])

def add_cache_buster(url: str) -> str:
    p = up.urlparse(url)
    q = up.parse_qs(p.query, keep_blank_values=True)
    q["_"] = [str(int(time.time())) + str(random.randint(100,999))]
    new_query = up.urlencode(q, doseq=True)
    return up.urlunparse(p._replace(query=new_query))

def all_variants(url: str):
    """Genera http/https + con/sin www + sus versiones proxificadas."""
    out = []

    def toggles(u: str):
        try:
            p = up.urlparse(u)
            hosts = [p.netloc]
            if p.netloc.startswith("www."):
                hosts.append(p.netloc[4:])
            else:
                hosts.append("www."+p.netloc)
            schemes = ["https", "http"] if p.scheme == "https" else ["http", "https"]
            for h in dict.fromkeys(hosts):
                for s in schemes:
                    yield up.urlunparse(p._replace(scheme=s, netloc=h))
        except Exception:
            yield u

    base = add_cache_buster(url)
    for v in toggles(base):
        out.append(v)

    # proxys para cada variante
    prox = []
    for v in out:
        p = up.urlparse(v)
        prox.append(f"https://r.jina.ai/{p.scheme}://{p.netloc}{p.path}{('?' + p.query) if p.query else ''}")

    # orden: variantes directas primero, luego proxys
    full = list(dict.fromkeys(out + prox))
    return full

def fetch_xml_with_fallback(url: str) -> bytes | None:
    """Intenta descargar XML probando variantes y proxies; valida parse."""
    tried = all_variants(url)
    for candidate in tried:
        try:
            r = requests.get(candidate, headers=HTTP_HEADERS, timeout=60, allow_redirects=True)
            print(f"[FETCH] {candidate} -> {r.status_code} bytes:{len(r.content)}")

            # 204/3xx/4xx/5xx
            r.raise_for_status()

            head = safe_head(r.content, 200)
            if is_probably_html_or_json(r.content):
                print(f"[FETCH] Parece HTML/JSON. Head: {head!r}")
                continue

            # Validar que sea XML
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

    # RSS
    for it in root.findall(".//item"):
        title = (it.findtext("title") or "").strip()
        link  = (it.findtext("link") or "").strip()
        desc  = (it.findtext("description") or "").strip()
        pub   = (it.findtext("pubDate") or "").strip()
        items.append((title, link, desc, pub))
    if items:
        return items

    # Atom con namespace
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
        "%a, %d %b %Y %H:%M:%S %z",
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
