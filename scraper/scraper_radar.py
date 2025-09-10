import os, requests, re, datetime as dt
import xml.etree.ElementTree as ET

# ====== Config ======
SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SERVICE_ROLE = os.environ["SUPABASE_SERVICE_ROLE"]

# RPCs en Supabase
RPC_UPSERT_TENDER = f"{SUPABASE_URL}/rest/v1/rpc/upsert_tender"
RPC_ASSIGN_CATS   = f"{SUPABASE_URL}/rest/v1/rpc/assign_categories_from_keywords"

# Sólo TED (ES) en Atom; son anuncios reales
SOURCES = [
    # Feed general en español; filtraremos por keywords
    ("TED", "https://ted.europa.eu/udl?uri=TED:FEED:ES:ATOM"),
]

# Palabras clave tecnológicas (ajústalas a tu criterio)
KEYWORDS = re.compile(
    r"(inteligencia artificial|machine learning|deep learning|datos|big data|analítica|"
    r"visualización|cloud|nube|aws|azure|gcp|kubernetes|devops|software|desarrollo|"
    r"ux|ui|diseño|ciberseguridad|seguridad|iot|realidad (virtual|aumentada)|blockchain|gemelos digitales)"
    r"\b", re.I
)

HTTP_HEADERS = {
    "User-Agent": "Radar-Teknovashop/1.0 (+https://teknovashop.com)",
    "Accept": "application/atom+xml, application/xml, text/xml, */*",
}

SB_HEADERS = {
    "apikey": SERVICE_ROLE,
    "Authorization": f"Bearer {SERVICE_ROLE}",
    "Content-Type": "application/json",
}

# ====== Utils ======
def fetch(url: str) -> bytes:
    r = requests.get(url, headers=HTTP_HEADERS, timeout=45, allow_redirects=True)
    r.raise_for_status()
    return r.content

def parse_atom(xml_bytes: bytes):
    root = ET.fromstring(xml_bytes)
    ns = {"atom": "http://www.w3.org/2005/Atom"}
    items = []
    for e in root.findall(".//atom:entry", ns):
        title = (e.findtext("atom:title", default="", namespaces=ns) or "").strip()
        link_el = e.find("atom:link", ns)
        link = link_el.get("href") if link_el is not None else ""
        summary = (e.findtext("atom:summary", default="", namespaces=ns) or
                   e.findtext("atom:content", default="", namespaces=ns) or "").strip()
        pub  = (e.findtext("atom:updated", default="", namespaces=ns) or
                e.findtext("atom:published", default="", namespaces=ns) or "")
        items.append((title, link, summary, pub))
    return items

def parse_date(s: str):
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try: return dt.datetime.strptime(s.replace("Z","+0000"), fmt)
        except Exception: pass
    try: return dt.datetime.fromisoformat(s.replace("Z","+00:00"))
    except Exception: return dt.datetime.utcnow()

def upsert_tender(source_code, external_id, title, summary, url, published_at):
    payload = {
        "p_source_code": source_code,                   # 'TED'
        "p_external_id": external_id,                   # usamos el propio link como id externo
        "p_title": title[:8000],
        "p_summary": (summary or "")[:8000] or None,
        "p_body": None,                                 # opcional si luego raspas el html
        "p_url": url or external_id or "https://example.com",
        "p_status": "open",
        "p_budget": None,
        "p_currency": "EUR",
        "p_entity": None,
        "p_cpv": None,
        "p_country": "EU",
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

# ====== Main ======
def main():
    total = 0
    for source_code, url in SOURCES:
        try:
            xml = fetch(url)
        except Exception as e:
            print(f"[WARN] No se pudo obtener {source_code}: {e}")
            continue

        try:
            items = parse_atom(xml)
        except Exception as e:
            print(f"[WARN] No se pudo parsear {source_code}: {e}")
            continue

        # Filtra por keywords en título + resumen
        filtered = []
        for title, link, summary, pub in items:
            text = f"{title}\n{summary}"
            if KEYWORDS.search(text):
                filtered.append((title, link, summary, pub))

        for title, link, summary, pub in filtered[:200]:
            published = parse_date(pub)
            external_id = link or (title[:32] + str(abs(hash(title+summary)) % 10**8))
            try:
                tid = upsert_tender(source_code, external_id, title, summary, link, published)
                assign_categories(tid)
                total += 1
                print("Upserted:", source_code, tid, title[:100])
            except Exception as e:
                print(f"[WARN] Falló upsert: {e}")

    print(f"TOTAL INSERTADOS (TED): {total}")

if __name__ == "__main__":
    main()
