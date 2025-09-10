import os, requests, re, datetime as dt
import xml.etree.ElementTree as ET

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SERVICE_ROLE = os.environ["SUPABASE_SERVICE_ROLE"]

RPC_UPSERT_TENDER = f"{SUPABASE_URL}/rest/v1/rpc/upsert_tender"
RPC_ASSIGN_CATS   = f"{SUPABASE_URL}/rest/v1/rpc/assign_categories_from_keywords"

HEADERS = {
    "apikey": SERVICE_ROLE,
    "Authorization": f"Bearer {SERVICE_ROLE}",
    "Content-Type": "application/json",
}

RSS_SOURCES = [
    ("BOE", "https://www.boe.es/diario_boe/rss.php"),
]

KEYWORDS = re.compile(r"(inteligencia artificial|machine learning|ux|diseño|realidad (virtual|aumentada)|kubernetes|aws|azure|datos|bi)\b", re.I)

def parse_rss(url: str):
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    root = ET.fromstring(r.content)
    items = []
    for it in root.findall(".//item"):
        title = (it.findtext("title") or "").strip()
        link  = (it.findtext("link") or "").strip()
        desc  = (it.findtext("description") or "").strip()
        pub   = (it.findtext("pubDate") or "")
        items.append((title, link, desc, pub))
    return items

def parse_dates(s):
    try:
        return dt.datetime.strptime(s, "%a, %d %b %Y %H:%M:%S %Z")
    except Exception:
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
        "p_url": url,
        "p_status": "open",
        "p_budget": None,
        "p_currency": "EUR",
        "p_entity": None,
        "p_cpv": None,
        "p_country": "ES",
        "p_region": None,
        "p_published": published_at.isoformat(),
        "p_deadline": None
    }
    r = requests.post(RPC_UPSERT_TENDER, headers=HEADERS, json=payload, timeout=30)
    r.raise_for_status()
    return r.json()[0] if isinstance(r.json(), list) else r.json()

def assign_categories(tender_id):
    r = requests.post(RPC_ASSIGN_CATS, headers=HEADERS, json={"p_tender_id": tender_id}, timeout=30)
    r.raise_for_status()

def main():
    for source_code, feed in RSS_SOURCES:
        for title, link, desc, pub in parse_rss(feed):
            text = f"{title}\n{desc}"
            if not KEYWORDS.search(text):
                continue
            published = parse_dates(pub)
            external_id = link or (title[:32] + str(abs(hash(text))%10**8))
            tid = upsert_tender(source_code, external_id, title, desc, None, link, published)
            assign_categories(tid)
            print("Upserted:", tid, title)

if __name__ == "__main__":
    main()
