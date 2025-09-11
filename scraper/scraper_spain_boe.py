# scraper/scraper_spain_boe.py
# ------------------------------------------------------------
# Lector BOE (RSS/ATOM configurables) -> Supabase (RPC radar.upsert_tender)
# Requiere variables de entorno:
#   - SUPABASE_URL
#   - SUPABASE_SERVICE_ROLE   (Service Role Key)
#   - BOE_FEEDS               (lista separada por comas de RSS/ATOM)
#
# Sugerencias para BOE_FEEDS (ponlo en GitHub Actions Secrets):
#   https://www.boe.es/diario_boe/rss.php?seccion=V
#   https://www.boe.es/diario_boe/rss.php?tipo=B
# ------------------------------------------------------------

import os
import re
import sys
import json
import time
import html
import datetime as dt
from typing import List, Optional
import xml.etree.ElementTree as ET

import requests

UA = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Radar/1.2 (+teknovaradar)",
    "Accept": "application/atom+xml, application/rss+xml, application/xml, text/xml, */*",
    "Accept-Language": "es-ES,es;q=0.9",
    "Connection": "close",
}

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE", "")
BOE_FEEDS = [u.strip() for u in os.environ.get("BOE_FEEDS", "").split(",") if u.strip()]

SOURCE_CODE = "ES-BOE"
SOURCE_NAME = "Boletín Oficial del Estado"

def log(*a):
    print(*a, flush=True)

def parse_datetime(text: str) -> Optional[str]:
    if not text:
        return None
    text = text.strip()
    # Intenta RFC822 (pubDate RSS) p.ej: "Wed, 10 Sep 2025 00:00:00 +0200"
    try:
        from email.utils import parsedate_to_datetime
        d = parsedate_to_datetime(text)
        return d.astimezone(dt.timezone.utc).isoformat()
    except Exception:
        pass
    # Intenta ISO
    try:
        return dt.datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(dt.timezone.utc).isoformat()
    except Exception:
        return None

def get_text(elem: Optional[ET.Element], path: str) -> str:
    if elem is None:
        return ""
    x = elem.find(path)
    return (x.text or "").strip() if x is not None else ""

def strip_html(s: str) -> str:
    s = html.unescape(s or "")
    s = re.sub(r"<br\s*/?>", "\n", s, flags=re.I)
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()

def fetch(url: str) -> Optional[bytes]:
    try:
        r = requests.get(url, headers=UA, timeout=60, allow_redirects=True)
        log(f"[BOE] GET {url} -> {r.status_code} bytes:{len(r.content)}")
        if r.status_code >= 400 or len(r.content) == 0:
            return None
        head = r.content[:200].lower()
        if head.startswith(b"<!doctype html") or b"<html" in head:
            # Es HTML (no RSS/ATOM)
            return None
        return r.content
    except Exception as e:
        log(f"[BOE] Error fetch {url}: {e}")
        return None

def supabase_upsert(item: dict) -> bool:
    """Llama a radar.upsert_tender (schema radar) vía PostgREST."""
    if not (SUPABASE_URL and SUPABASE_KEY):
        log("[ERR] Falta SUPABASE_URL o SUPABASE_SERVICE_ROLE")
        return False
    url = f"{SUPABASE_URL}/rest/v1/rpc/radar.upsert_tender"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }
    try:
        r = requests.post(url, headers=headers, json=item, timeout=60)
        if r.status_code >= 300:
            log("[SB] ERROR", r.status_code, r.text[:300])
            return False
        return True
    except Exception as e:
        log("[SB] EXC", e)
        return False

def build_payload(title: str, link: str, summary: str, published_iso: Optional[str]) -> dict:
    # external_id: intenta coger un identificador de la URL
    ext = link
    m = re.search(r"id=([A-Za-z0-9\-\._]+)", link)
    if m:
        ext = m.group(1)
    return {
        "p_source_code": SOURCE_CODE,
        "p_external_id": ext[:120],
        "p_title": title[:500],
        "p_url": link,
        "p_summary": summary[:6000] if summary else None,
        "p_status": "open",
        "p_currency": "EUR",
        "p_budget_amount": None,
        "p_entity": None,
        "p_country": "ES",
        "p_region": None,
        "p_published_at": published_iso,
        "p_deadline_at": None,
    }

def process_feed(url: str) -> int:
    raw = fetch(url)
    if not raw:
        log(f"[BOE] No XML válido en: {url}")
        return 0

    try:
        root = ET.fromstring(raw)
    except Exception as e:
        log("[BOE] XML parse error:", e)
        return 0

    # Soporta <rss><channel><item> y <feed><entry>
    total = 0
    if root.tag.lower().endswith("rss"):
        channel = root.find("./channel")
        items = channel.findall("./item") if channel is not None else []
        for it in items:
            title = get_text(it, "title")
            link = get_text(it, "link")
            pub = get_text(it, "pubDate")
            desc = get_text(it, "description")
            payload = build_payload(title, link, strip_html(desc), parse_datetime(pub))
            if supabase_upsert(payload):
                total += 1
    else:
        # Atom
        ns = {"a": "http://www.w3.org/2005/Atom"}
        entries = root.findall("./a:entry", ns)
        for e in entries:
            title = get_text(e, "{http://www.w3.org/2005/Atom}title") or get_text(e, "title")
            # En Atom, <link href="...">
            link_el = e.find("{http://www.w3.org/2005/Atom}link")
            href = link_el.attrib.get("href") if link_el is not None else get_text(e, "link")
            when = get_text(e, "{http://www.w3.org/2005/Atom}updated") or get_text(e, "{http://www.w3.org/2005/Atom}published")
            summ = get_text(e, "{http://www.w3.org/2005/Atom}summary") or get_text(e, "{http://www.w3.org/2005/Atom}content")
            payload = build_payload(title, href, strip_html(summ), parse_datetime(when))
            if supabase_upsert(payload):
                total += 1

    log(f"[BOE] Insertados desde feed: {total}")
    return total

def main():
    if not BOE_FEEDS:
        log("[BOE] No hay BOE_FEEDS configurados. Nada que hacer.")
        return
    inserted = 0
    for u in BOE_FEEDS:
        try:
            inserted += process_feed(u)
            time.sleep(1.0)
        except Exception as e:
            log("[BOE] Error en feed", u, e)
    log(f"[DONE] TOTAL INSERTADOS (ES-BOE): {inserted}")

if __name__ == "__main__":
    main()
