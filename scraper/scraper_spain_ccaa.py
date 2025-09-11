# scraper/scraper_spain_ccaa.py
# ------------------------------------------------------------
# Lector genérico CCAA/Locales (RSS/ATOM/JSON) -> Supabase.
# Requiere:
#   - SUPABASE_URL
#   - SUPABASE_SERVICE_ROLE
#   - CCAA_FEEDS   (lista separada por comas de fuentes)
#
# Formato de cada fuente en CCAA_FEEDS:
#   codigo|nombre|tipo|url
#   - tipo: RSS, ATOM, JSON
#
# Ejemplos (pon en Secrets, separado por comas):
#   CAT|Generalitat de Catalunya|RSS|https://contractaciopublica.gencat.cat/ecofin_pscp/AppJava/notice/searchRSS.do
#   MAD|Comunidad de Madrid|RSS|https://www.comunidad.madrid/contratos-publicos/rss
#   ARA|Gobierno de Aragón|JSON|https://opendata.aragon.es/...(endpoint JSON con title,link,fecha)
#
# Si alguna URL devuelve HTML o 4xx/5xx, se ignora sin romper.
# ------------------------------------------------------------

import os
import re
import sys
import json
import time
import html
import datetime as dt
from typing import Optional, List, Dict, Any
import xml.etree.ElementTree as ET

import requests

UA = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Radar/1.2 (+teknovaradar)",
    "Accept": "application/atom+xml, application/rss+xml, application/json, application/xml, text/xml, */*",
    "Accept-Language": "es-ES,es;q=0.9",
    "Connection": "close",
}

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE", "")

def log(*a):
    print(*a, flush=True)

def parse_datetime(text: str) -> Optional[str]:
    if not text:
        return None
    text = text.strip()
    # RFC822
    try:
        from email.utils import parsedate_to_datetime
        d = parsedate_to_datetime(text)
        return d.astimezone(dt.timezone.utc).isoformat()
    except Exception:
        pass
    # ISO
    try:
        return dt.datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(dt.timezone.utc).isoformat()
    except Exception:
        return None

def strip_html(s: str) -> str:
    s = html.unescape(s or "")
    s = re.sub(r"<br\s*/?>", "\n", s, flags=re.I)
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()

def fetch(url: str) -> Optional[bytes]:
    try:
        r = requests.get(url, headers=UA, timeout=60, allow_redirects=True)
        log(f"[CCAA] GET {url} -> {r.status_code} bytes:{len(r.content)}")
        if r.status_code >= 400 or len(r.content) == 0:
            return None
        return r.content
    except Exception as e:
        log(f"[CCAA] Error fetch {url}: {e}")
        return None

def supabase_upsert(item: dict) -> bool:
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

def payload(source_code: str, title: str, link: str, summary: str, published_iso: Optional[str]) -> dict:
    ext = link
    m = re.search(r"([A-Za-z0-9]{6,}|\d{6,})", link)
    if m:
        ext = m.group(1)
    return {
        "p_source_code": source_code,
        "p_external_id": ext[:120],
        "p_title": (title or "")[:500],
        "p_url": link,
        "p_summary": (summary or "")[:6000] or None,
        "p_status": "open",
        "p_currency": "EUR",
        "p_budget_amount": None,
        "p_entity": None,
        "p_country": "ES",
        "p_region": None,
        "p_published_at": published_iso,
        "p_deadline_at": None,
    }

# --------- Parsers ---------

def parse_rss_atom(xml_bytes: bytes) -> List[Dict[str, str]]:
    try:
        root = ET.fromstring(xml_bytes)
    except Exception:
        return []

    out = []
    if root.tag.lower().endswith("rss"):
        ch = root.find("./channel")
        items = ch.findall("./item") if ch is not None else []
        for it in items:
            out.append({
                "title": (it.findtext("title") or "").strip(),
                "link": (it.findtext("link") or "").strip(),
                "summary": strip_html(it.findtext("description") or ""),
                "published": (it.findtext("pubDate") or "").strip(),
            })
    else:
        ns = {"a": "http://www.w3.org/2005/Atom"}
        entries = root.findall("./a:entry", ns)
        for e in entries:
            link_el = e.find("{http://www.w3.org/2005/Atom}link")
            href = link_el.attrib.get("href") if link_el is not None else (e.findtext("link") or "")
            out.append({
                "title": (e.findtext("{http://www.w3.org/2005/Atom}title") or e.findtext("title") or "").strip(),
                "link": href.strip(),
                "summary": strip_html(e.findtext("{http://www.w3.org/2005/Atom}summary") or e.findtext("{http://www.w3.org/2005/Atom}content") or ""),
                "published": (e.findtext("{http://www.w3.org/2005/Atom}updated") or e.findtext("{http://www.w3.org/2005/Atom}published") or "").strip(),
            })
    return out

def parse_json(json_bytes: bytes) -> List[Dict[str, str]]:
    try:
        data = json.loads(json_bytes.decode("utf-8", errors="ignore"))
    except Exception:
        return []

    items = []
    if isinstance(data, dict):
        # Socrata típico: {"data":[...]} o CKAN {"result":{"records":[...]}}
        if "data" in data and isinstance(data["data"], list):
            seq = data["data"]
        elif "result" in data and isinstance(data["result"], dict) and isinstance(data["result"].get("records"), list):
            seq = data["result"]["records"]
        else:
            # busca lista en alguna clave
            seq = None
            for v in data.values():
                if isinstance(v, list) and v and isinstance(v[0], dict):
                    seq = v
                    break
        if not seq:
            return []

    elif isinstance(data, list):
        seq = data
    else:
        return []

    for r in seq:
        # Heurística de campos
        title = str(r.get("title") or r.get("titulo") or r.get("name") or r.get("asunto") or "")
        link = str(r.get("link") or r.get("url") or r.get("enlace") or r.get("uri") or "")
        desc = str(r.get("description") or r.get("descripcion") or r.get("detalle") or "")
        pub = r.get("published_at") or r.get("fecha") or r.get("publication_date") or r.get("pubDate") or r.get("date")
        items.append({
            "title": title,
            "link": link,
            "summary": strip_html(desc),
            "published": str(pub or ""),
        })
    return items

# --------- Main ---------

def main():
    feeds_raw = os.environ.get("CCAA_FEEDS", "")
    if not feeds_raw.strip():
        log("[CCAA] No hay CCAA_FEEDS configurados. Saltando…")
        return

    total = 0
    for raw in feeds_raw.split(","):
        raw = raw.strip()
        if not raw:
            continue
        try:
            code, name, kind, url = [p.strip() for p in raw.split("|", 3)]
        except ValueError:
            log(f"[CCAA] Formato inválido: {raw}")
            continue

        source_code = f"ES-{code}"
        log(f"[CCAA] Leyendo {source_code} ({name}) tipo={kind} -> {url}")
        content = fetch(url)
        if not content:
            log(f"[CCAA] Sin contenido util en {url}")
            continue

        items = []
        if kind.upper() in ("RSS", "ATOM"):
            items = parse_rss_atom(content)
        elif kind.upper() == "JSON":
            items = parse_json(content)
        else:
            log(f"[CCAA] Tipo desconocido: {kind}")
            continue

        inserted = 0
        for it in items:
            t = it.get("title") or ""
            l = it.get("link") or ""
            s = it.get("summary") or ""
            p = parse_datetime(it.get("published") or "")
            if not l:
                continue
            pay = payload(source_code, t, l, s, p)
            if supabase_upsert(pay):
                inserted += 1
        total += inserted
        log(f"[CCAA] Insertados desde {code}: {inserted}")
        time.sleep(1.0)

    log(f"[DONE] TOTAL INSERTADOS (ES-CCAA): {total}")

if __name__ == "__main__":
    main()
