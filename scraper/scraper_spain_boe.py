# scraper/scraper_spain_boe.py
import os
import re
import sys
import json
import time
import math
import uuid
import html
import datetime as dt
from typing import List, Dict, Optional

import requests
from bs4 import BeautifulSoup

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE = os.environ.get("SUPABASE_SERVICE_ROLE")

UA = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Radar-BOE/1.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9",
    "Referer": "https://www.boe.es/",
    "Connection": "keep-alive",
}

SESSION = requests.Session()
SESSION.headers.update(UA)
SESSION.timeout = 60

SOURCE_CODE = "ES-BOE"
SOURCE_NAME  = "Boletín Oficial del Estado"

# Palabras que suelen aparecer en anuncios de contratación/subvenciones/tech
KEYWORDS = [
    "licitación", "licitacion",
    "contratación", "contratacion", "contrato",
    "concurso", "adjudicación", "adjudicacion",
    "pliegos", "subvención", "subvenciones",
    "acuerdo marco", "servicio", "suministro", "equipamiento",
    "tecnolog", "software", "hardware", "informát", "digital",
    "plataforma", "sistema", "desarrollo", "mantenimiento",
    "cloud", "nube", "ciberseguridad", "redes", "datacenter",
]

def supa_insert(rows: List[Dict]) -> int:
    if not rows:
        return 0
    url = f"{SUPABASE_URL}/rest/v1/public_tenders"
    headers = {
        "apikey": SUPABASE_SERVICE_ROLE,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }
    r = SESSION.post(url, headers=headers, data=json.dumps(rows))
    if r.status_code not in (200, 201):
        print("[BOE] Supabase insert error:", r.status_code, r.text[:500], file=sys.stderr)
        return 0
    return len(r.json())

def looks_relevant(text: str) -> bool:
    t = text.lower()
    return any(k in t for k in KEYWORDS)

def fetch(url: str) -> Optional[str]:
    try:
        r = SESSION.get(url, timeout=60)
        if r.status_code == 200 and r.text:
            return r.text
        print(f"[BOE] GET {url} -> {r.status_code} bytes:{len(r.text) if r.text else 0}")
        return None
    except Exception as e:
        print(f"[BOE] Error GET {url}: {e}")
        return None

def build_boe_day_urls(day: dt.date) -> Dict[str, str]:
    # Ejemplo base visible que ya viste: https://www.boe.es/boe/dias/2025/01/01/
    base = f"https://www.boe.es/boe/dias/{day.year:04d}/{day.month:02d}/{day.day:02d}/"
    # El sumario suele estar en index.html o sumario.php; probamos ambos.
    return {
        "index": base,                   # …/YYYY/MM/DD/
        "sumario": base + "sumario.php", # …/YYYY/MM/DD/sumario.php
        "indice": base + "index.php",    # fallback adicional
    }

def parse_sumario(html_text: str, day: dt.date) -> List[Dict]:
    soup = BeautifulSoup(html_text, "lxml")

    # Buscamos el bloque “V. Anuncios”. Suele aparecer como un <h2> con “V. Anuncios”
    # y la lista de items debajo. Hacemos varias estrategias por robustez.
    section_headers = soup.find_all(["h2", "h3"], string=lambda s: s and "Anuncios" in s)
    items: List[Dict] = []

    def normalize_space(s: str) -> str:
        return re.sub(r"\s+", " ", s or "").strip()

    if not section_headers:
        # plan B: buscar un enlace de índice con texto “V. Anuncios”
        anchors = soup.find_all("a", string=lambda s: s and "Anuncios" in s)
        for a in anchors:
            # si apunta a ancla dentro de la misma página, el listado suele estar después
            parent = a.find_parent()
            for ul in parent.find_all_next(["ul", "ol"], limit=2):
                for li in ul.find_all("li", recursive=True):
                    title = normalize_space(li.get_text(" ", strip=True))
                    href = None
                    link = li.find("a", href=True)
                    if link:
                        href = link["href"]
                        if href.startswith("/"):
                            href = "https://www.boe.es" + href
                    if href and looks_relevant(title):
                        items.append({
                            "id": str(uuid.uuid4()),
                            "title": title[:500],
                            "summary": None,
                            "url": href,
                            "status": "open",
                            "budget_amount": None,
                            "currency": None,
                            "entity": None,
                            "country": "ES",
                            "region": None,
                            "published_at": f"{day.isoformat()}T00:00:00+00:00",
                            "deadline_at": None,
                            "source_code": SOURCE_CODE,
                            "source_name": SOURCE_NAME,
                            "category": None
                        })
        return items

    # Si encontramos el encabezado, tomamos las listas cercanas
    for h in section_headers:
        # tomamos las 2 siguientes listas como máximo
        for ul in h.find_all_next(["ul", "ol"], limit=2):
            for li in ul.find_all("li", recursive=True):
                title = normalize_space(li.get_text(" ", strip=True))
                href = None
                link = li.find("a", href=True)
                if link:
                    href = link["href"]
                    if href.startswith("/"):
                        href = "https://www.boe.es" + href
                if href and looks_relevant(title):
                    items.append({
                        "id": str(uuid.uuid4()),
                        "title": title[:500],
                        "summary": None,
                        "url": href,
                        "status": "open",
                        "budget_amount": None,
                        "currency": None,
                        "entity": None,
                        "country": "ES",
                        "region": None,
                        "published_at": f"{day.isoformat()}T00:00:00+00:00",
                        "deadline_at": None,
                        "source_code": SOURCE_CODE,
                        "source_name": SOURCE_NAME,
                        "category": None
                    })
    return items

def process_day(day: dt.date) -> List[Dict]:
    urls = build_boe_day_urls(day)
    html_text = None
    for name, url in urls.items():
        html_text = fetch(url)
        if html_text:
            break
    if not html_text:
        return []
    items = parse_sumario(html_text, day)
    return items

def main():
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE:
        print("[BOE] Faltan credenciales Supabase")
        sys.exit(0)

    today = dt.date.today()
    days_back = 7
    total_inserted = 0
    batch: List[Dict] = []

    for i in range(days_back):
        day = today - dt.timedelta(days=i)
        print(f"[BOE] Día {day.isoformat()} …")
        try:
            items = process_day(day)
            print(f"[BOE] Encontrados {len(items)} candidatos")
            batch.extend(items)
        except Exception as e:
            print(f"[BOE] Error día {day}: {e}")

    # De-duplicar por (url + title)
    seen = set()
    dedup = []
    for it in batch:
        key = (it["url"], it["title"])
        if key in seen:
            continue
        seen.add(key)
        dedup.append(it)

    print(f"[BOE] Total candidatos tras dedupe: {len(dedup)}")

    # Insertar por lotes de 200
    for i in range(0, len(dedup), 200):
        chunk = dedup[i:i+200]
        ins = supa_insert(chunk)
        total_inserted += ins
        print(f"[BOE] Insertados {ins} / {len(chunk)}")

    print(f"[DONE] TOTAL INSERTADOS (ES-BOE): {total_inserted}")

if __name__ == "__main__":
    main()
