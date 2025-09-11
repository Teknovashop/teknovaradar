# scraper/scraper_spain_ccaa.py
import os, sys, json, uuid, requests, datetime as dt
from typing import List, Dict, Optional

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE = os.environ.get("SUPABASE_SERVICE_ROLE")

UA = {
    "User-Agent": "Mozilla/5.0 Radar-CCAA/1.0",
    "Accept": "application/rss+xml, application/xml, text/xml, */*",
    "Accept-Language": "es-ES,es;q=0.9",
}

SESSION = requests.Session()
SESSION.headers.update(UA)

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
    r = SESSION.post(url, headers=headers, data=json.dumps(rows), timeout=60)
    if r.status_code not in (200, 201):
        print("[CCAA] Supabase insert error:", r.status_code, r.text[:500], file=sys.stderr)
        return 0
    return len(r.json())

def main():
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE:
        print("[CCAA] Faltan credenciales Supabase")
        return

    total = 0
    rows: List[Dict] = []

    # Ejemplo: PSCP (Catalunya) – puede devolver timeout/redirect desde runners.
    feeds = [
        ("ES-CAT", "Generalitat de Catalunya", "https://contractaciopublica.gencat.cat/ecofin_pscp/AppJava/notice/searchRSS.do"),
    ]

    for code, name, url in feeds:
        try:
            print(f"[CCAA] Leyendo {code} ({name}) -> {url}")
            r = SESSION.get(url, timeout=60, allow_redirects=True)
            if r.status_code != 200 or not r.content:
                print(f"[CCAA] Error fetch {url}: {r.status_code}")
                continue
            # Muchos portales devuelven HTML; este archivo queda como placeholder.
            # Aquí podrías parsear el RSS XML cuando sea válido.
            # Para no insertar ruido, de momento no generamos filas.
            # (Dejamos la integración ‘best effort’ hasta que confirmemos un feed estable).
        except Exception as e:
            print(f"[CCAA] Error fetch {url}: {e}")
            continue

    ins = supa_insert(rows)
    total += ins
    print(f"[DONE] TOTAL INSERTADOS (ES-CCAA): {total}")

if __name__ == "__main__":
    main()
