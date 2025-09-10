"use client";
import { useEffect, useState } from "react";

const fDate = (d?: string) =>
  d ? new Intl.DateTimeFormat("es-ES", { dateStyle: "medium" }).format(new Date(d)) : "";

const fMoney = (v?: number, c?: string) =>
  v != null
    ? new Intl.NumberFormat("es-ES", { style: "currency", currency: c || "EUR" }).format(v)
    : "Sin presupuesto";

export default function RadarPage() {
  const [items, setItems] = useState<any[]>([]);
  const [q, setQ] = useState("");
  const [src, setSrc] = useState("");
  const [cat, setCat] = useState("");
  const [page, setPage] = useState(0);
  const limit = 20;

  async function fetchItems(p = page) {
    const params = new URLSearchParams();
    params.set("limit", String(limit));
    params.set("offset", String(p * limit));
    if (q)   params.set("q", q);
    if (src) params.set("src", src);
    if (cat) params.set("cat", cat);

    const res  = await fetch(`/api/radar/search?${params.toString()}`, { cache: "no-store" });
    const json = await res.json();
    setItems(json.items || []);
  }

  useEffect(() => { fetchItems(0); }, []);

  return (
    <main style={{ maxWidth: 920, margin: "0 auto", padding: 16 }}>
      <h1 style={{ fontSize: 30, fontWeight: 800, marginBottom: 12 }}>Radar Tecnológico</h1>

      <form
        onSubmit={(e) => { e.preventDefault(); setPage(0); fetchItems(0); }}
        style={{ display: "grid", gap: 8, gridTemplateColumns: "2fr 1fr 1fr auto", marginBottom: 16 }}
      >
        <input placeholder="Buscar IA, UX, VR, datos..." value={q}   onChange={e => setQ(e.target.value)} />
        <input placeholder="Fuente (ej. BOE, TED)"      value={src} onChange={e => setSrc(e.target.value)} />
        <input placeholder="Categoría (slug)"           value={cat} onChange={e => setCat(e.target.value)} />
        <button type="submit">Buscar</button>
      </form>

      {!items.length && <p><i>Sin resultados (¿hay datos en Supabase?).</i></p>}

      <ul style={{ listStyle: "disc", paddingLeft: 18 }}>
        {items.map((t: any) => (
          <li key={t.id} style={{ marginBottom: 18 }}>
            <a href={t.url} target="_blank" rel="noreferrer" style={{ fontWeight: 600, textDecoration: "underline" }}>
              {t.title}
            </a>
            <div style={{ opacity: 0.75 }}>
              {t.source_name} · {t.entity || "—"} · {fMoney(t.budget_amount, t.currency)}
            </div>
            <p style={{ marginTop: 6 }}>{t.summary}</p>
            <div style={{ opacity: 0.6, fontSize: 12 }}>
              {t.deadline_at ? `Cierre: ${fDate(t.deadline_at)}` : ""}
            </div>
          </li>
        ))}
      </ul>

      <div style={{ display: "flex", gap: 8, marginTop: 10 }}>
        <button disabled={page === 0} onClick={() => { const p = Math.max(page - 1, 0); setPage(p); fetchItems(p); }}>
          Anterior
        </button>
        <button onClick={() => { const p = page + 1; setPage(p); fetchItems(p); }}>
          Siguiente
        </button>
      </div>
    </main>
  );
}
