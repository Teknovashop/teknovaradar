"use client";
import { useEffect, useState } from "react";

const fDate = (d?: string) =>
  d ? new Intl.DateTimeFormat("es-ES", { dateStyle: "medium" }).format(new Date(d)) : "";

const fMoney = (v?: number, c?: string) =>
  v != null ? new Intl.NumberFormat("es-ES", { style: "currency", currency: c || "EUR" }).format(v) : "Sin presupuesto";

export default function RadarPage() {
  const [items, setItems] = useState<any[]>([]);
  const [q, setQ] = useState("");
  const [src, setSrc] = useState("");
  const [cat, setCat] = useState("");

  async function fetchItems() {
    const p = new URLSearchParams();
    if (q) p.set("q", q);
    if (src) p.set("src", src);
    if (cat) p.set("cat", cat);
    const res = await fetch(`/api/radar/search?${p.toString()}`, { cache: "no-store" });
    const json = await res.json();
    setItems(json.items || []);
  }

  useEffect(() => { fetchItems(); }, []);

  return (
    <main style={{ maxWidth: 900, margin: "0 auto", padding: 16 }}>
      <h1 style={{ fontSize: 28, fontWeight: 700, marginBottom: 12 }}>Radar Tecnológico</h1>

      <div style={{ display: "grid", gap: 8, gridTemplateColumns: "2fr 1fr 1fr auto", marginBottom: 16 }}>
        <input placeholder="Buscar IA, UX, VR, datos..." value={q} onChange={e => setQ(e.target.value)} />
        <input placeholder="Fuente (ej. BOE, TED)" value={src} onChange={e => setSrc(e.target.value)} />
        <input placeholder="Categoría (slug)" value={cat} onChange={e => setCat(e.target.value)} />
        <button onClick={fetchItems}>Buscar</button>
      </div>

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
            <div style={{ opacity: 0.6, fontSize: 12 }}>{t.deadline_at ? `Cierre: ${fDate(t.deadline_at)}` : ""}</div>
          </li>
        ))}
      </ul>
    </main>
  );
}
