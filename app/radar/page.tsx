"use client";

import { useState, useEffect } from "react";

export default function RadarPage() {
  const [items, setItems] = useState<any[]>([]);
  const [q, setQ] = useState("");
  const [src, setSrc] = useState("");
  const [cat, setCat] = useState("");

  async function search() {
    const params = new URLSearchParams();
    if (q) params.set("q", q);
    if (src) params.set("src_code", src);
    if (cat) params.set("cat_slug", cat);

    const res = await fetch(`/api/radar/search?${params.toString()}`);
    const json = await res.json();
    setItems(json.items || []);
  }

  useEffect(() => {
    search();
  }, []);

  return (
    <div style={{ padding: "1rem" }}>
      <h1>Radar Tecnológico</h1>
      <div style={{ marginBottom: "1rem" }}>
        <input
          type="text"
          placeholder="Buscar IA, UX, VR, datos..."
          value={q}
          onChange={(e) => setQ(e.target.value)}
        />
        <input
          type="text"
          placeholder="Fuente (ej. BOE, TED)"
          value={src}
          onChange={(e) => setSrc(e.target.value)}
        />
        <input
          type="text"
          placeholder="Categoría (slug)"
          value={cat}
          onChange={(e) => setCat(e.target.value)}
        />
        <button onClick={search}>Buscar</button>
      </div>

      {items.length === 0 ? (
        <p><i>Sin resultados (¿hay datos en Supabase?).</i></p>
      ) : (
        <ul>
          {items.map((tender) => (
            <li key={tender.id} style={{ marginBottom: "1rem" }}>
              <a href={tender.url} target="_blank" rel="noopener noreferrer">
                {tender.title}
              </a>
              <div>
                {tender.source_name} · {tender.entity} ·{" "}
                {tender.budget_amount
                  ? `${tender.budget_amount} ${tender.currency}`
                  : "— Sin presupuesto"}
              </div>
              <p>{tender.summary}</p>
              <small>
                Cierre:{" "}
                {tender.deadline_at
                  ? new Date(tender.deadline_at).toLocaleDateString()
                  : "N/D"}
              </small>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
