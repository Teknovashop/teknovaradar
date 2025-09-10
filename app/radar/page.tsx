"use client";
import { useEffect, useState } from "react";

type Tender = {
  id: string;
  title: string;
  summary?: string;
  url: string;
  status?: string;
  budget_amount?: number | null;
  currency?: string | null;
  entity?: string | null;
  country?: string | null;
  region?: string | null;
  published_at?: string | null;
  deadline_at?: string | null;
  source_code?: string | null;
  source_name?: string | null;
};

const fDate = (d?: string | null) =>
  d ? new Intl.DateTimeFormat("es-ES", { dateStyle: "medium" }).format(new Date(d)) : "";

const fMoney = (v?: number | null, c?: string | null) =>
  v != null ? new Intl.NumberFormat("es-ES", { style: "currency", currency: c || "EUR" }).format(v) : "Sin presupuesto";

export default function RadarPage() {
  const [items, setItems] = useState<Tender[]>([]);
  const [q, setQ] = useState("");
  const [src, setSrc] = useState("");
  const [cat, setCat] = useState("");
  const [page, setPage] = useState(0);
  const [loading, setLoading] = useState(false);
  const limit = 20;

  async function fetchItems(nextPage = page) {
    setLoading(true);
    const params = new URLSearchParams();
    params.set("limit", String(limit));
    params.set("offset", String(nextPage * limit));
    if (q) params.set("q", q);
    if (src) params.set("src", src);
    if (cat) params.set("cat", cat);

    const res = await fetch(`/api/radar/search?${params.toString()}`, { cache: "no-store" });
    const json = await res.json();
    setItems(json.items || []);
    setLoading(false);
  }

  useEffect(() => { fetchItems(0); }, []);

  return (
    <main>
      <header className="mb-6">
        <h1 className="text-3xl font-bold tracking-tight">Radar Tecnológico</h1>
        <p className="text-sm text-gray-600 mt-1">Licitaciones públicas de tecnología, diseño y ciencia.</p>
      </header>

      <form
        onSubmit={(e) => { e.preventDefault(); setPage(0); fetchItems(0); }}
        className="grid grid-cols-1 gap-3 md:grid-cols-[1fr,280px,220px,120px]"
      >
        <input className="input" placeholder="Buscar IA, UX, VR, datos..." value={q} onChange={(e) => setQ(e.target.value)} />
        <input className="input" placeholder="Fuente (ej. BOE, TED)" value={src} onChange={(e) => setSrc(e.target.value)} />
        <input className="input" placeholder="Categoría (slug)" value={cat} onChange={(e) => setCat(e.target.value)} />
        <button className="btn btn-primary" type="submit">Buscar</button>
      </form>

      {/* Loading */}
      {loading && (
        <div className="mt-6 text-sm text-gray-600">Cargando resultados…</div>
      )}

      {/* Empty state */}
      {!loading && !items.length && (
        <div className="mt-8 card">
          <div className="flex items-start gap-3">
            <div className="badge">Sin resultados</div>
            <div>
              <p className="text-sm text-gray-700">
                No se han encontrado licitaciones con los filtros actuales. Prueba a quitar filtros o verifica que hay datos en Supabase.
              </p>
            </div>
          </div>
        </div>
      )}

      {/* Results */}
      <ul className="mt-6 space-y-4">
        {items.map((t) => (
          <li key={t.id} className="card">
            <div className="flex flex-col gap-1">
              <a href={t.url} target="_blank" rel="noreferrer" className="text-lg font-semibold underline underline-offset-4 decoration-blue-300 hover:decoration-2">
                {t.title}
              </a>
              <div className="flex flex-wrap items-center gap-2 text-sm text-gray-600">
                <span className="badge">{t.source_name || t.source_code || "—"}</span>
                <span>·</span>
                <span>{t.entity || "—"}</span>
                <span>·</span>
                <span className="badge">{fMoney(t.budget_amount ?? null, t.currency ?? "EUR")}</span>
                {t.status && (
                  <>
                    <span>·</span>
                    <span className="badge">{t.status}</span>
                  </>
                )}
              </div>
              {t.summary && <p className="mt-2 text-sm text-gray-700">{t.summary}</p>}
              <div className="mt-1 text-xs text-gray-500">
                {t.deadline_at ? `Cierre: ${fDate(t.deadline_at)}` : ""}
              </div>
            </div>
          </li>
        ))}
      </ul>

      {/* Pagination */}
      <div className="mt-6 flex gap-2">
        <button
          className="btn"
          disabled={page === 0 || loading}
          onClick={() => { const p = Math.max(page - 1, 0); setPage(p); fetchItems(p); }}
        >
          Anterior
        </button>
        <button
          className="btn"
          disabled={loading || items.length < limit}
          onClick={() => { const p = page + 1; setPage(p); fetchItems(p); }}
        >
          Siguiente
        </button>
      </div>
    </main>
  );
}
