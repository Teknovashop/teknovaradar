import { headers } from "next/headers";

export const dynamic = "force-dynamic";

const fDate = (d?: string) =>
  d ? new Intl.DateTimeFormat("es-ES", { dateStyle: "medium" }).format(new Date(d)) : "";

const fMoney = (v?: number, c?: string) =>
  v != null ? new Intl.NumberFormat("es-ES", { style: "currency", currency: c || "EUR" }).format(v) : "Sin presupuesto";

async function fetchItems(searchParams?: { q?: string; src?: string; cat?: string }) {
  const h = headers();
  const host = h.get("x-forwarded-host") || h.get("host") || "localhost:3000";
  const proto = (h.get("x-forwarded-proto") || "https") + "://";
  const base = `${proto}${host}`;
  const url = new URL("/api/radar/search", base);
  if (searchParams?.q) url.searchParams.set("q", searchParams.q);
  if (searchParams?.src) url.searchParams.set("src", searchParams.src);
  if (searchParams?.cat) url.searchParams.set("cat", searchParams.cat);

  const res = await fetch(url.toString(), { cache: "no-store" });
  if (!res.ok) {
    console.error("API error", await res.text());
    return [];
  }
  const { items } = await res.json();
  return items || [];
}

export default async function RadarPage({ searchParams }: { searchParams?: { q?: string; src?: string; cat?: string } }) {
  const items = await fetchItems(searchParams);

  return (
    <main className="max-w-5xl mx-auto p-6 space-y-6">
      <h1 className="text-2xl font-semibold">Radar Tecnológico</h1>

      <form className="grid gap-2 md:grid-cols-[1fr,200px,200px,120px]">
        <input name="q" defaultValue={searchParams?.q || ""} placeholder="Buscar IA, UX, VR, datos..." className="border px-3 py-2 rounded w-full" />
        <input name="src" defaultValue={searchParams?.src || ""} placeholder="Fuente (ej. BOE, TED)" className="border px-3 py-2 rounded w-full" />
        <input name="cat" defaultValue={searchParams?.cat || ""} placeholder="Categoría (slug)" className="border px-3 py-2 rounded w-full" />
        <button className="px-4 py-2 border rounded">Buscar</button>
      </form>

      {!items.length && <p className="text-sm">Sin resultados (¿hay datos en Supabase?).</p>}

      <ul className="space-y-3">
        {items.map((t: any) => (
          <li key={t.id} className="border rounded p-4">
            <a href={t.url} target="_blank" rel="noreferrer" className="font-medium underline">
              {t.title}
            </a>
            <div className="text-sm opacity-70">
              {t.source_name} · {t.entity || "—"} · {fMoney(t.budget_amount, t.currency)}
            </div>
            <p className="mt-2 text-sm">{t.summary}</p>
            <div className="text-xs opacity-60 mt-1">{t.deadline_at ? `Cierre: ${fDate(t.deadline_at)}` : ""}</div>
          </li>
        ))}
      </ul>
    </main>
  );
}
