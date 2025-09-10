export const dynamic = 'force-dynamic';

async function fetchItems(q?: string) {
  const base = process.env.NEXT_PUBLIC_BASE_URL || 'http://localhost:3000';
  const url = new URL('/api/radar/search', base);
  if (q) url.searchParams.set('q', q);
  const res = await fetch(url.toString(), { cache: 'no-store' });
  if (!res.ok) return [];
  const { items } = await res.json();
  return items || [];
}

export default async function RadarPage({ searchParams }: { searchParams?: { q?: string } }) {
  const q = searchParams?.q || '';
  const items = await fetchItems(q);

  return (
    <main className="max-w-5xl mx-auto p-6 space-y-6">
      <h1 className="text-2xl font-semibold">Radar Tecnológico</h1>
      <form className="flex gap-2">
        <input name="q" defaultValue={q} placeholder="Buscar IA, UX, VR, datos..." className="border px-3 py-2 rounded w-full" />
        <button className="px-4 py-2 border rounded">Buscar</button>
      </form>
      {!items.length && <p className="text-sm">Sin resultados (¿has cargado datos en Supabase?).</p>}
      <ul className="space-y-3">
        {items.map((t: any) => (
          <li key={t.id} className="border rounded p-4">
            <a href={t.url} target="_blank" rel="noreferrer" className="font-medium underline">{t.title}</a>
            <div className="text-sm opacity-70">
              {t.source_name} · {t.entity || "—"} · {t.budget_amount ? `${t.budget_amount} ${t.currency}` : "Sin presupuesto"}
            </div>
            <p className="mt-2 text-sm">{t.summary}</p>
            <div className="text-xs opacity-60 mt-1">{t.deadline_at ? `Cierre: ${new Date(t.deadline_at).toLocaleDateString()}` : ""}</div>
          </li>
        ))}
      </ul>
    </main>
  );
}
