import { NextResponse } from "next/server";

export const dynamic = "force-dynamic";

export async function GET(req: Request) {
  const { searchParams } = new URL(req.url);

  const q        = searchParams.get("q");
  const src_code = searchParams.get("src");   // ej: BOE, TED
  const cat_slug = searchParams.get("cat");   // slug de la categoría
  const limit_n  = Number(searchParams.get("limit")  || 50);
  const offset_n = Number(searchParams.get("offset") || 0);

  const url  = `${process.env.SUPABASE_URL}/rest/v1/rpc/search_tenders`;
  const body = { q, cat_slug, src_code, limit_n, offset_n };

  const r = await fetch(url, {
    method: "POST",
    headers: {
      apikey: process.env.SUPABASE_SERVICE_ROLE || "",
      Authorization: `Bearer ${process.env.SUPABASE_SERVICE_ROLE}`,
      "Content-Type": "application/json",
      Prefer: "count=exact",
    },
    body: JSON.stringify(body),
    cache: "no-store",
  });

  if (!r.ok) {
    const err = await r.text();
    return NextResponse.json({ error: err }, { status: r.status });
  }

  const data = await r.json();
  return NextResponse.json({ items: data });
}
