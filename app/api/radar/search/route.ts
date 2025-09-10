import { NextResponse } from "next/server";

export const dynamic = "force-dynamic";

export async function GET(req: Request) {
  const { searchParams } = new URL(req.url);
  const q = searchParams.get("q");
  const cat = searchParams.get("cat");
  const src = searchParams.get("src");
  const limit = Number(searchParams.get("limit") || 50);
  const offset = Number(searchParams.get("offset") || 0);

  const url = `${process.env.SUPABASE_URL}/rest/v1/rpc/search_tenders`;
  const body = { q, cat_slug: cat, src_code: src, limit_n: limit, offset_n: offset };

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
