import { NextResponse } from "next/server";
import { createClient } from "@supabase/supabase-js";

const supabase = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE! // clave de servicio (solo en backend)
);

export async function GET(req: Request) {
  const { searchParams } = new URL(req.url);

  const q = searchParams.get("q") || null;
  const cat_slug = searchParams.get("cat_slug") || null;
  const src_code = searchParams.get("src_code") || null;
  const limit_n = parseInt(searchParams.get("limit") || "20", 10);
  const offset_n = parseInt(searchParams.get("offset") || "0", 10);

  const { data, error } = await supabase.rpc("radar.search_tenders", {
    q,
    cat_slug,
    src_code,
    limit_n,
    offset_n,
  });

  if (error) {
    return NextResponse.json({ error }, { status: 500 });
  }

  return NextResponse.json({ items: data });
}
