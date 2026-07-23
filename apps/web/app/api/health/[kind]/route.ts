import { ready } from "@/lib/server-application";

export async function GET(
  _request: Request,
  context: { params: Promise<{ kind: string }> },
): Promise<Response> {
  const { kind } = await context.params;
  if (kind === "live")
    return Response.json({ status: "ok" }, { headers: { "cache-control": "no-store" } });
  if (kind !== "ready") return Response.json({ status: "not_found" }, { status: 404 });
  try {
    const isReady = await ready();
    return Response.json(
      { status: isReady ? "ready" : "unavailable" },
      { status: isReady ? 200 : 503, headers: { "cache-control": "no-store" } },
    );
  } catch {
    return Response.json(
      { status: "unavailable" },
      { status: 503, headers: { "cache-control": "no-store" } },
    );
  }
}
