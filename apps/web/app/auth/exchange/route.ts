import { execute } from "@/lib/server-application";

export const dynamic = "force-dynamic";

export async function GET(request: Request): Promise<Response> {
  const response = await execute("auth.exchange.prepare", request);
  if (!response.ok) return redirect("/sign-in?error=expired");
  return redirect("/auth/verify");
}

function redirect(location: string): Response {
  return new Response(null, {
    status: 303,
    headers: {
      location,
      "cache-control": "no-store, max-age=0",
      "referrer-policy": "no-referrer",
    },
  });
}
