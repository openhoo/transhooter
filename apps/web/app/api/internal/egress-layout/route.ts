export const dynamic = "force-dynamic";

export function GET(request: Request): Response {
  const source = new URL(request.url);
  const destination = new URL("/egress-layout", source.origin);
  destination.search = source.search;
  return Response.redirect(destination, 307);
}
