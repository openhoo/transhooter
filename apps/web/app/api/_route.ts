import "server-only";
import { execute } from "@/lib/server-application";

type RouteParams = Record<string, string>;
type RouteContext = { params: Promise<RouteParams> };
type AwaitableRouteParams = RouteParams | Promise<RouteParams>;

function safeErrorSummary(error: unknown): {
  name: string;
  code?: string;
  status?: number;
} {
  const summary: { name: string; code?: string; status?: number } = {
    name: error instanceof Error ? error.name : typeof error,
  };
  let current = error;
  for (let depth = 0; depth < 4 && current !== null && typeof current === "object"; depth += 1) {
    const record = current as Record<string, unknown>;
    if (summary.code === undefined && ["string", "number"].includes(typeof record.code)) {
      summary.code = String(record.code);
    }
    const status = record.statusCode ?? record.status;
    if (summary.status === undefined && typeof status === "number") {
      summary.status = status;
    }
    current = record.cause;
  }
  return summary;
}

export async function executeRoute(
  operation: string,
  request: Request,
  params: AwaitableRouteParams = {},
): Promise<Response> {
  try {
    return await execute(operation, request, await params);
  } catch (error) {
    console.error("Unhandled API request failure", safeErrorSummary(error));
    const isInvalidJson = error instanceof SyntaxError;
    const message = isInvalidJson
      ? "Request body is not valid JSON"
      : "The request could not be completed";
    const status = isInvalidJson ? 400 : 503;

    return Response.json(
      { code: "REQUEST_FAILED", message },
      {
        status,
        headers: { "cache-control": "no-store" },
      },
    );
  }
}

export function createRoute<const Operation extends string>(operation: Operation) {
  return (request: Request, context?: RouteContext): Promise<Response> =>
    executeRoute(operation, request, context?.params);
}
