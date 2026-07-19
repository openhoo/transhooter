import { execute } from "@/lib/server-application";

export const dynamic = "force-dynamic";

type Match = { operation: string; params: Record<string, string> };

const staticOperations: Readonly<Record<string, string>> = {
  "POST auth/magic-link": "auth.magicLink.request",
  "POST auth/verify": "auth.exchange.verify",
  "POST auth/archive-delete-reauth": "auth.archiveDeleteReauth.request",
  "POST auth/logout": "auth.logout",
  "GET consultations": "consultations.list",
  "POST consultations": "consultations.create",
  "GET archives": "archives.list",
  "GET languages": "languages.catalog",
  "POST internal/capabilities": "internal.capabilities.update",
  "POST internal/heartbeat": "internal.worker.heartbeat",
  "POST internal/checkpoint": "internal.archive.checkpoint",
  "POST internal/provider-attempt": "internal.providerAttempt",
  "POST internal/archive-object": "internal.archiveObject",
  "POST internal/failure": "internal.failure",
  "POST internal/finalize": "internal.archive.finalize",
  "POST internal/archive-recording": "internal.archiveRecording",
  "POST internal/delete-drain": "internal.deleteDrain",
  "POST webhooks/livekit": "webhooks.livekit.receive",
};

const dynamicOperations: ReadonlyArray<readonly [pattern: RegExp, operation: string]> = [
  [/^GET consultations\/(?<id>[0-9a-f-]+)$/i, "consultations.get"],
  [/^POST consultations\/(?<id>[0-9a-f-]+)\/preferences$/i, "consultations.preferences.update"],
  [/^POST consultations\/(?<id>[0-9a-f-]+)\/consent$/i, "consultations.consent.record"],
  [/^POST consultations\/(?<id>[0-9a-f-]+)\/join$/i, "consultations.join"],
  [/^POST consultations\/(?<id>[0-9a-f-]+)\/livekit-token$/i, "consultations.livekitToken"],
  [/^GET consultations\/(?<id>[0-9a-f-]+)\/room$/i, "consultations.room"],
  [/^POST consultations\/(?<id>[0-9a-f-]+)\/end$/i, "consultations.end"],
  [/^POST consultations\/(?<id>[0-9a-f-]+)\/cancel$/i, "consultations.cancel"],
  [/^POST consultations\/(?<id>[0-9a-f-]+)\/resend$/i, "consultations.invitation.resend"],
  [/^GET archives\/(?<id>[0-9a-f-]+)$/i, "archives.get"],
  [/^GET archives\/(?<id>[0-9a-f-]+)\/objects$/i, "archives.objects.list"],
  [/^POST archives\/(?<id>[0-9a-f-]+)\/download$/i, "archives.object.download"],
  [/^POST archives\/(?<id>[0-9a-f-]+)\/hold$/i, "archives.hold.update"],
  [/^POST archives\/(?<id>[0-9a-f-]+)\/delete$/i, "archives.delete"],
  [/^GET admin\/failures$/i, "admin.failures.list"],
  [/^GET admin\/languages$/i, "admin.languages.list"],
  [/^POST admin\/languages$/i, "admin.languages.update"],
];

function match(method: string, path: string): Match | null {
  const key = `${method} ${path}`;
  const operation = staticOperations[key];
  if (operation) {
    return { operation, params: {} };
  }
  for (const [pattern, operation] of dynamicOperations) {
    const result = pattern.exec(key);
    if (result) {
      return {
        operation,
        params: { ...(result.groups ?? {}) },
      };
    }
  }
  return null;
}

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

async function route(
  request: Request,
  context: { params: Promise<{ path: string[] }> },
): Promise<Response> {
  const { path } = await context.params;
  const selected = match(request.method, path.join("/"));
  if (!selected) {
    return Response.json({ code: "NOT_FOUND", message: "Route not found" }, { status: 404 });
  }
  try {
    return await execute(selected.operation, request, selected.params);
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

export const GET = route;
export const POST = route;
