export class ApiError extends Error {
  constructor(
    readonly status: number,
    readonly code: string | undefined,
    message: string,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

function csrfToken(): string {
  const cookieParts = document.cookie.split("; ");
  const csrfCookie = cookieParts.find((part) => part.startsWith("csrf="));
  const token = csrfCookie?.slice(5);

  if (!token) {
    throw new Error("Security token is unavailable. Reload the page and try again.");
  }

  return decodeURIComponent(token);
}

export async function api<T>(path: string, init: RequestInit = {}): Promise<T> {
  const method = (init.method ?? "GET").toUpperCase();
  const headers = new Headers(init.headers);
  headers.set("accept", "application/json");

  const isReadOnlyMethod = method === "GET" || method === "HEAD";
  if (!isReadOnlyMethod) {
    headers.set("content-type", "application/json");
    headers.set("x-csrf-token", csrfToken());
  }

  const response = await fetch(path, {
    ...init,
    headers,
    credentials: "same-origin",
    cache: "no-store",
  });

  let body: unknown;
  if (response.status !== 204) {
    body = await response.json();
  }

  if (!response.ok) {
    const payload =
      body && typeof body === "object"
        ? (body as { code?: unknown; message?: unknown })
        : undefined;
    const code = typeof payload?.code === "string" ? payload.code : undefined;
    const message =
      typeof payload?.message === "string"
        ? payload.message
        : `Request failed (${String(response.status)})`;
    throw new ApiError(response.status, code, message);
  }

  return body as T;
}
