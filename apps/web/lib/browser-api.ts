export type ApiError = {
  code: string;
  message: string;
};

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
    const error = body as Partial<ApiError>;
    const message = error.message ?? `Request failed (${String(response.status)})`;
    throw new Error(message);
  }

  return body as T;
}
