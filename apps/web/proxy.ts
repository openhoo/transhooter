import type { NextRequest } from "next/server";
import { NextResponse } from "next/server";

function generateCsrfToken(): string {
  const bytes = crypto.getRandomValues(new Uint8Array(32));
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

export function proxy(request: NextRequest): NextResponse {
  const response = NextResponse.next();
  if (!request.cookies.has("csrf")) {
    const token = generateCsrfToken();
    response.cookies.set("csrf", token, {
      httpOnly: false,
      secure: process.env.APP_ENV !== "development" && process.env.APP_ENV !== "test",
      sameSite: "strict",
      path: "/",
    });
  }
  response.headers.set("cache-control", "no-store, max-age=0");
  response.headers.set("referrer-policy", "no-referrer");
  return response;
}

export const config = {
  matcher: ["/((?!_next/static|_next/image|favicon.ico).*)"],
};
