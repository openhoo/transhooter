"use client";

import { api } from "@/lib/browser-api";

export function LogoutButton() {
  async function logout(): Promise<void> {
    await api("/api/auth/logout", { method: "POST", body: "{}" });
    window.location.replace("/sign-in");
  }
  return (
    <button
      className="button secondary"
      type="button"
      onClick={() => {
        void logout();
      }}
    >
      Sign out
    </button>
  );
}
