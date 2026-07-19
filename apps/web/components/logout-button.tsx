"use client";

import { useState } from "react";

import { api } from "@/lib/browser-api";

export function LogoutButton() {
  const [pending, setPending] = useState(false);
  const [error, setError] = useState("");
  async function logout(): Promise<void> {
    setPending(true);
    setError("");
    try {
      await api("/api/auth/logout", { method: "POST", body: "{}" });
      window.location.replace("/sign-in");
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Sign out could not be completed");
      setPending(false);
    }
  }
  return (
    <div className="logoutControl">
      <button
        aria-busy={pending}
        className="button secondary"
        disabled={pending}
        type="button"
        onClick={() => {
          void logout();
        }}
      >
        {pending ? "Signing out…" : "Sign out"}
      </button>
      {error && (
        <span className="error" role="alert">
          {error}
        </span>
      )}
    </div>
  );
}
