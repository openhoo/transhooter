"use client";

import { useState } from "react";

import { api } from "@/lib/browser/browser-api";
import { Button } from "@/components/ui/button";

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
    <div className="logoutControl shrink-0 pb-1.5">
      <Button
        aria-busy={pending}
        disabled={pending}
        type="button"
        variant="ghost"
        onClick={() => {
          void logout();
        }}
      >
        {pending ? "Signing out…" : "Sign out"}
      </Button>
      {error && (
        <span className="error max-w-72 text-right text-xs" role="alert">
          {error}
        </span>
      )}
    </div>
  );
}
