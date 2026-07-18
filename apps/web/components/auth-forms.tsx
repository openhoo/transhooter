"use client";

import type { FormEvent } from "react";
import { useState } from "react";
import { api } from "@/lib/browser-api";

type MagicLinkState = "idle" | "sending" | "sent" | "error";
type VerifyState = "idle" | "verifying" | "error";

export function MagicLinkForm() {
  const [state, setState] = useState<MagicLinkState>("idle");

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const emailValue = new FormData(event.currentTarget).get("email");
    const email = typeof emailValue === "string" ? emailValue : "";
    setState("sending");

    try {
      await api("/api/auth/magic-link", {
        method: "POST",
        body: JSON.stringify({ email }),
      });
      setState("sent");
    } catch {
      setState("error");
    }
  }

  if (state === "sent") {
    return (
      <div className="notice" role="status">
        If this address can sign in, a private link is on its way. It expires in 15 minutes.
      </div>
    );
  }

  return (
    <form
      className="stack"
      onSubmit={(event) => {
        void submit(event);
      }}
    >
      <div className="field">
        <label htmlFor="email">Email address</label>
        <input id="email" name="email" type="email" autoComplete="email" required />
      </div>
      {state === "error" && (
        <p className="error" role="alert">
          The request could not be sent. Check your connection and try again.
        </p>
      )}
      <button className="button" disabled={state === "sending"} type="submit">
        {state === "sending" ? "Sending…" : "Email me a sign-in link"}
      </button>
    </form>
  );
}

export function VerifyForm() {
  const [state, setState] = useState<VerifyState>("idle");

  async function verify() {
    setState("verifying");

    try {
      const result = await api<{ redirectTo: string }>("/api/auth/verify", {
        method: "POST",
        body: "{}",
      });
      window.location.replace(result.redirectTo);
    } catch {
      setState("error");
    }
  }

  return (
    <div className="stack">
      <p className="lede">
        Your link has been checked but has not been used. Continue to finish signing in on this
        device.
      </p>
      {state === "error" && (
        <p className="error" role="alert">
          This sign-in cannot be completed. Request a new link.
        </p>
      )}
      <button
        className="button"
        disabled={state === "verifying"}
        type="button"
        onClick={() => {
          void verify();
        }}
      >
        {state === "verifying" ? "Verifying…" : "Continue securely"}
      </button>
    </div>
  );
}
