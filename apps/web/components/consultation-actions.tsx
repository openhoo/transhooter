"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import type { FormEvent, ReactNode } from "react";
import { useEffect, useRef, useState } from "react";
import { api } from "@/lib/browser-api";

type ProviderProfileOption = {
  id: string;
  name: string;
  revision: number;
};

type NewConsultationFormProps = {
  profiles: ReadonlyArray<ProviderProfileOption>;
};

type ConsultationActionProps = {
  id: string;
  action: "cancel" | "resend";
  children: ReactNode;
  contextLabel: string;
  danger?: boolean;
};

export function NewConsultationForm({ profiles }: NewConsultationFormProps) {
  const router = useRouter();
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);

  if (profiles.length === 0) {
    return (
      <section className="stack panel" aria-labelledby="no-provider-profiles">
        <h2 id="no-provider-profiles">No provider profiles are available</h2>
        <p className="muted">
          Review the language catalog and refresh provider capabilities before inviting a customer.
        </p>
        <Link className="button secondary" href="/admin/languages">
          Review provider languages
        </Link>
      </section>
    );
  }
  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setBusy(true);
    setError("");

    const values = new FormData(event.currentTarget);

    try {
      const result = await api<{ id: string }>("/api/consultations", {
        method: "POST",
        body: JSON.stringify({
          customerEmail: values.get("email"),
          customerName: values.get("name"),
          providerProfileId: values.get("profile"),
        }),
      });
      router.push(`/consultations/${result.id}/lobby`);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Consultation could not be created");
      setBusy(false);
    }
  }

  return (
    <form
      className="stack panel"
      onSubmit={(event) => {
        void submit(event);
      }}
    >
      <div className="field">
        <label htmlFor="name">Customer name</label>
        <input id="name" name="name" autoComplete="name" required />
      </div>
      <div className="field">
        <label htmlFor="email">Customer email</label>
        <input id="email" name="email" type="email" autoComplete="email" required />
      </div>
      <div className="field">
        <label htmlFor="profile">Translation provider profile</label>
        <select id="profile" name="profile" required>
          {profiles.map((profile) => (
            <option key={`${profile.id}:${String(profile.revision)}`} value={profile.id}>
              {profile.name} · revision {profile.revision}
            </option>
          ))}
        </select>
      </div>
      <p className="meta">
        The chosen revision is frozen for this consultation and shown to both participants before
        consent.
      </p>
      {error && (
        <p className="error" role="alert">
          {error}
        </p>
      )}
      <button className="button" disabled={busy} type="submit">
        {busy ? "Creating…" : "Create and send invitation"}
      </button>
    </form>
  );
}

export function ConsultationAction({
  id,
  action,
  children,
  contextLabel,
  danger = false,
}: ConsultationActionProps) {
  const router = useRouter();
  const [busy, setBusy] = useState(false);
  const [confirming, setConfirming] = useState(false);
  const [error, setError] = useState("");
  const [success, setSuccess] = useState("");
  const cancelConfirmationRef = useRef<HTMLButtonElement>(null);
  const cancelTriggerRef = useRef<HTMLButtonElement>(null);
  const restoreCancelTrigger = useRef(false);

  useEffect(() => {
    if (confirming) {
      cancelConfirmationRef.current?.focus();
      return;
    }

    if (restoreCancelTrigger.current) {
      restoreCancelTrigger.current = false;
      cancelTriggerRef.current?.focus();
    }
  }, [confirming]);
  async function run() {
    setBusy(true);
    setError("");
    setSuccess("");

    try {
      await api(`/api/consultations/${id}/${action}`, {
        method: "POST",
        body: "{}",
      });
      setConfirming(false);
      setSuccess(
        action === "resend"
          ? `Invitation resent for ${contextLabel}.`
          : `Consultation with ${contextLabel} cancelled.`,
      );
      router.refresh();
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Action failed");
    } finally {
      setBusy(false);
    }
  }

  if (confirming) {
    return (
      <div className="inlineConfirmation">
        <p id={`cancel-confirmation-${id}`}>
          Cancel the consultation with {contextLabel}? Its invitation will stop working.
        </p>
        <div className="actions">
          <button
            ref={cancelConfirmationRef}
            className="button secondary"
            disabled={busy}
            type="button"
            onClick={() => {
              restoreCancelTrigger.current = true;
              setConfirming(false);
            }}
          >
            Keep consultation
          </button>
          <button
            aria-describedby={`cancel-confirmation-${id}`}
            className="button danger"
            disabled={busy}
            type="button"
            onClick={() => {
              void run();
            }}
          >
            {busy ? "Cancelling…" : `Confirm cancellation for ${contextLabel}`}
          </button>
        </div>
        {error && (
          <p className="error" role="alert">
            {error}
          </p>
        )}
      </div>
    );
  }

  return (
    <div>
      <button
        ref={cancelTriggerRef}
        aria-label={`${action === "cancel" ? "Cancel" : "Resend invitation for"} ${contextLabel}`}
        className={`button ${danger ? "danger" : "secondary"}`}
        disabled={busy}
        type="button"
        onClick={() => {
          if (action === "cancel") {
            setConfirming(true);
            setError("");
            setSuccess("");
          } else {
            void run();
          }
        }}
      >
        {busy ? "Working…" : children}
      </button>
      {success && (
        <p className="notice" role="status">
          {success}
        </p>
      )}
      {error && (
        <p className="error" role="alert">
          {error}
        </p>
      )}
    </div>
  );
}
