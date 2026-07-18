"use client";

import { useRouter } from "next/navigation";
import type { FormEvent, ReactNode } from "react";
import { useState } from "react";
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
  danger?: boolean;
};

export function NewConsultationForm({ profiles }: NewConsultationFormProps) {
  const router = useRouter();
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);

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
  danger = false,
}: ConsultationActionProps) {
  const router = useRouter();
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");

  async function run() {
    setBusy(true);
    setError("");

    try {
      await api(`/api/consultations/${id}/${action}`, {
        method: "POST",
        body: "{}",
      });
      router.refresh();
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Action failed");
      setBusy(false);
    }
  }

  return (
    <div>
      <button
        className={`button ${danger ? "danger" : "secondary"}`}
        disabled={busy}
        type="button"
        onClick={() => {
          void run();
        }}
      >
        {busy ? "Working…" : children}
      </button>
      {error && (
        <p className="error" role="alert">
          {error}
        </p>
      )}
    </div>
  );
}
