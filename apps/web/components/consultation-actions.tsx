"use client";

import { AlertCircle, Ban, Loader2, Lock, MailCheck, MailPlus } from "lucide-react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import type { FormEvent, ReactNode } from "react";
import { useEffect, useRef, useState } from "react";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { api } from "@/lib/browser/browser-api";

type ProviderProfileOption = { id: string; name: string; revision: number };
type NewConsultationFormProps = { profiles: ReadonlyArray<ProviderProfileOption> };
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
  const pendingSubmission = useRef<{ fingerprint: string; key: string } | null>(null);

  if (profiles.length === 0) {
    return (
      <Card aria-labelledby="no-provider-profiles">
        <CardHeader>
          <CardTitle id="no-provider-profiles" className="font-serif text-xl">
            No provider profiles are available
          </CardTitle>
          <CardDescription className="leading-relaxed">
            Review the language catalog and refresh provider capabilities before inviting a
            customer.
          </CardDescription>
        </CardHeader>
        <CardFooter>
          <Button render={<Link href="/admin/languages" />} variant="outline">
            Review provider languages
          </Button>
        </CardFooter>
      </Card>
    );
  }

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setBusy(true);
    setError("");
    const values = new FormData(event.currentTarget);
    const payload = {
      customerEmail: String(values.get("email") ?? ""),
      customerName: String(values.get("name") ?? ""),
      providerProfileId: String(values.get("profile") ?? ""),
    };
    const fingerprint = JSON.stringify(payload);
    let pending = pendingSubmission.current;
    if (pending?.fingerprint !== fingerprint) {
      pending = { fingerprint, key: crypto.randomUUID() };
      pendingSubmission.current = pending;
    }

    try {
      const result = await api<{ id: string }>("/api/consultations", {
        method: "POST",
        body: JSON.stringify({ ...payload, creationIdempotencyKey: pending.key }),
      });
      pendingSubmission.current = null;
      router.push(`/consultations/${result.id}/lobby`);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Consultation could not be created");
      setBusy(false);
    }
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle className="font-serif text-xl">Consultation details</CardTitle>
        <CardDescription className="leading-relaxed">
          The customer receives an email invitation with a secure sign-in link.
        </CardDescription>
      </CardHeader>
      <form onSubmit={(event) => void submit(event)}>
        <CardContent className="flex flex-col gap-5">
          {error && (
            <Alert variant="destructive">
              <AlertCircle className="size-4" aria-hidden="true" />
              <AlertTitle>Consultation could not be created</AlertTitle>
              <AlertDescription>{error}</AlertDescription>
            </Alert>
          )}
          <div className="flex flex-col gap-2">
            <Label htmlFor="name">Customer name</Label>
            <Input
              id="name"
              name="name"
              autoComplete="name"
              placeholder="Full name"
              required
              disabled={busy}
            />
          </div>
          <div className="flex flex-col gap-2">
            <Label htmlFor="email">Customer email</Label>
            <Input
              id="email"
              name="email"
              type="email"
              autoComplete="email"
              placeholder="customer@example.com"
              required
              disabled={busy}
            />
          </div>
          <div className="flex flex-col gap-2">
            <Label htmlFor="profile">Translation provider profile</Label>
            <select
              id="profile"
              name="profile"
              required
              disabled={busy}
              className="h-9 w-full rounded-lg border border-input bg-transparent px-3 text-sm outline-none focus-visible:border-ring focus-visible:ring-3 focus-visible:ring-ring/50 disabled:cursor-not-allowed disabled:opacity-50"
            >
              {profiles.map((profile) => (
                <option key={`${profile.id}:${String(profile.revision)}`} value={profile.id}>
                  {profile.name} · revision {profile.revision}
                </option>
              ))}
            </select>
            <p className="flex items-start gap-1.5 text-xs leading-relaxed text-muted-foreground">
              <Lock className="mt-0.5 size-3 shrink-0" aria-hidden="true" />
              The chosen revision is frozen for this consultation and shown to both participants
              before consent.
            </p>
          </div>
        </CardContent>
        <CardFooter className="flex-col items-stretch gap-2 pt-4">
          <Button className="w-full" disabled={busy} type="submit">
            {busy ? (
              <>
                <Loader2 className="size-4 animate-spin" aria-hidden="true" />
                Creating consultation…
              </>
            ) : (
              "Create and send invitation"
            )}
          </Button>
          <p className="text-center text-xs text-muted-foreground">
            Retrying the same details will not create a duplicate consultation.
          </p>
        </CardFooter>
      </form>
    </Card>
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
  const confirmationRef = useRef<HTMLButtonElement>(null);
  const triggerRef = useRef<HTMLButtonElement>(null);
  const restoreTrigger = useRef(false);

  useEffect(() => {
    if (confirming) confirmationRef.current?.focus();
    else if (restoreTrigger.current) {
      restoreTrigger.current = false;
      triggerRef.current?.focus();
    }
  }, [confirming]);

  async function run() {
    setBusy(true);
    setError("");
    setSuccess("");
    try {
      await api(`/api/consultations/${id}/${action}`, { method: "POST", body: "{}" });
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
      <div className="min-w-64 rounded-lg border border-destructive/30 bg-destructive/5 p-3">
        <p id={`cancel-confirmation-${id}`} className="text-sm leading-relaxed">
          Cancel the consultation with {contextLabel}? Its invitation will stop working.
        </p>
        <div className="mt-3 flex flex-wrap justify-end gap-2">
          <Button
            ref={confirmationRef}
            variant="outline"
            disabled={busy}
            type="button"
            onClick={() => {
              restoreTrigger.current = true;
              setConfirming(false);
            }}
          >
            Keep consultation
          </Button>
          <Button
            aria-describedby={`cancel-confirmation-${id}`}
            variant="destructive"
            disabled={busy}
            type="button"
            onClick={() => void run()}
          >
            {busy ? (
              <>
                <Loader2 className="size-4 animate-spin" aria-hidden="true" />
                Cancelling…
              </>
            ) : (
              <>
                <Ban className="size-4" aria-hidden="true" />
                Confirm cancellation
              </>
            )}
          </Button>
        </div>
        {error && (
          <p className="mt-2 text-sm text-destructive" role="alert">
            {error}
          </p>
        )}
      </div>
    );
  }

  return (
    <div>
      <Button
        ref={triggerRef}
        aria-label={`${action === "cancel" ? "Cancel" : "Resend invitation for"} ${contextLabel}`}
        variant={danger ? "destructive" : "ghost"}
        size="sm"
        disabled={busy}
        type="button"
        onClick={() => {
          if (action === "cancel") {
            setConfirming(true);
            setError("");
            setSuccess("");
          } else void run();
        }}
      >
        {busy ? (
          <Loader2 className="size-4 animate-spin" aria-hidden="true" />
        ) : action === "resend" ? (
          <MailPlus className="size-4" aria-hidden="true" />
        ) : null}
        {busy ? "Working…" : children}
      </Button>
      {success && (
        <p className="mt-1 text-xs text-verified" role="status">
          <MailCheck className="mr-1 inline size-3" aria-hidden="true" />
          {success}
        </p>
      )}
      {error && (
        <p className="mt-1 text-xs text-destructive" role="alert">
          {error}
        </p>
      )}
    </div>
  );
}
