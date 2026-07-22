"use client";

import type { FormEvent } from "react";
import { useState } from "react";
import { AlertCircle, Loader2, MailCheck, ShieldCheck } from "lucide-react";
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

type MagicLinkState = "idle" | "sending" | "sent" | "error";
type VerifyState = "idle" | "verifying" | "error";

export function MagicLinkForm() {
  const [email, setEmail] = useState("");
  const [state, setState] = useState<MagicLinkState>("idle");

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
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
      <Card>
        <CardHeader>
          <div className="flex size-11 items-center justify-center rounded-full bg-verified/10">
            <MailCheck className="size-5 text-verified" aria-hidden="true" />
          </div>
          <CardTitle className="font-serif text-xl">Check your email</CardTitle>
          <CardDescription className="leading-relaxed">
            If this address can sign in, a private link is on its way. It expires in 15 minutes.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <p className="text-sm leading-relaxed text-muted-foreground">
            Opening the link will not activate your camera or join a consultation. You will confirm
            sign-in on this device first.
          </p>
        </CardContent>
        <CardFooter>
          <Button
            variant="outline"
            type="button"
            onClick={() => {
              setEmail("");
              setState("idle");
            }}
          >
            Use a different email
          </Button>
        </CardFooter>
      </Card>
    );
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle className="font-serif text-xl">Request a sign-in link</CardTitle>
        <CardDescription className="leading-relaxed">
          Enter the email address your consultation invitation was sent to.
        </CardDescription>
      </CardHeader>
      <form
        onSubmit={(event) => {
          void submit(event);
        }}
      >
        <CardContent className="flex flex-col gap-4">
          {state === "error" && (
            <Alert variant="destructive">
              <AlertCircle className="size-4" aria-hidden="true" />
              <AlertTitle>The link could not be sent</AlertTitle>
              <AlertDescription>
                Nothing was changed on your account. Check your connection and try again.
              </AlertDescription>
            </Alert>
          )}
          <div className="flex flex-col gap-2">
            <Label htmlFor="email">Email address</Label>
            <Input
              id="email"
              name="email"
              type="email"
              autoComplete="email"
              required
              placeholder="you@example.com"
              value={email}
              disabled={state === "sending"}
              onChange={(event) => setEmail(event.target.value)}
            />
          </div>
          <p className="text-xs leading-relaxed text-muted-foreground">
            No password is needed. The one-time link expires after 15 minutes.
          </p>
        </CardContent>
        <CardFooter className="flex-col items-stretch gap-3 pt-4">
          <Button className="w-full" disabled={state === "sending"} type="submit">
            {state === "sending" ? (
              <>
                <Loader2 className="size-4 animate-spin" aria-hidden="true" />
                Sending secure link…
              </>
            ) : (
              "Email me a sign-in link"
            )}
          </Button>
        </CardFooter>
      </form>
    </Card>
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
    <Card className="w-full max-w-md">
      <CardHeader>
        <div className="flex size-11 items-center justify-center rounded-full bg-primary/10">
          <ShieldCheck className="size-5 text-primary" aria-hidden="true" />
        </div>
        <CardTitle className="font-serif text-xl">Confirm sign-in on this device</CardTitle>
        <CardDescription className="leading-relaxed">
          Your secure link has been checked but has not been used. Confirm to finish signing in.
          Your camera and microphone will remain off.
        </CardDescription>
      </CardHeader>
      <CardContent className="flex flex-col gap-4">
        <p className="text-sm leading-relaxed text-muted-foreground">
          If you did not request this link, you can safely close this page. No one can sign in
          without confirming here.
        </p>
        {state === "error" && (
          <Alert variant="destructive">
            <AlertCircle className="size-4" aria-hidden="true" />
            <AlertTitle>Sign-in could not be completed</AlertTitle>
            <AlertDescription>Request a new link and try again.</AlertDescription>
          </Alert>
        )}
      </CardContent>
      <CardFooter>
        <Button
          className="w-full"
          disabled={state === "verifying"}
          type="button"
          onClick={() => {
            void verify();
          }}
        >
          {state === "verifying" ? (
            <>
              <Loader2 className="size-4 animate-spin" aria-hidden="true" />
              Verifying securely…
            </>
          ) : (
            "Continue securely"
          )}
        </Button>
      </CardFooter>
    </Card>
  );
}
