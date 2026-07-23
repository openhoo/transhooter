import { KeyRound, ShieldCheck, VideoOff } from "lucide-react";
import type { Metadata } from "next";
import { MagicLinkForm } from "@/components/auth-forms";

export const metadata: Metadata = { title: "Sign in" };

export default function SignInPage() {
  return (
    <div className="mx-auto flex w-full max-w-6xl flex-col items-center gap-10 py-8 md:flex-row md:items-start md:gap-16 md:py-16">
      <section className="w-full max-w-md md:flex-1" aria-labelledby="sign-in-title">
        <p className="text-xs font-semibold uppercase tracking-[0.18em] text-primary">
          Private consultation
        </p>
        <h1
          id="sign-in-title"
          className="mt-3 text-balance text-3xl font-semibold tracking-tight md:text-4xl"
        >
          Sign in to your consultation
        </h1>
        <p className="mt-4 text-pretty leading-relaxed text-muted-foreground">
          Transhooter connects you and your consultant with live video, spoken interpretation, and
          translated captions — even when you speak different languages.
        </p>
        <ul className="mt-8 flex flex-col gap-5">
          <li className="flex items-start gap-3">
            <KeyRound className="mt-0.5 size-5 shrink-0 text-primary" aria-hidden="true" />
            <div>
              <p className="text-sm font-semibold">No password required</p>
              <p className="text-sm leading-relaxed text-muted-foreground">
                We email you a secure one-time link that expires after 15 minutes.
              </p>
            </div>
          </li>
          <li className="flex items-start gap-3">
            <VideoOff className="mt-0.5 size-5 shrink-0 text-primary" aria-hidden="true" />
            <div>
              <p className="text-sm font-semibold">Opening the link does not start a call</p>
              <p className="text-sm leading-relaxed text-muted-foreground">
                Your camera and microphone stay off until you review your devices and choose to
                join.
              </p>
            </div>
          </li>
          <li className="flex items-start gap-3">
            <ShieldCheck className="mt-0.5 size-5 shrink-0 text-verified" aria-hidden="true" />
            <div>
              <p className="text-sm font-semibold">Recording requires your consent</p>
              <p className="text-sm leading-relaxed text-muted-foreground">
                You will review exactly what is recorded before entering the consultation.
              </p>
            </div>
          </li>
        </ul>
      </section>
      <section className="w-full max-w-md md:flex-1" aria-label="Sign-in form">
        <MagicLinkForm />
      </section>
    </div>
  );
}
