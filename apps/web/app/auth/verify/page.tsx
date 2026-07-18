import type { Metadata } from "next";
import { VerifyForm } from "@/components/auth-forms";

export const metadata: Metadata = { title: "Verify sign in" };
export const dynamic = "force-dynamic";

export default function VerifyPage() {
  return (
    <section className="panel narrow">
      <p className="eyebrow">Final sign-in step</p>
      <h1>Continue on this device.</h1>
      <VerifyForm />
    </section>
  );
}
