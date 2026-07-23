import type { Metadata } from "next";
import { VerifyForm } from "@/components/auth-forms";

export const metadata: Metadata = { title: "Verify sign in" };
export const dynamic = "force-dynamic";

export default function VerifyPage() {
  return (
    <section
      className="flex min-h-[65svh] items-center justify-center py-10"
      aria-label="Sign-in verification"
    >
      <VerifyForm />
    </section>
  );
}
