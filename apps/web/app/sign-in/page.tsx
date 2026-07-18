import type { Metadata } from "next";
import { MagicLinkForm } from "@/components/auth-forms";

export const metadata: Metadata = { title: "Sign in" };

export default function SignInPage() {
  return (
    <section className="grid" aria-labelledby="sign-in-title">
      <div>
        <p className="eyebrow">Private consultation</p>
        <h1 id="sign-in-title">Join without another password.</h1>
        <p className="lede">
          We send one short-lived link to your email. Opening it does not start the consultation or
          turn on your camera.
        </p>
      </div>
      <div className="panel">
        <h2>Request your link</h2>
        <MagicLinkForm />
      </div>
    </section>
  );
}
