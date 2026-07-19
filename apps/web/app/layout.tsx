import type { Metadata } from "next";
import Link from "next/link";
import { LogoutButton } from "@/components/logout-button";
import { optionalPageViewer } from "@/lib/server-application";
import "./globals.css";

export const metadata: Metadata = {
  title: { default: "Transhooter", template: "%s · Transhooter" },
  description: "Private, recorded consultations with live translated captions and interpretation.",
  robots: { index: false, follow: false },
  referrer: "no-referrer",
};

export default async function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  const viewer = await optionalPageViewer();
  return (
    <html lang="en">
      <body>
        <header className="shell topbar">
          <Link className="brand" href={viewer ? "/consultations" : "/sign-in"}>
            Transhooter
          </Link>
          {viewer && (
            <nav className="nav" aria-label="Primary">
              <Link href="/consultations">Consultations</Link>
              {viewer.staffRole && <Link href="/admin/languages">Languages</Link>}
              {viewer.staffRole && <Link href="/admin/failures">Failures</Link>}
              <LogoutButton />
            </nav>
          )}
        </header>
        <main className="shell main">{children}</main>
      </body>
    </html>
  );
}
