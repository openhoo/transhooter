import type { Metadata, Viewport } from "next";

import { AppShell } from "@/components/app-shell";
import { optionalPageViewer } from "@/lib/server-application";

import "./globals.css";

export const metadata: Metadata = {
  title: { default: "Transhooter", template: "%s · Transhooter" },
  description: "Private, recorded consultations with live translated captions and interpretation.",
  robots: { index: false, follow: false },
  referrer: "no-referrer",
};

export const viewport: Viewport = {
  colorScheme: "light",
  themeColor: "#294b73",
};

export default async function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  const viewer = await optionalPageViewer();

  return (
    <html lang="en" className="bg-background">
      <body>
        <AppShell viewer={viewer}>{children}</AppShell>
      </body>
    </html>
  );
}
