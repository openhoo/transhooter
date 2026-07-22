"use client";

import { ShieldCheck } from "lucide-react";
import Link from "next/link";
import { usePathname } from "next/navigation";

import { BrandWordmark } from "@/components/brand-mark";
import { LogoutButton } from "@/components/logout-button";
import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";

type Viewer = {
  staffRole: "employee" | "admin" | null;
};

const authenticatedNavigation = [{ href: "/consultations", label: "Consultations" }] as const;
const staffNavigation = [
  { href: "/admin/languages", label: "Languages" },
  { href: "/admin/failures", label: "Failures" },
] as const;

export function AppShell({
  children,
  viewer,
}: {
  children: React.ReactNode;
  viewer: Viewer | null;
}) {
  const pathname = usePathname();
  const navigation = viewer
    ? [...authenticatedNavigation, ...(viewer.staffRole ? staffNavigation : [])]
    : [];
  const homeHref = viewer ? ("/consultations" as const) : ("/sign-in" as const);

  return (
    <div className="flex min-h-svh flex-col bg-background text-foreground">
      <header className="border-b border-border bg-card">
        <div className="mx-auto flex w-full max-w-6xl items-center justify-between gap-4 px-4 py-3 md:px-6">
          <Link href={homeHref} aria-label="Transhooter home" className="rounded-md">
            <BrandWordmark />
          </Link>
          <div className="flex items-center gap-3">
            <Badge
              variant="outline"
              className="hidden items-center gap-1.5 border-border text-muted-foreground sm:flex"
            >
              <ShieldCheck className="size-3.5 text-verified" aria-hidden="true" />
              Private consultation service
            </Badge>
            {viewer?.staffRole && (
              <Badge variant="secondary">
                {viewer.staffRole === "admin" ? "Administrator" : "Employee"}
              </Badge>
            )}
          </div>
        </div>
        {viewer && (
          <div className="mx-auto flex w-full max-w-6xl items-end justify-between gap-3 px-4 md:px-6">
            <nav aria-label="Primary" className="flex min-w-0 items-center gap-1 overflow-x-auto">
              {navigation.map((item) => {
                const active = pathname === item.href || pathname.startsWith(`${item.href}/`);
                return (
                  <Link
                    key={item.href}
                    href={item.href}
                    aria-current={active ? "page" : undefined}
                    className={cn(
                      "whitespace-nowrap border-b-2 px-3 py-2.5 text-sm font-medium transition-colors",
                      active
                        ? "border-primary text-primary"
                        : "border-transparent text-muted-foreground hover:text-foreground",
                    )}
                  >
                    {item.label}
                  </Link>
                );
              })}
            </nav>
            <LogoutButton />
          </div>
        )}
      </header>
      <main className="shell main w-full flex-1">{children}</main>
      <footer className="border-t border-border bg-card">
        <div className="mx-auto w-full max-w-6xl px-4 py-5 text-xs text-muted-foreground md:px-6">
          Transhooter · Private interpreted consultations
        </div>
      </footer>
    </div>
  );
}
