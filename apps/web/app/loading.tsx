export default function Loading() {
  return (
    <section className="flex min-h-64 items-center justify-center rounded-md border border-border bg-card p-8" role="status" aria-live="polite">
      <div className="flex items-center gap-3 text-sm text-muted-foreground"><span className="size-2 animate-pulse rounded-full bg-primary" aria-hidden="true" />Loading verified consultation data…</div>
    </section>
  );
}
