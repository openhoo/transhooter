import Link from "next/link";

export default function NotFound() {
  return (
    <section className="mx-auto flex min-h-[24rem] max-w-2xl flex-col items-start justify-center rounded-md border border-border bg-card p-8">
      <p className="text-xs font-semibold uppercase tracking-widest text-muted-foreground">Not found</p>
      <h1 className="mt-2 font-serif text-2xl font-semibold tracking-tight text-foreground md:text-3xl">This consultation view is unavailable.</h1>
      <p className="mt-3 text-sm leading-relaxed text-muted-foreground">It may have ended, been deleted, or not belong to your account.</p>
      <Link className="mt-5 inline-flex min-h-10 items-center justify-center rounded-md bg-primary px-4 text-sm font-semibold text-primary-foreground hover:opacity-90" href="/consultations">Return to consultations</Link>
    </section>
  );
}
