"use client";

type ErrorPageProps = {
  error: Error & { digest?: string };
  reset: () => void;
};

export function ErrorRecovery({ reset }: ErrorPageProps) {
  return (
    <section className="mx-auto flex min-h-[24rem] max-w-2xl flex-col items-start justify-center rounded-md border border-border bg-card p-8">
      <p className="text-xs font-semibold uppercase tracking-widest text-muted-foreground">Data unavailable</p>
      <h1 className="mt-2 font-serif text-2xl font-semibold tracking-tight text-foreground md:text-3xl">This view could not be loaded.</h1>
      <p className="mt-3 max-w-xl text-sm leading-relaxed text-muted-foreground">The consultation service did not return verified data. No media or consent state was changed.</p>
      <button className="mt-5 inline-flex min-h-10 items-center justify-center rounded-md bg-primary px-4 text-sm font-semibold text-primary-foreground hover:opacity-90" type="button" onClick={reset}>Try again</button>
    </section>
  );
}

export default ErrorRecovery;
