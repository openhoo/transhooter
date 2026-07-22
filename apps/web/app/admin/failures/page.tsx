import type { Metadata } from "next";
import Link from "next/link";
import { requirePageData } from "@/lib/server-application";

export const metadata: Metadata = { title: "Operational failures" };
export const dynamic = "force-dynamic";

type Failure = {
  id: string;
  occurredAt: string;
  consultationId: string;
  stage: string;
  code: string;
  summary: string;
  archiveId: string | null;
};

const failureDateFormatter = new Intl.DateTimeFormat("en", {
  dateStyle: "medium",
  timeStyle: "short",
});

function formatEvidence(summary: string): string {
  try {
    return JSON.stringify(JSON.parse(summary), null, 2);
  } catch {
    return summary;
  }
}

export default async function FailuresPage() {
  const data = await requirePageData<{ failures: Failure[] }>("admin.failures.list");

  return (
    <div className="flex flex-col gap-6">
      <header className="border-b border-border pb-5">
        <p className="text-xs font-semibold uppercase tracking-widest text-muted-foreground">Administration</p>
        <h1 className="mt-1 font-serif text-2xl font-semibold tracking-tight text-foreground md:text-3xl">Operational failures</h1>
        <p className="mt-2 max-w-2xl text-sm leading-relaxed text-muted-foreground">
          Recorded incidents from live sessions and archive processing. Every item preserves the evidence required for supervisory review; provider errors never silently switch vendors.
        </p>
      </header>
      {data.failures.length === 0 ? (
        <section className="rounded-md border border-border bg-card p-6">
          <h2 className="font-serif text-lg font-semibold">No unresolved failures</h2>
          <p className="mt-2 text-sm text-muted-foreground">Archive and worker supervision currently report no action requiring review.</p>
        </section>
      ) : (
        <ol className="flex flex-col gap-4" aria-label="Unresolved preservation failures">
          {data.failures.map((failure) => (
            <li key={failure.id}>
              <article className="overflow-hidden rounded-md border border-border bg-card">
                <header className="flex flex-col gap-3 border-b border-border px-5 py-4 sm:flex-row sm:items-center sm:justify-between">
                  <div>
                    <h2 className="font-mono text-sm font-semibold text-destructive">{failure.code}</h2>
                    <p className="mt-1 text-xs text-muted-foreground">{failure.stage}</p>
                  </div>
                  <div className="flex flex-wrap items-center gap-3">
                    <code className="rounded-full border border-border px-2 py-1 text-xs">{failure.consultationId}</code>
                    <time className="text-xs text-muted-foreground" dateTime={failure.occurredAt}>{failureDateFormatter.format(new Date(failure.occurredAt))}</time>
                  </div>
                </header>
                <div className="px-5 py-4">
                  <p className="text-xs font-semibold uppercase tracking-widest text-muted-foreground">Persisted evidence</p>
                  <pre className="mt-2 max-h-80 overflow-auto whitespace-pre-wrap break-words rounded-md bg-secondary p-4 font-mono text-xs leading-relaxed text-foreground">{formatEvidence(failure.summary)}</pre>
                  {failure.archiveId && (
                    <Link className="mt-3 inline-flex min-h-9 items-center text-sm font-medium text-primary underline-offset-4 hover:underline" href={`/archives/${failure.archiveId}`}>Open archive evidence</Link>
                  )}
                </div>
              </article>
            </li>
          ))}
        </ol>
      )}
    </div>
  );
}
