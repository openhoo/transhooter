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
    <div className="stack">
      <div>
        <p className="eyebrow">Preservation supervision</p>
        <h1>Failures and gaps</h1>
        <p className="lede">
          Every item links to persisted evidence. Provider errors never silently switch a
          consultation to another vendor.
        </p>
      </div>
      {data.failures.length === 0 ? (
        <section className="empty flat">
          <h2>No unresolved failures</h2>
          <p className="muted">
            Archive and worker supervision currently report no action requiring review.
          </p>
        </section>
      ) : (
        <section className="failureList" aria-label="Unresolved preservation failures">
          {data.failures.map((failure) => (
            <article className="failureItem" key={failure.id}>
              <div className="failureIdentity">
                <div className="failureHeading">
                  <time className="meta" dateTime={failure.occurredAt}>
                    {failureDateFormatter.format(new Date(failure.occurredAt))}
                  </time>
                  <span className="status danger">{failure.stage}</span>
                </div>
                <h2>{failure.code}</h2>
                <dl className="failureMetadata">
                  <div>
                    <dt>Consultation</dt>
                    <dd>
                      <code>{failure.consultationId}</code>
                    </dd>
                  </div>
                </dl>
                {failure.archiveId && (
                  <Link
                    className="button secondary failureLink"
                    href={`/archives/${failure.archiveId}`}
                  >
                    Open archive evidence
                  </Link>
                )}
              </div>
              <div className="failureEvidence">
                <p className="failureEvidenceLabel">Persisted evidence</p>
                <pre>{formatEvidence(failure.summary)}</pre>
              </div>
            </article>
          ))}
        </section>
      )}
    </div>
  );
}
