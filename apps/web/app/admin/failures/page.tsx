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
        <div className="tableWrap">
          <table>
            <thead>
              <tr>
                <th>Time</th>
                <th>Consultation</th>
                <th>Stage</th>
                <th>Evidence</th>
              </tr>
            </thead>
            <tbody>
              {data.failures.map((failure) => (
                <tr key={failure.id}>
                  <td>{failureDateFormatter.format(new Date(failure.occurredAt))}</td>
                  <td>
                    <code>{failure.consultationId}</code>
                  </td>
                  <td>
                    {failure.stage}
                    <br />
                    <span className="error">{failure.code}</span>
                  </td>
                  <td>
                    {failure.summary}
                    {failure.archiveId && (
                      <>
                        <br />
                        <Link href={`/archives/${failure.archiveId}`}>Open archive evidence</Link>
                      </>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
