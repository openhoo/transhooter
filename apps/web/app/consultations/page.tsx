import type { Metadata } from "next";
import Link from "next/link";
import { ConsultationAction } from "@/components/consultation-actions";
import { requirePageData, requirePageViewer } from "@/lib/server-application";

export const metadata: Metadata = { title: "Consultations" };
export const dynamic = "force-dynamic";

type Consultation = {
  id: string;
  customerName: string;
  status: string;
  startsAt: string | null;
  href: string;
  canCancel: boolean;
  canResend: boolean;
};

type ConsultationsPageData = {
  consultations: Consultation[];
};

const consultationDateFormatter = new Intl.DateTimeFormat("en", {
  dateStyle: "medium",
  timeStyle: "short",
});

function EmptyConsultations({ canCreate }: { canCreate: boolean }) {
  return (
    <section className="empty flat">
      <h2>{canCreate ? "No consultations scheduled" : "No consultations yet"}</h2>
      <p className="muted">
        {canCreate
          ? "Create one to invite a customer and choose its provider profile."
          : "Your invitations and recorded consultations will appear here."}
      </p>
      {canCreate && (
        <Link className="button" href="/consultations/new">
          Create consultation
        </Link>
      )}
    </section>
  );
}

function ConsultationRow({
  canManage,
  consultation,
}: {
  canManage: boolean;
  consultation: Consultation;
}) {
  return (
    <article className="flat row">
      <div>
        <h2>
          <Link href={{ pathname: consultation.href }}>{consultation.customerName}</Link>
        </h2>
        <span className={`status ${consultation.status === "finalizing" ? "warning" : ""}`}>
          {consultation.status}
        </span>
        {consultation.startsAt && (
          <p className="meta">
            {consultationDateFormatter.format(new Date(consultation.startsAt))}
          </p>
        )}
      </div>
      <div className="actions">
        <Link className="button secondary" href={{ pathname: consultation.href }}>
          Open
        </Link>
        {canManage && consultation.canResend && (
          <ConsultationAction id={consultation.id} action="resend">
            Resend invite
          </ConsultationAction>
        )}
        {consultation.canCancel && (
          <ConsultationAction id={consultation.id} action="cancel" danger>
            Cancel
          </ConsultationAction>
        )}
      </div>
    </article>
  );
}

function ConsultationList({
  canManage,
  consultations,
}: {
  canManage: boolean;
  consultations: Consultation[];
}) {
  if (consultations.length === 0) {
    return <EmptyConsultations canCreate={canManage} />;
  }

  return (
    <div>
      {consultations.map((consultation) => (
        <ConsultationRow canManage={canManage} consultation={consultation} key={consultation.id} />
      ))}
    </div>
  );
}

export default async function ConsultationsPage() {
  const { consultations } = await requirePageData<ConsultationsPageData>("consultations.list");
  const viewer = await requirePageViewer();
  const canManage = viewer.staffRole !== null;

  return (
    <div className="stack">
      <div className="row">
        <div>
          <p className="eyebrow">{canManage ? "Employee workspace" : "Your workspace"}</p>
          <h1>Consultations</h1>
        </div>
        {canManage && (
          <Link className="button" href="/consultations/new">
            New consultation
          </Link>
        )}
      </div>
      <ConsultationList canManage={canManage} consultations={consultations} />
    </div>
  );
}
