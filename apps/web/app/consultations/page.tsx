import { ArrowRight, CalendarClock, Plus } from "lucide-react";
import type { Metadata } from "next";
import Link from "next/link";
import { ConsultationAction } from "@/components/consultation-actions";
import { StatusBadge } from "@/components/status-badge";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
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
type ConsultationsPageData = { consultations: Consultation[] };
const consultationDateFormatter = new Intl.DateTimeFormat("en", {
  dateStyle: "medium",
  timeStyle: "short",
});

function EmptyConsultations({ canCreate }: { canCreate: boolean }) {
  return (
    <Card className="flex flex-col items-center gap-4 py-16 text-center">
      <div className="flex size-12 items-center justify-center rounded-full bg-muted">
        <CalendarClock className="size-6 text-muted-foreground" aria-hidden="true" />
      </div>
      <div>
        <h2 className="font-serif text-xl">
          {canCreate ? "No consultations scheduled" : "No consultations yet"}
        </h2>
        <p className="mt-1 text-sm text-muted-foreground">
          {canCreate
            ? "Create one to invite a customer and choose its provider profile."
            : "Your invitations and recorded consultations will appear here."}
        </p>
      </div>
      {canCreate && (
        <Button render={<Link href="/consultations/new" />}>
          <Plus className="size-4" aria-hidden="true" />
          New consultation
        </Button>
      )}
    </Card>
  );
}

function ConsultationList({
  canManage,
  consultations,
}: {
  canManage: boolean;
  consultations: Consultation[];
}) {
  if (consultations.length === 0) return <EmptyConsultations canCreate={canManage} />;

  return (
    <Card className="overflow-hidden p-0">
      <Table>
        <TableHeader>
          <TableRow className="bg-muted/50 hover:bg-muted/50">
            <TableHead>Customer</TableHead>
            <TableHead>Status</TableHead>
            <TableHead className="hidden sm:table-cell">Date and time</TableHead>
            <TableHead className="text-right">Actions</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {consultations.map((consultation) => (
            <TableRow key={consultation.id}>
              <TableCell className="font-medium">
                <Link
                  className="hover:underline hover:underline-offset-2"
                  href={{ pathname: consultation.href }}
                >
                  {consultation.customerName}
                </Link>
              </TableCell>
              <TableCell>
                <StatusBadge status={consultation.status} />
              </TableCell>
              <TableCell className="hidden text-sm text-muted-foreground sm:table-cell">
                {consultation.startsAt
                  ? consultationDateFormatter.format(new Date(consultation.startsAt))
                  : "Not scheduled"}
              </TableCell>
              <TableCell>
                <div className="flex flex-wrap items-start justify-end gap-1">
                  {canManage && consultation.canResend && (
                    <ConsultationAction
                      contextLabel={consultation.customerName}
                      id={consultation.id}
                      action="resend"
                    >
                      Resend
                    </ConsultationAction>
                  )}
                  {consultation.canCancel && (
                    <ConsultationAction
                      contextLabel={consultation.customerName}
                      id={consultation.id}
                      action="cancel"
                      danger
                    >
                      Cancel
                    </ConsultationAction>
                  )}
                  <Button
                    render={
                      <Link
                        aria-label={`Open consultation with ${consultation.customerName}`}
                        href={{ pathname: consultation.href }}
                      />
                    }
                    variant="outline"
                    size="sm"
                  >
                    Open
                    <ArrowRight className="size-3.5" aria-hidden="true" />
                  </Button>
                </div>
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </Card>
  );
}

export default async function ConsultationsPage() {
  const { consultations } = await requirePageData<ConsultationsPageData>("consultations.list");
  const viewer = await requirePageViewer();
  const canManage = viewer.staffRole !== null;

  return (
    <div className="flex flex-col gap-6">
      <div className="flex flex-col gap-4 sm:flex-row sm:items-end sm:justify-between">
        <div>
          <p className="text-xs font-semibold uppercase tracking-[0.18em] text-primary">
            {canManage ? "Employee workspace" : "Your workspace"}
          </p>
          <h1 className="mt-2 text-2xl font-semibold tracking-tight md:text-3xl">Consultations</h1>
          <p className="mt-1 leading-relaxed text-muted-foreground">
            Your interpreted video consultations, past and upcoming.
          </p>
        </div>
        {canManage && (
          <Button className="w-fit" render={<Link href="/consultations/new" />}>
            <Plus className="size-4" aria-hidden="true" />
            New consultation
          </Button>
        )}
      </div>
      <ConsultationList canManage={canManage} consultations={consultations} />
    </div>
  );
}
