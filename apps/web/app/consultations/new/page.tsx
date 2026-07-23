import type { Metadata } from "next";
import { NewConsultationForm } from "@/components/consultation-actions";
import { requirePageData } from "@/lib/server-application";

export const metadata: Metadata = { title: "New consultation" };
export const dynamic = "force-dynamic";

type ConsultationProfileOption = { id: string; name: string; revision: number };
type NewConsultationPageData = { profiles: ConsultationProfileOption[] };

export default async function NewConsultationPage() {
  const { profiles } = await requirePageData<NewConsultationPageData>(
    "consultations.create.options",
  );

  return (
    <div className="mx-auto flex w-full max-w-2xl flex-col gap-6">
      <div>
        <p className="text-xs font-semibold uppercase tracking-[0.18em] text-primary">
          Employee workspace
        </p>
        <h1 className="mt-2 text-2xl font-semibold tracking-tight md:text-3xl">New consultation</h1>
        <p className="mt-2 leading-relaxed text-muted-foreground">
          Invite one customer to a private interpreted consultation. Language preferences and
          recording consent are completed separately in the lobby.
        </p>
      </div>
      <NewConsultationForm profiles={profiles} />
    </div>
  );
}
