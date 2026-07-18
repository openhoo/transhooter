import type { Metadata } from "next";
import { NewConsultationForm } from "@/components/consultation-actions";
import { requirePageData } from "@/lib/server-application";

export const metadata: Metadata = { title: "New consultation" };
export const dynamic = "force-dynamic";

type ConsultationProfileOption = {
  id: string;
  name: string;
  revision: number;
};

type NewConsultationPageData = {
  profiles: ConsultationProfileOption[];
};

export default async function NewConsultationPage() {
  const { profiles } = await requirePageData<NewConsultationPageData>(
    "consultations.create.options",
  );

  return (
    <section className="grid">
      <div>
        <p className="eyebrow">New consultation</p>
        <h1>Invite one customer.</h1>
        <p className="lede">
          Two people, one immutable translation profile. Language preferences and recording consent
          are completed separately in the lobby.
        </p>
      </div>
      <NewConsultationForm profiles={profiles} />
    </section>
  );
}
