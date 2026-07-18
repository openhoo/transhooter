import type { Metadata } from "next";
import { ConsultationRoom } from "@/components/consultation-room";
import { requirePageData } from "@/lib/server-application";

export const metadata: Metadata = { title: "Live consultation" };
export const dynamic = "force-dynamic";

type RoomPageProps = {
  params: Promise<{ id: string }>;
};

export default async function RoomPage({ params }: RoomPageProps) {
  const { id } = await params;
  const initial = await requirePageData<React.ComponentProps<typeof ConsultationRoom>["initial"]>(
    "consultations.room",
    { id },
  );

  return <ConsultationRoom initial={initial} />;
}
