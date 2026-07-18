import type { Metadata } from "next";
import { Lobby } from "@/components/lobby";
import { requirePageData } from "@/lib/server-application";

export const metadata: Metadata = { title: "Consultation lobby" };
export const dynamic = "force-dynamic";

type LobbyPageProps = {
  params: Promise<{ id: string }>;
};

export default async function LobbyPage({ params }: LobbyPageProps) {
  const { id } = await params;
  const initial = await requirePageData<React.ComponentProps<typeof Lobby>["initial"]>(
    "consultations.get",
    { id },
  );

  return <Lobby consultationId={id} initial={initial} />;
}
