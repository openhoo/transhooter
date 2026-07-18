import { EgressLayout } from "@/components/egress-layout";
import { requireSignedEgressLayout } from "@/lib/server-application";

export const dynamic = "force-dynamic";

type EgressLayoutSearchParams = {
  consultationId?: string;
  generation?: string;
  expires?: string;
  signature?: string;
};

type EgressLayoutPageProps = {
  searchParams: Promise<EgressLayoutSearchParams>;
};

export default async function EgressLayoutPage({ searchParams }: EgressLayoutPageProps) {
  const values = await searchParams;
  const data = await requireSignedEgressLayout({
    consultationId: values.consultationId ?? "",
    generation: values.generation ?? "",
    expires: values.expires ?? "",
    signature: values.signature ?? "",
  });

  return <EgressLayout participants={data.participants} />;
}
